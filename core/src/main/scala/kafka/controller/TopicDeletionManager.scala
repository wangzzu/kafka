/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller


import kafka.server.ConfigType
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractResponse, StopReplicaResponse}

import collection.mutable
import collection.JavaConverters._
import kafka.utils.{Logging, ShutdownableThread}
import kafka.utils.CoreUtils._
import kafka.utils.ZkUtils._

import collection.Set
import kafka.common.TopicAndPartition
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This manages the state machine for topic deletion.
 * 1. TopicCommand issues topic deletion by creating a new admin path /admin/delete_topics/<topic>
 * 2. The controller listens for child changes on /admin/delete_topic and starts topic deletion for the respective topics
 * 3. The controller has a background thread that handles topic deletion. The purpose of having this background thread
 *    is to accommodate the TTL feature, when we have it. This thread is signaled whenever deletion for a topic needs to
 *    be started or resumed. Currently, a topic's deletion can be started only by the onPartitionDeletion callback on the
 *    controller. In the future, it can be triggered based on the configured TTL for the topic. A topic will be ineligible
 *    for deletion in the following scenarios -
 *    3.1 broker hosting one of the replicas for that topic goes down
 *    3.2 partition reassignment for partitions of that topic is in progress
 *    3.3 preferred replica election for partitions of that topic is in progress
 *    (though this is not strictly required since it holds the controller lock for the entire duration from start to end)
 * 4. Topic deletion is resumed when -
 *    4.1 broker hosting one of the replicas for that topic is started
 *    4.2 preferred replica election for partitions of that topic completes
 *    4.3 partition reassignment for partitions of that topic completes
 * 5. Every replica for a topic being deleted is in either of the 3 states -
 *    5.1 TopicDeletionStarted (Replica enters TopicDeletionStarted phase when the onPartitionDeletion callback is invoked.
 *        This happens when the child change watch for /admin/delete_topics fires on the controller. As part of this state
 *        change, the controller sends StopReplicaRequests to all replicas. It registers a callback for the
 *        StopReplicaResponse when deletePartition=true thereby invoking a callback when a response for delete replica
 *        is received from every replica)
 *    5.2 TopicDeletionSuccessful (deleteTopicStopReplicaCallback() moves replicas from
 *        TopicDeletionStarted->TopicDeletionSuccessful depending on the error codes in StopReplicaResponse)
 *    5.3 TopicDeletionFailed. (deleteTopicStopReplicaCallback() moves replicas from
 *        TopicDeletionStarted->TopicDeletionFailed depending on the error codes in StopReplicaResponse.
 *        In general, if a broker dies and if it hosted replicas for topics being deleted, the controller marks the
 *        respective replicas in TopicDeletionFailed state in the onBrokerFailure callback. The reason is that if a
 *        broker fails before the request is sent and after the replica is in TopicDeletionStarted state,
 *        it is possible that the replica will mistakenly remain in TopicDeletionStarted state and topic deletion
 *        will not be retried when the broker comes back up.)
 * 6. The delete topic thread marks a topic successfully deleted only if all replicas are in TopicDeletionSuccessful
 *    state and it starts the topic deletion teardown mode where it deletes all topic state from the controllerContext
 *    as well as from zookeeper. This is the only time the /brokers/topics/<topic> path gets deleted. On the other hand,
 *    if no replica is in TopicDeletionStarted state and at least one replica is in TopicDeletionFailed state, then
 *    it marks the topic for deletion retry.
 * @param controller
 * @param initialTopicsToBeDeleted The topics that are queued up for deletion in zookeeper at the time of controller failover
 * @param initialTopicsIneligibleForDeletion The topics ineligible for deletion due to any of the conditions mentioned in #3 above
 */
class TopicDeletionManager(controller: KafkaController,
                           initialTopicsToBeDeleted: Set[String] = Set.empty,
                           initialTopicsIneligibleForDeletion: Set[String] = Set.empty) extends Logging {
  this.logIdent = "[Topic Deletion Manager " + controller.config.brokerId + "], "
  val controllerContext = controller.controllerContext
  val partitionStateMachine = controller.partitionStateMachine
  val replicaStateMachine = controller.replicaStateMachine
  val deleteLock = new ReentrantLock()
  val deleteTopicsCond = deleteLock.newCondition()
  val deleteTopicStateChanged: AtomicBoolean = new AtomicBoolean(false)
  var deleteTopicsThread: DeleteTopicsThread = null
  val isDeleteTopicEnabled = controller.config.deleteTopicEnable //note: 是否允许删除 topic
  val topicsToBeDeleted: mutable.Set[String] = if (isDeleteTopicEnabled) { //note: 等待删除的 topic 集合
    mutable.Set.empty[String] ++ initialTopicsToBeDeleted //note: 允许删除 topic 时
  } else { //note: 不允许删除 topic 时,从 zk 中清除删除 topic 的触发节点
    // if delete topic is disabled clean the topic entries under /admin/delete_topics
    val zkUtils = controllerContext.zkUtils
    for (topic <- initialTopicsToBeDeleted) {
      val deleteTopicPath = getDeleteTopicPath(topic)
      info("Removing " + deleteTopicPath + " since delete topic is disabled")
      zkUtils.zkClient.delete(deleteTopicPath)
    }
    mutable.Set.empty[String]
  }
  val topicsIneligibleForDeletion: mutable.Set[String] = mutable.Set.empty[String] ++
    (initialTopicsIneligibleForDeletion & topicsToBeDeleted) //note: 不能删除的 topic
  val partitionsToBeDeleted: mutable.Set[TopicAndPartition] = topicsToBeDeleted.flatMap(controllerContext.partitionsForTopic)

  /**
   * Invoked at the end of new controller initiation
   */
  //note: controller 初始化完成,触发这个操作,删除 topic 线程启动
  def start() {
    if (isDeleteTopicEnabled) {
      deleteTopicsThread = new DeleteTopicsThread()
      if (topicsToBeDeleted.nonEmpty)
        deleteTopicStateChanged.set(true)
      deleteTopicsThread.start()
    }
  }

  /**
   * Invoked when the current controller resigns. At this time, all state for topic deletion should be cleared.
   */
  //note: controller 挂时,由 controller 来触发
  def shutdown() {
    // Only allow one shutdown to go through
    if (isDeleteTopicEnabled && deleteTopicsThread.initiateShutdown()) {
      // Resume the topic deletion so it doesn't block on the condition
      resumeTopicDeletionThread()
      // Await delete topic thread to exit
      deleteTopicsThread.awaitShutdown()
      topicsToBeDeleted.clear()
      partitionsToBeDeleted.clear()
      topicsIneligibleForDeletion.clear()
    }
  }

  /**
   * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
   * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
   * i.e. all replicas of all partitions of that topic are deleted successfully.
   * @param topics Topics that should be deleted
   */
  def enqueueTopicsForDeletion(topics: Set[String]) {
    if(isDeleteTopicEnabled) {
      topicsToBeDeleted ++= topics
      partitionsToBeDeleted ++= topics.flatMap(controllerContext.partitionsForTopic)
      resumeTopicDeletionThread()
    }
  }

  /**
   * Invoked when any event that can possibly resume topic deletion occurs. These events include -
   * 1. New broker starts up. Any replicas belonging to topics queued up for deletion can be deleted since the broker is up
   * 2. Partition reassignment completes. Any partitions belonging to topics queued up for deletion finished reassignment
   * 3. Preferred replica election completes. Any partitions belonging to topics queued up for deletion finished
   *    preferred replica election
   * @param topics Topics for which deletion can be resumed
   */
  //note: 下面的情况发生时,将会触发 topic 从不可删除状态移除,topic 将会可以删除
  //note: 1. 新的 broker 启动, 由于 broker 的启动,任何影响 topic 删除的 replica 就已经启动;
  //note: 2. partition reassignment 完成;
  //note: 3. partition 的 leader 选举完成。
  //note: topic 删除恢复。
  def resumeDeletionForTopics(topics: Set[String] = Set.empty) {
    if(isDeleteTopicEnabled) {
      val topicsToResumeDeletion = topics & topicsToBeDeleted
      if(topicsToResumeDeletion.nonEmpty) {
        topicsIneligibleForDeletion --= topicsToResumeDeletion
        resumeTopicDeletionThread()
      }
    }
  }

  /**
   * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
   * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
   * are moved from ReplicaDeletionStarted to ReplicaDeletionIneligible state. Also, the topic is added to the list of topics
   * ineligible for deletion until further notice. The delete topic thread is notified so it can retry topic deletion
   * if it has received a response for all replicas of a topic to be deleted
   * @param replicas Replicas for which deletion has failed
   */
  //note: 对删除失败的 replica 处理
  def failReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    if(isDeleteTopicEnabled) {
      val replicasThatFailedToDelete = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
      if(replicasThatFailedToDelete.nonEmpty) {
        val topics = replicasThatFailedToDelete.map(_.topic)
        debug("Deletion failed for replicas %s. Halting deletion for topics %s"
          .format(replicasThatFailedToDelete.mkString(","), topics))
        //note: 副本状态转为 ReplicaDeletionIneligible
        controller.replicaStateMachine.handleStateChanges(replicasThatFailedToDelete, ReplicaDeletionIneligible)
        markTopicIneligibleForDeletion(topics) //note: topic 设置为不能删除
        resumeTopicDeletionThread()
      }
    }
  }

  /**
   * Halt delete topic if -
   * 1. replicas being down
   * 2. partition reassignment in progress for some partitions of the topic
   * 3. preferred replica election in progress for some partitions of the topic
   * @param topics Topics that should be marked ineligible for deletion. No op if the topic is was not previously queued up for deletion
   */
  //note: 将 topic 标记为不可删除,如果发生下面的情况:
  //note: 1. 副本正在下线;
  //note: 2. topic 的一些 partition 正在进行 partition reassignment;
  //note: 3. topic 的一些 partition 还在进行 leader 选举期间。
  def markTopicIneligibleForDeletion(topics: Set[String]) {
    if(isDeleteTopicEnabled) {
      val newTopicsToHaltDeletion = topicsToBeDeleted & topics
      topicsIneligibleForDeletion ++= newTopicsToHaltDeletion
      if(newTopicsToHaltDeletion.nonEmpty)
        info("Halted deletion of topics %s".format(newTopicsToHaltDeletion.mkString(",")))
    }
  }

  //note: topic 不能被删除
  def isTopicIneligibleForDeletion(topic: String): Boolean = {
    if(isDeleteTopicEnabled) {
      topicsIneligibleForDeletion.contains(topic)
    } else
      true
  }

  def isTopicDeletionInProgress(topic: String): Boolean = {
    if(isDeleteTopicEnabled) {
      controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)
    } else
      false
  }

  def isPartitionToBeDeleted(topicAndPartition: TopicAndPartition) = {
    if(isDeleteTopicEnabled) {
      partitionsToBeDeleted.contains(topicAndPartition)
    } else
      false
  }

  //note: 当前的 topic 是否设置了删除
  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    if(isDeleteTopicEnabled) {
      topicsToBeDeleted.contains(topic)
    } else
      false
  }

  /**
   * Invoked by the delete-topic-thread to wait until events that either trigger, restart or halt topic deletion occur.
   * controllerLock should be acquired before invoking this API
   */
  private def awaitTopicDeletionNotification() {
    inLock(deleteLock) { //note: 判断是否需要等待
      //note: 删除 topic 线程正在运行并且没有设置删除标志位为 true 时需要等待
      while(deleteTopicsThread.isRunning.get() && !deleteTopicStateChanged.compareAndSet(true, false)) {
        debug("Waiting for signal to start or continue topic deletion")
        deleteTopicsCond.await()
      }
    }
  }

  /**
   * Signals the delete-topic-thread to process topic deletion
   */
  private def resumeTopicDeletionThread() {
    deleteTopicStateChanged.set(true)
    inLock(deleteLock) {
      deleteTopicsCond.signal()
    }
  }

  /**
   * Invoked by the StopReplicaResponse callback when it receives no error code for a replica of a topic to be deleted.
   * As part of this, the replicas are moved from ReplicaDeletionStarted to ReplicaDeletionSuccessful state. The delete
   * topic thread is notified so it can tear down the topic if all replicas of a topic have been successfully deleted
   * @param replicas Replicas that were successfully deleted by the broker
   */
  //note: 被 StopReplicaResponse 触发,副本状态从 ReplicaDeletionStarted 变为 ReplicaDeletionSuccessful
  private def completeReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    val successfullyDeletedReplicas = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
    debug("Deletion successfully completed for replicas %s".format(successfullyDeletedReplicas.mkString(",")))
    controller.replicaStateMachine.handleStateChanges(successfullyDeletedReplicas, ReplicaDeletionSuccessful)
    resumeTopicDeletionThread()
  }

  /**
   * Topic deletion can be retried if -
   * 1. Topic deletion is not already complete
   * 2. Topic deletion is currently not in progress for that topic
   * 3. Topic is currently marked ineligible for deletion
   * @param topic Topic
   * @return Whether or not deletion can be retried for the topic
   */
  //note: 如果 topic 删除没有完成、topic 当前并不是在删除中、topic 也没有被标记为不可删除,那么 topic 的删除操作就可以重试
  private def isTopicEligibleForDeletion(topic: String): Boolean = {
    topicsToBeDeleted.contains(topic) && (!isTopicDeletionInProgress(topic) && !isTopicIneligibleForDeletion(topic))
  }

  /**
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionIneligible to OfflineReplica state
   *@param topic Topic for which deletion should be retried
   */
  //note: 找出这个 topic 删除失败的副本,重新删除
  private def markTopicForDeletionRetry(topic: String) {
    // reset replica states from ReplicaDeletionIneligible to OfflineReplica
    val failedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionIneligible)
    info("Retrying delete topic for topic %s since replicas %s were not successfully deleted"
      .format(topic, failedReplicas.mkString(",")))
    controller.replicaStateMachine.handleStateChanges(failedReplicas, OfflineReplica)
  }

  //note: topic 删除后,从 controller 缓存、状态机以及 zk 移除这个 topic 相关记录
  private def completeDeleteTopic(topic: String) {
    // deregister partition change listener on the deleted topic. This is to prevent the partition change listener
    // firing before the new topic listener when a deleted topic gets auto created
    //note: 1. 取消 zk 对这个 topic 的 partition-modify-listener
    partitionStateMachine.deregisterPartitionChangeListener(topic)
    //note: 2. 过滤出副本状态为 ReplicaDeletionSuccessful 的副本列表
    val replicasForDeletedTopic = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
    // controller will remove this replica from the state machine as well as its partition assignment cache
    //note: controller 将会从副本状态机和 Partition-AR 缓存中移除这些副本
    replicaStateMachine.handleStateChanges(replicasForDeletedTopic, NonExistentReplica)
    val partitionsForDeletedTopic = controllerContext.partitionsForTopic(topic)
    // move respective partition to OfflinePartition and NonExistentPartition state
    //note: 3. 从分区状态机中下线并移除这个 topic 的分区
    partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, OfflinePartition)
    partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, NonExistentPartition)
    topicsToBeDeleted -= topic //note: 删除成功,从删除 topic 列表中移除
    partitionsToBeDeleted.retain(_.topic != topic) //note: 从 partitionsToBeDeleted 移除这个 topic
    val zkUtils = controllerContext.zkUtils
    //note: 4. 删除 zk 上关于这个 topic 的相关记录
    zkUtils.zkClient.deleteRecursive(getTopicPath(topic))
    zkUtils.zkClient.deleteRecursive(getEntityConfigPath(ConfigType.Topic, topic))
    zkUtils.zkClient.delete(getDeleteTopicPath(topic))
    //note: 5. 从 controller 的所有缓存中再次移除关于这个 topic 的信息
    controllerContext.removeTopic(topic)
  }

  /**
   * This callback is invoked by the DeleteTopics thread with the list of topics to be deleted
   * It invokes the delete partition callback for all partitions of a topic.
   * The updateMetadataRequest is also going to set the leader for the topics being deleted to
   * {@link LeaderAndIsr#LeaderDuringDelete}. This lets each broker know that this topic is being deleted and can be
   * removed from their caches.
   */
  //note: Topic 删除
  private def onTopicDeletion(topics: Set[String]) {
    info("Topic deletion callback for %s".format(topics.mkString(",")))
    // send update metadata so that brokers stop serving data for topics to be deleted
    val partitions = topics.flatMap(controllerContext.partitionsForTopic) //note: topic 的所有 Partition
    controller.sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions) //note: 更新meta
    val partitionReplicaAssignmentByTopic = controllerContext.partitionReplicaAssignment.groupBy(p => p._1.topic)
    topics.foreach { topic => //note:  删除 topic 的每一个 Partition
      onPartitionDeletion(partitionReplicaAssignmentByTopic(topic).keySet)
    }
  }

  /**
   * Invoked by the onPartitionDeletion callback. It is the 2nd step of topic deletion, the first being sending
   * UpdateMetadata requests to all brokers to start rejecting requests for deleted topics. As part of starting deletion,
   * the topics are added to the in progress list. As long as a topic is in the in progress list, deletion for that topic
   * is never retried. A topic is removed from the in progress list when
   * 1. Either the topic is successfully deleted OR
   * 2. No replica for the topic is in ReplicaDeletionStarted state and at least one replica is in ReplicaDeletionIneligible state
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * As part of starting deletion, all replicas are moved to the ReplicaDeletionStarted state where the controller sends
   * the replicas a StopReplicaRequest (delete=true)
   * This callback does the following things -
   * 1. Move all dead replicas directly to ReplicaDeletionIneligible state. Also mark the respective topics ineligible
   *    for deletion if some replicas are dead since it won't complete successfully anyway
   * 2. Move all alive replicas to ReplicaDeletionStarted state so they can be deleted successfully
   *@param replicasForTopicsToBeDeleted
   */
  //note: 被 onPartitionDeletion 方法触发,删除副本具体的实现的地方
  private def startReplicaDeletion(replicasForTopicsToBeDeleted: Set[PartitionAndReplica]) {
    replicasForTopicsToBeDeleted.groupBy(_.topic).keys.foreach { topic =>
      //note: topic 所有存活的 replica
      val aliveReplicasForTopic = controllerContext.allLiveReplicas().filter(p => p.topic == topic)
      //note: topic 的 dead replica
      val deadReplicasForTopic = replicasForTopicsToBeDeleted -- aliveReplicasForTopic
      //note: topic 中已经处于 ReplicaDeletionSuccessful 状态的副本
      val successfullyDeletedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
      //note: 还没有成功删除的、存活的副本
      val replicasForDeletionRetry = aliveReplicasForTopic -- successfullyDeletedReplicas
      // move dead replicas directly to failed state
      //note: 将 dead replica 设置为 ReplicaDeletionIneligible（删除无效的状态）
      replicaStateMachine.handleStateChanges(deadReplicasForTopic, ReplicaDeletionIneligible)
      // send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader
      //note: 将 replicasForDeletionRetry 设置为 OfflineReplica（发送 StopReplica 请求）
      replicaStateMachine.handleStateChanges(replicasForDeletionRetry, OfflineReplica)
      debug("Deletion started for replicas %s".format(replicasForDeletionRetry.mkString(",")))
      //note: 将 replicasForDeletionRetry 设置为 ReplicaDeletionStarted 状态
      controller.replicaStateMachine.handleStateChanges(replicasForDeletionRetry, ReplicaDeletionStarted,
        new Callbacks.CallbackBuilder().stopReplicaCallback(deleteTopicStopReplicaCallback).build)
      if(deadReplicasForTopic.nonEmpty) { //note: 将 topic 标记为不能删除
        debug("Dead Replicas (%s) found for topic %s".format(deadReplicasForTopic.mkString(","), topic))
        markTopicIneligibleForDeletion(Set(topic))
      }
    }
  }

  /**
   * This callback is invoked by the delete topic callback with the list of partitions for topics to be deleted
   * It does the following -
   * 1. Send UpdateMetadataRequest to all live brokers (that are not shutting down) for partitions that are being
   *    deleted. The brokers start rejecting all client requests with UnknownTopicOrPartitionException
   * 2. Move all replicas for the partitions to OfflineReplica state. This will send StopReplicaRequest to the replicas
   *    and LeaderAndIsrRequest to the leader with the shrunk ISR. When the leader replica itself is moved to OfflineReplica state,
   *    it will skip sending the LeaderAndIsrRequest since the leader will be updated to -1
   * 3. Move all replicas to ReplicaDeletionStarted state. This will send StopReplicaRequest with deletePartition=true. And
   *    will delete all persistent data from all replicas of the respective partitions
   */
  //note: 这个方法是 delete-topic 回调,用于删除 topic 的所有 partition
  //note: 1. 发送 UpdateMetadata 请求给所有的 broker,告诉它们这些 partition 是正在删除的,broker 将会拒绝所有 client 的请求（UnknownTopicOrPartitionException）
  //note: 2. 将 partition 的所有的副本设置为 OfflineReplica 状态,打昂 StopReplica 给所有副本及 LeaderAndIsr 给 leader。当前 leader replica 的状态
  //note:    变为 OfflineReplica 时,它将跳过发送 LeaderAndIsr 请求,因为这些 partition 的 leader 将被更新为-1
  //note: 3. 将所有 replica 的状态设置为 ReplicaDeletionStarted 状态, 它将会发送 StopReplicaRequest（deletePartition=true）,副本将会删除磁盘的数据
  private def onPartitionDeletion(partitionsToBeDeleted: Set[TopicAndPartition]) {
    info("Partition deletion callback for %s".format(partitionsToBeDeleted.mkString(",")))
    val replicasPerPartition = controllerContext.replicasForPartition(partitionsToBeDeleted)
    startReplicaDeletion(replicasPerPartition)
  }

  //note: Topic 副本删除的回调方法
  private def deleteTopicStopReplicaCallback(stopReplicaResponseObj: AbstractResponse, replicaId: Int) {
    val stopReplicaResponse = stopReplicaResponseObj.asInstanceOf[StopReplicaResponse]
    debug("Delete topic callback invoked for %s".format(stopReplicaResponse))
    val responseMap = stopReplicaResponse.responses.asScala
    val partitionsInError =
      if (stopReplicaResponse.errorCode != Errors.NONE.code) responseMap.keySet
      else responseMap.filter { case (_, error) => error != Errors.NONE.code }.keySet
    val replicasInError = partitionsInError.map(p => PartitionAndReplica(p.topic, p.partition, replicaId))
    inLock(controllerContext.controllerLock) {
      // move all the failed replicas to ReplicaDeletionIneligible
      //note: 将删除失败的 replica 设置为 ReplicaDeletionIneligible 状态
      failReplicaDeletion(replicasInError)
      //note: 将删除成功的 replica 设置为 删除成功 状态
      if (replicasInError.size != responseMap.size) { //note: 证明有 replica 删除成功了
        // some replicas could have been successfully deleted
        val deletedReplicas = responseMap.keySet -- partitionsInError //note: 已经删除的 replica
        //note: 对于删除成功的 replica 进行处理（状态转变）
        completeReplicaDeletion(deletedReplicas.map(p => PartitionAndReplica(p.topic, p.partition, replicaId)))
      }
    }
  }

  //note: topic 删除线程
  class DeleteTopicsThread() extends ShutdownableThread(name = "delete-topics-thread-" + controller.config.brokerId, isInterruptible = false) {
    val zkUtils = controllerContext.zkUtils
    override def doWork() {
      awaitTopicDeletionNotification()

      if (!isRunning.get)
        return

      inLock(controllerContext.controllerLock) {
        //note: 要删除的 topic 列表
        val topicsQueuedForDeletion = Set.empty[String] ++ topicsToBeDeleted

        if(topicsQueuedForDeletion.nonEmpty)
          info("Handling deletion for topics " + topicsQueuedForDeletion.mkString(","))

        topicsQueuedForDeletion.foreach { topic =>
        // if all replicas are marked as deleted successfully, then topic deletion is done
          if(controller.replicaStateMachine.areAllReplicasForTopicDeleted(topic)) {//note: 如果 Topic 删除成功的情况下
            // clear up all state for this topic from controller cache and zookeeper
            //note: 从 controller 的缓存和 zk 中清除这个 topic 的所有记录
            completeDeleteTopic(topic)
            info("Deletion of topic %s successfully completed".format(topic))
          } else {
            if(controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)) {
              //note: topic 的副本至少有一个状态为 ReplicaDeletionStarted 时
              // ignore since topic deletion is in progress
              //note: 过滤出 Topic 中副本状态为 ReplicaDeletionStarted 的 Partition 列表
              val replicasInDeletionStartedState = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionStarted)
              //note: 表明了上面这些副本正在删除中
              val replicaIds = replicasInDeletionStartedState.map(_.replica)
              val partitions = replicasInDeletionStartedState.map(r => TopicAndPartition(r.topic, r.partition))
              info("Deletion for replicas %s for partition %s of topic %s in progress".format(replicaIds.mkString(","),
                partitions.mkString(","), topic))
            } else { //note:副本既没有全部删除完成、也没有一个副本是在删除过程中，证明这个 topic 还没有开始删除或者至少一个副本删除失败 
              // if you come here, then no replica is in TopicDeletionStarted and all replicas are not in
              // TopicDeletionSuccessful. That means, that either given topic haven't initiated deletion
              // or there is at least one failed replica (which means topic deletion should be retried).
              if(controller.replicaStateMachine.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
                //note: 如果有副本删除失败,那么进行重试操作
                // mark topic for deletion retry
                markTopicForDeletionRetry(topic)
              }
            }
          }
          // Try delete topic if it is eligible for deletion.
          if(isTopicEligibleForDeletion(topic)) { //note: 如果 topic 可以被删除
            info("Deletion of topic %s (re)started".format(topic))
            // topic deletion will be kicked off
            //note: 开始删除 topic
            onTopicDeletion(Set(topic))
          } else if(isTopicIneligibleForDeletion(topic)) {
            info("Not retrying deletion of topic %s at this time since it is marked ineligible for deletion".format(topic))
          }
        }
      }
    }
  }
}
