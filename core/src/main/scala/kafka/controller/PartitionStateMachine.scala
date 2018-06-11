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

import collection._
import java.util.concurrent.atomic.AtomicBoolean
import kafka.api.LeaderAndIsr
import kafka.common.{LeaderElectionNotNeededException, TopicAndPartition, StateChangeFailedException, NoReplicaOnlineException}
import kafka.utils.{Logging, ReplicationUtils}
import kafka.utils.ZkUtils._
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.controller.Callbacks.CallbackBuilder
import kafka.utils.CoreUtils._

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 */
//note: 分区状态机
//note: 1. NonExistentPartition: Partition 不存在,或者曾经存在但现在删除了; 
//note: 2. NewPartition: Partition 刚创建时的状态,此时 Partition 有副本,但是还没有 leader 和 isr 信息;
//note: 3. OnlinePartition: Partition 的 leader 被选举之后的状态
//note: 4. OfflinePartition: Partition 的 leader 掉线之后进入的状态
class PartitionStateMachine(controller: KafkaController) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkUtils = controllerContext.zkUtils
  private val partitionState: mutable.Map[TopicAndPartition, PartitionState] = mutable.Map.empty
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
  private val hasStarted = new AtomicBoolean(false)
  private val noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext)
  private val topicChangeListener = new TopicChangeListener(controller)
  private val deleteTopicsListener = new DeleteTopicsListener(controller)
  private val partitionModificationsListeners: mutable.Map[String, PartitionModificationsListener] = mutable.Map.empty
  private val stateChangeLogger = KafkaController.stateChangeLogger

  this.logIdent = "[Partition state machine on Controller " + controllerId + "]: "

  /**
   * Invoked on successful controller election. First registers a topic change listener since that triggers all
   * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
   * the OnlinePartition state change for all new or offline partitions.
   */
  //note: Controller 启动时触发
  //note: 初始化所有 Partition 的状态（从 zk 获取）, 然后对于 new/offline Partition 触发选主（选主成功的话,变为 OnlinePartition）
  def startup() {
    // initialize partition state
    //note: 初始化 partition 的状态,如果 leader 所在 broker 是 alive 的,那么状态为 OnlinePartition,否则为 OfflinePartition
    initializePartitionState()
    // set started flag
    hasStarted.set(true)
    // try to move partitions to online state
    //note: 为所有处理 NewPartition 或 OnlinePartition 状态 Partition 选举 leader
    triggerOnlinePartitionStateChange()

    info("Started partition state machine with initial state -> " + partitionState.toString())
  }

  // register topic and partition change listeners
  //note: 注册 topic 和 partition 变更监控的 listener
  def registerListeners() {
    registerTopicChangeListener() //note: 监听【/brokers/topics】, 是否有新 topic 创建
    registerDeleteTopicListener() //note: 监听【/admin/delete_topics】,是否有 topic 设置删除
  }

  // de-register topic and partition change listeners
  //note: 关闭 Topic 删除和 Partition 变化的监听器
  def deregisterListeners() {
    deregisterTopicChangeListener()
    partitionModificationsListeners.foreach {
      case (topic, listener) =>
        zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), listener)
    }
    partitionModificationsListeners.clear()
    deregisterDeleteTopicListener()
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    // reset started flag
    hasStarted.set(false)
    // clear partition state
    partitionState.clear()
    // de-register all ZK listeners
    deregisterListeners()

    info("Stopped partition state machine")
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   */
  //note: 这个方法是在 controller 选举后或 broker 上线或下线时时触发的
  def triggerOnlinePartitionStateChange() {
    try {
      brokerRequestBatch.newBatch()
      // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions
      // that belong to topics to be deleted
      //note: 开始为所有状态在 NewPartition or OfflinePartition 状态的 partition 更新状态（除去将要被删除的 topic）
      for((topicAndPartition, partitionState) <- partitionState
          if !controller.deleteTopicManager.isTopicQueuedUpForDeletion(topicAndPartition.topic)) {
        if(partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
          //note: 尝试为处在 OfflinePartition 或 NewPartition 状态的 Partition 选主,成功后转换为 OnlinePartition 
          handleStateChange(topicAndPartition.topic, topicAndPartition.partition, OnlinePartition, controller.offlinePartitionSelector,
                            (new CallbackBuilder).build)
      }
      //note: 发送请求给所有的 broker,包括 LeaderAndIsr 请求和 UpdateMetadata 请求（这里只是添加到 Broker 对应的 RequestQueue 中,后台有线程去发送）
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
    } catch {
      case e: Throwable => error("Error while moving some partitions to the online state", e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger leader election for those partitions
    }
  }

  def partitionsInState(state: PartitionState): Set[TopicAndPartition] = {
    partitionState.filter(p => p._2 == state).keySet
  }

  /**
   * This API is invoked by the partition change zookeeper listener
   * @param partitions   The list of partitions that need to be transitioned to the target state
   * @param targetState  The state that the partitions should be moved to
   */
  //note: partition 状态变化,如果 handleStateChange() 方法中需要发送一些 request,在这个方法最后会发送 request
  def handleStateChanges(partitions: Set[TopicAndPartition], targetState: PartitionState,
                         leaderSelector: PartitionLeaderSelector = noOpPartitionLeaderSelector,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    info("Invoking state change to %s for partitions %s".format(targetState, partitions.mkString(",")))
    try {
      brokerRequestBatch.newBatch()//note: 确保之前 request 已经发送完成
      partitions.foreach { topicAndPartition =>
        handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector, callbacks)
      }//note: 处理 Partition 对象状态的变化
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch)//note: 发送 request（request 来自 handleStateChange 添加到 brokerRequestBatch 中的 request）
    }catch {
      case e: Throwable => error("Error while moving some partitions to %s state".format(targetState), e)
      // TODO: It is not enough to bail out and log an error, it is important to trigger state changes for those partitions
    }
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
   * @param topic       The topic of the partition for which the state transition is invoked
   * @param partition   The partition for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   */
  //note: Partition 状态的变化的处理方法
  private def handleStateChange(topic: String, partition: Int, targetState: PartitionState,
                                leaderSelector: PartitionLeaderSelector,
                                callbacks: Callbacks) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change for partition %s to %s failed because " +
                                            "the partition state machine has not started")
                                              .format(controllerId, controller.epoch, topicAndPartition, targetState))
    val currState = partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition)
    try {
      targetState match {
        case NewPartition => //note: 新建一个 partition
          // pre: partition did not exist before this
          assertValidPreviousStates(topicAndPartition, List(NonExistentPartition), NewPartition)
          partitionState.put(topicAndPartition, NewPartition) //note: 缓存 partition 的状态
          val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).mkString(",")
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s with assigned replicas %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState,
                                            assignedReplicas))
          // post: partition has been assigned replicas
        case OnlinePartition =>
          //note: 判断 Partition 之前的状态是否可以转换为目的状态
          assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OnlinePartition)
          partitionState(topicAndPartition) match {
            case NewPartition => //note: 新建的 Partition
              //note: 选举 leader 和 isr,更新到 zk 和 controller 中,如果没有存活的 replica,抛出异常
              // initialize leader and isr path for new partition
              initializeLeaderAndIsrForPartition(topicAndPartition)
            case OfflinePartition => //note: leader 挂掉的 Partition
              //note: 进行 leader 选举,更新到 zk 及 controller 缓存中,失败的抛出异常
              electLeaderForPartition(topic, partition, leaderSelector)
            case OnlinePartition => // invoked when the leader needs to be re-elected
              //note:这种只有在 leader 需要重新选举时才会触发
              electLeaderForPartition(topic, partition, leaderSelector)
            case _ => // should never come here since illegal previous states are checked above
          }
          partitionState.put(topicAndPartition, OnlinePartition)
          val leader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s from %s to %s with leader %d"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState, leader))
           // post: partition has a leader
        case OfflinePartition => //note: 将 Partition 设置为 Offline 状态
          // pre: partition should be in New or Online state
          assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OfflinePartition)
          // should be called when the leader for a partition is no longer alive
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          partitionState.put(topicAndPartition, OfflinePartition)
          // post: partition has no alive leader
        case NonExistentPartition => //note: 将 Partition 设置为 NonExistentPartition
          // pre: partition should be in Offline state
          assertValidPreviousStates(topicAndPartition, List(OfflinePartition), NonExistentPartition)
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s"
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          partitionState.put(topicAndPartition, NonExistentPartition)
          // post: partition state is deleted from all brokers and zookeeper
      }
    } catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change for partition %s from %s to %s failed"
          .format(controllerId, controller.epoch, topicAndPartition, currState, targetState), t)
    }
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   */
  //note: 根据从 zk 获取的所有 Partition,进行状态初始化
  private def initializePartitionState() {
    for (topicPartition <- controllerContext.partitionReplicaAssignment.keys) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      controllerContext.partitionLeadershipInfo.get(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          if (controllerContext.liveBrokerIds.contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader))
            // leader is alive
            //note: 有 LeaderAndIsr 信息,并且 leader 存活,设置为 OnlinePartition 状态 
            partitionState.put(topicPartition, OnlinePartition)
          else
            //note: 有 LeaderAndIsr 信息,但是 leader 不存活,设置为 OfflinePartition 状态 
            partitionState.put(topicPartition, OfflinePartition)
        case None =>
          //note: 没有 LeaderAndIsr 信息,设置为 NewPartition 状态（这个 Partition 还没有）
          partitionState.put(topicPartition, NewPartition)
      }
    }
  }

  //note: 确保 partition 当前状态可以改变为目的状态
  private def assertValidPreviousStates(topicAndPartition: TopicAndPartition, fromStates: Seq[PartitionState],
                                        targetState: PartitionState) {
    if(!fromStates.contains(partitionState(topicAndPartition)))
      throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state"
        .format(topicAndPartition, fromStates.mkString(","), targetState) + ". Instead it is in %s state"
        .format(partitionState(topicAndPartition)))
  }

  /**
   * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
   * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, its leader and isr
   * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
   * OfflinePartition state.
   * @param topicAndPartition   The topic/partition whose leader and isr path is to be initialized
   */
  //note: 当 partition 状态由 NewPartition 变为 OnlinePartition 时,将触发这一方法,用来初始化 partition 的 leader 和 isr
  private def initializeLeaderAndIsrForPartition(topicAndPartition: TopicAndPartition) {
    //note: Partition 的 AR
    val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
    //note: Partition 存活的 AR
    val liveAssignedReplicas = replicaAssignment.filter(r => controllerContext.liveBrokerIds.contains(r))
    liveAssignedReplicas.size match {
      case 0 => //note: 没有存活副本的情况下,抛出异常信息
        val failMsg = ("encountered error during state change of partition %s from New to Online, assigned replicas are [%s], " +
                       "live brokers are [%s]. No assigned replica is alive.")
                         .format(topicAndPartition, replicaAssignment.mkString(","), controllerContext.liveBrokerIds)
        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg)
      case _ => //note: 有存活副本时,从存活的副本中选举 leader 和 isr,更新到 zk 和 controller 的缓存中
        debug("Live assigned replicas for partition %s are: [%s]".format(topicAndPartition, liveAssignedReplicas))
        // make the first replica in the list of assigned replicas, the leader
        val leader = liveAssignedReplicas.head //note: replicas 中的第一个 replica 选做 leader
        val leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, liveAssignedReplicas.toList),
          controller.epoch)
        debug("Initializing leader and isr for partition %s to %s".format(topicAndPartition, leaderIsrAndControllerEpoch))
        try { //note: 将leader 和 isr 更新到 zk 和 controller 的缓存中
          zkUtils.createPersistentPath(
            getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
            zkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch))//note: zk 上初始化节点信息
          // NOTE: the above write can fail only if the current controller lost its zk session and the new controller
          // took over and initialized this partition. This can happen if the current controller went into a long
          // GC pause
          controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch)
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic,
            topicAndPartition.partition, leaderIsrAndControllerEpoch, replicaAssignment)//note: 向 live 的 Replica 发送  LeaderAndIsr 请求
        } catch {
          case _: ZkNodeExistsException =>
            // read the controller epoch
            val leaderIsrAndEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topicAndPartition.topic,
              topicAndPartition.partition).get
            val failMsg = ("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
                           "exists with value %s and controller epoch %d")
                             .format(topicAndPartition, leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch)
            stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
            throw new StateChangeFailedException(failMsg)
        }
    }
  }

  /**
   * Invoked on the OfflinePartition,OnlinePartition->OnlinePartition state change.
   * It invokes the leader election API to elect a leader for the input offline partition
   * @param topic               The topic of the offline partition
   * @param partition           The offline partition
   * @param leaderSelector      Specific leader selector (e.g., offline/reassigned/etc.)
   */
  //note: 被 OfflinePartition,OnlinePartition->OnlinePartition 状态转变时触发
  def electLeaderForPartition(topic: String, partition: Int, leaderSelector: PartitionLeaderSelector) {
    val topicAndPartition = TopicAndPartition(topic, partition)
    // handle leader election for the partitions whose leader is no longer alive
    stateChangeLogger.trace("Controller %d epoch %d started leader election for partition %s"
                              .format(controllerId, controller.epoch, topicAndPartition))
    try {
      var zookeeperPathUpdateSucceeded: Boolean = false //note: 是否更新 zk 成功的标志
      var newLeaderAndIsr: LeaderAndIsr = null
      var replicasForThisPartition: Seq[Int] = Seq.empty[Int]
      while(!zookeeperPathUpdateSucceeded) {
        //note: 当前这个 partition 的详细信息
        val currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition)
        val currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr
        val controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch
        if (controllerEpoch > controller.epoch) { //note: 因为 controller epoch 的问题,无法更新,抛出异常
          val failMsg = ("aborted leader election for partition [%s,%d] since the LeaderAndIsr path was " +
                         "already written by another controller. This probably means that the current controller %d went through " +
                         "a soft failure and another controller was elected with epoch %d.")
                           .format(topic, partition, controllerId, controllerEpoch)
          stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
          throw new StateChangeFailedException(failMsg)
        }
        // elect new leader or throw exception
        //note: leader 选举,选举失败的话抛出异常
        val (leaderAndIsr, replicas) = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr)
        //note: 将结果更新到 zk 中
        val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
          leaderAndIsr, controller.epoch, currentLeaderAndIsr.zkVersion)
        newLeaderAndIsr = leaderAndIsr
        newLeaderAndIsr.zkVersion = newVersion
        zookeeperPathUpdateSucceeded = updateSucceeded
        replicasForThisPartition = replicas
      }
      //note: 更新到 controller 中
      val newLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch)
      // update the leader cache
      controllerContext.partitionLeadershipInfo.put(TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch)
      stateChangeLogger.trace("Controller %d epoch %d elected leader %d for Offline partition %s"
                                .format(controllerId, controller.epoch, newLeaderAndIsr.leader, topicAndPartition))
      val replicas = controllerContext.partitionReplicaAssignment(TopicAndPartition(topic, partition))
      // store new leader and isr info in cache
      //note: 添加相应的 LeaderAndIsr 请求给对应的 broker
      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
        newLeaderIsrAndControllerEpoch, replicas)
    } catch {
      case _: LeaderElectionNotNeededException => // swallow
      case nroe: NoReplicaOnlineException => throw nroe
      case sce: Throwable =>
        val failMsg = "encountered error while electing leader for partition %s due to: %s.".format(topicAndPartition, sce.getMessage)
        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg, sce)
    }
    debug("After leader election, leader cache is updated to %s".format(controllerContext.partitionLeadershipInfo.map(l => (l._1, l._2))))
  }

  //note: 在 zk 上注册一个监控,监控 topic 节点下的变化情况
  private def registerTopicChangeListener() = {
    zkUtils.zkClient.subscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  private def deregisterTopicChangeListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  def registerPartitionChangeListener(topic: String) = {
    partitionModificationsListeners.put(topic, new PartitionModificationsListener(controller, topic))
    zkUtils.zkClient.subscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
  }

  //note: 删除 zk 上对这个 topic 的 PartitionModificationsListener
  def deregisterPartitionChangeListener(topic: String) = {
    zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
    partitionModificationsListeners.remove(topic)
  }

  private def registerDeleteTopicListener() = {
    zkUtils.zkClient.subscribeChildChanges(DeleteTopicsPath, deleteTopicsListener)
  }

  private def deregisterDeleteTopicListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(DeleteTopicsPath, deleteTopicsListener)
  }

  private def getLeaderIsrAndEpochOrThrowException(topic: String, partition: Int): LeaderIsrAndControllerEpoch = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition) match {
      case Some(currentLeaderIsrAndEpoch) => currentLeaderIsrAndEpoch
      case None =>
        val failMsg = "LeaderAndIsr information doesn't exist for partition %s in %s state"
                        .format(topicAndPartition, partitionState(topicAndPartition))
        throw new StateChangeFailedException(failMsg)
    }
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a partition
   */
  //note: 监控 zk 上 Topic 子节点的变化,KafkaController 会进行相应的处理
  class TopicChangeListener(protected val controller: KafkaController) extends ControllerZkChildListener {

    protected def logName = "TopicChangeListener"

    //note: 当 zk 上 topic 节点上有变更时,这个方法就会调用
    def doHandleChildChange(parentPath: String, children: Seq[String]) {
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          try {
            val currentChildren = {
              debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
              children.toSet
            }
            val newTopics = currentChildren -- controllerContext.allTopics//note: 新创建的 topic 列表
            val deletedTopics = controllerContext.allTopics -- currentChildren//note: 已经删除的 topic 列表
            controllerContext.allTopics = currentChildren

            //note: 新创建 topic 对应的 partition 列表
            val addedPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(newTopics.toSeq)
            controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
              !deletedTopics.contains(p._1.topic))//note: 把已经删除 partition 过滤掉
            controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)//note: 将新增的 tp-replicas 更新到缓存中
            info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
              deletedTopics, addedPartitionReplicaAssignment))
            if (newTopics.nonEmpty)//note: 处理新建的 topic
              controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet)
          } catch {
            case e: Throwable => error("Error while handling new topic", e)
          }
        }
      }
    }
  }

  /**
   * Delete topics includes the following operations -
   * 1. Add the topic to be deleted to the delete topics cache, only if the topic exists
   * 2. If there are topics to be deleted, it signals the delete topic thread
   */
  //note: 删除 Topic 包括以下操作:
  //note: 1. 如果要删除的 topic 存在,将 Topic 添加到 Topic 将要删除的缓存中;
  //note: 2. 如果有 Topic 将要被删除,那么将触发 Topic 删除线程
  class DeleteTopicsListener(protected val controller: KafkaController) extends ControllerZkChildListener {
    private val zkUtils = controllerContext.zkUtils

    protected def logName = "DeleteTopicsListener"

    /**
     * Invoked when a topic is being deleted
     * @throws Exception On any error.
     */
    //note: 当 topic 需要被删除时,才会触发
    @throws[Exception]
    def doHandleChildChange(parentPath: String, children: Seq[String]) {
      inLock(controllerContext.controllerLock) {
        var topicsToBeDeleted = children.toSet
        debug("Delete topics listener fired for topics %s to be deleted".format(topicsToBeDeleted.mkString(",")))
        //note: 不存在的、需要删除的 topic, 直接清除 zk 上的记录
        val nonExistentTopics = topicsToBeDeleted -- controllerContext.allTopics
        if (nonExistentTopics.nonEmpty) {
          warn("Ignoring request to delete non-existing topics " + nonExistentTopics.mkString(","))
          nonExistentTopics.foreach(topic => zkUtils.deletePathRecursive(getDeleteTopicPath(topic)))
        }
        topicsToBeDeleted --= nonExistentTopics
        if (controller.config.deleteTopicEnable) {
          if (topicsToBeDeleted.nonEmpty) { //note: 有 Topic 需要删除
            info("Starting topic deletion for topics " + topicsToBeDeleted.mkString(","))
            // mark topic ineligible for deletion if other state changes are in progress
            topicsToBeDeleted.foreach { topic => //note: 如果 topic 正在最优 leader 选举或正在迁移,那么将 topic 标记为不合法删除状态
              val preferredReplicaElectionInProgress =
                controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic).contains(topic)
              val partitionReassignmentInProgress =
                controllerContext.partitionsBeingReassigned.keySet.map(_.topic).contains(topic)
              if (preferredReplicaElectionInProgress || partitionReassignmentInProgress)
                controller.deleteTopicManager.markTopicIneligibleForDeletion(Set(topic))
            }
            // add topic to deletion list
            //note: 将要删除的 topic 添加到待删除的 topic
            controller.deleteTopicManager.enqueueTopicsForDeletion(topicsToBeDeleted)
          }
        } else {
          // If delete topic is disabled remove entries under zookeeper path : /admin/delete_topics
          for (topic <- topicsToBeDeleted) {
            info("Removing " + getDeleteTopicPath(topic) + " since delete topic is disabled")
            zkUtils.zkClient.delete(getDeleteTopicPath(topic))
          }
        }
      }
    }

    def doHandleDataDeleted(dataPath: String) {}
  }

  //note: Partition change 监听器
  class PartitionModificationsListener(protected val controller: KafkaController, topic: String) extends ControllerZkDataListener {

    protected def logName = "AddPartitionsListener"

    def doHandleDataChange(dataPath: String, data: AnyRef) {
      inLock(controllerContext.controllerLock) {
        try {
          info(s"Partition modification triggered $data for path $dataPath")
          val partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(List(topic))
          val partitionsToBeAdded = partitionReplicaAssignment.filter(p =>
            !controllerContext.partitionReplicaAssignment.contains(p._1))//note: 获取新增的 partition 列表
          if(controller.deleteTopicManager.isTopicQueuedUpForDeletion(topic))
            error("Skipping adding partitions %s for topic %s since it is currently being deleted"
                  .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))
          else {
            if (partitionsToBeAdded.nonEmpty) {
              info("New partitions to be added %s".format(partitionsToBeAdded))
              controllerContext.partitionReplicaAssignment.++=(partitionsToBeAdded)
              controller.onNewPartitionCreation(partitionsToBeAdded.keySet)//note: 创建新的 partition
            }
          }
        } catch {
          case e: Throwable => error("Error while handling add partitions for data path " + dataPath, e)
        }
      }
    }

    // this is not implemented for partition change
    def doHandleDataDeleted(parentPath: String): Unit = {}
  }
}

sealed trait PartitionState { def state: Byte }
case object NewPartition extends PartitionState { val state: Byte = 0 }
case object OnlinePartition extends PartitionState { val state: Byte = 1 }
case object OfflinePartition extends PartitionState { val state: Byte = 2 }
case object NonExistentPartition extends PartitionState { val state: Byte = 3 }
