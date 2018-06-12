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

import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse}

import scala.collection._
import com.yammer.metrics.core.{Gauge, Meter}
import java.util.concurrent.TimeUnit

import kafka.admin.AdminUtils
import kafka.admin.PreferredReplicaLeaderElectionCommand
import kafka.api._
import kafka.cluster.Broker
import kafka.common._
import kafka.log.LogConfig
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.ZkUtils._
import kafka.utils._
import kafka.utils.CoreUtils._
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.I0Itec.zkclient.{IZkStateListener, ZkClient}
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import java.util.concurrent.locks.ReentrantLock

import kafka.server._
import kafka.common.TopicAndPartition

//note: controller 的上下文数据,启动控制器时从 zk 初始化数据
class ControllerContext(val zkUtils: ZkUtils) {
  var controllerChannelManager: ControllerChannelManager = null
  val controllerLock: ReentrantLock = new ReentrantLock() //note: 重入锁,默认是非公平选择,当多个线程被阻塞时,锁释放后,随机选择一个线程
  var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty
  val brokerShutdownLock: Object = new Object //note: broker shutdown 时使用的锁
  var epoch: Int = KafkaController.InitialControllerEpoch - 1
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion - 1
  var allTopics: Set[String] = Set.empty
  var partitionReplicaAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map.empty //note: partition 与 Replicas 对应关系
  var partitionLeadershipInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty
  val partitionsBeingReassigned: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap
  val partitionsUndergoingPreferredReplicaElection: mutable.Set[TopicAndPartition] = new mutable.HashSet

  private var liveBrokersUnderlying: Set[Broker] = Set.empty
  private var liveBrokerIdsUnderlying: Set[Int] = Set.empty

  // setter
  def liveBrokers_=(brokers: Set[Broker]) {
    liveBrokersUnderlying = brokers
    liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
  }

  // getter
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
  def liveBrokerIds = liveBrokerIdsUnderlying -- shuttingDownBrokerIds

  def liveOrShuttingDownBrokerIds = liveBrokerIdsUnderlying
  def liveOrShuttingDownBrokers = liveBrokersUnderlying

  //note: 获取这个 Broker 的 Partition 集合
  def partitionsOnBroker(brokerId: Int): Set[TopicAndPartition] = {
    partitionReplicaAssignment.collect { //note: 在这里 collect 方法与 map 方法类似
      case (topicAndPartition, replicas) if replicas.contains(brokerId) => topicAndPartition
    }.toSet
  }

  //note: 获取 broker 的 PartitionAndReplica 集合
  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.flatMap { brokerId =>
      partitionReplicaAssignment.collect {
        case (topicAndPartition, replicas) if replicas.contains(brokerId) =>
          new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, brokerId)
      }
    }.toSet
  }

  //note: 获取 topic 的 PartitionAndReplica 集合
  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionReplicaAssignment
      .filter { case (topicAndPartition, _) => topicAndPartition.topic == topic }
      .flatMap { case (topicAndPartition, replicas) =>
        replicas.map { r =>
          new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, r)
        }
      }.toSet
  }

  //note: 获取这个 topic 的 TopicAndPartition 集合
  def partitionsForTopic(topic: String): collection.Set[TopicAndPartition] =
    partitionReplicaAssignment.keySet.filter(topicAndPartition => topicAndPartition.topic == topic)

  def allLiveReplicas(): Set[PartitionAndReplica] = {
    replicasOnBrokers(liveBrokerIds)
  }

  //note: 获取这个分区的 PartitionAndReplica 集合
  def replicasForPartition(partitions: collection.Set[TopicAndPartition]): collection.Set[PartitionAndReplica] = {
    partitions.flatMap { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(r => new PartitionAndReplica(p.topic, p.partition, r))
    }
  }

  //note: 从缓存中移除 topic 的相关信息
  def removeTopic(topic: String) = {
    partitionLeadershipInfo = partitionLeadershipInfo.filter{ case (topicAndPartition, _) => topicAndPartition.topic != topic }
    partitionReplicaAssignment = partitionReplicaAssignment.filter{ case (topicAndPartition, _) => topicAndPartition.topic != topic }
    allTopics -= topic
  }

}


object KafkaController extends Logging {
  val stateChangeLogger = new StateChangeLogger("state.change.logger")
  val InitialControllerEpoch = 1
  val InitialControllerEpochZkVersion = 1

  case class StateChangeLogger(override val loggerName: String) extends Logging

  //note: 获取 controller 的 id 信息
  def parseControllerId(controllerInfoString: String): Int = {
    try {
      Json.parseFull(controllerInfoString) match {
        case Some(m) =>
          val controllerInfo = m.asInstanceOf[Map[String, Any]]
          controllerInfo("brokerid").asInstanceOf[Int]
        case None => throw new KafkaException("Failed to parse the controller info json [%s].".format(controllerInfoString))
      }
    } catch {
      case _: Throwable =>
        // It may be due to an incompatible controller register version
        warn("Failed to parse the controller info as json. "
          + "Probably this controller is still using the old format [%s] to store the broker id in zookeeper".format(controllerInfoString))
        try {
          controllerInfoString.toInt
        } catch {
          case t: Throwable => throw new KafkaException("Failed to parse the controller info: " + controllerInfoString + ". This is neither the new or the old format.", t)
        }
    }
  }
}

class KafkaController(val config: KafkaConfig, zkUtils: ZkUtils, val brokerState: BrokerState, time: Time, metrics: Metrics, threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Controller " + config.brokerId + "]: "
  private var isRunning = true
  private val stateChangeLogger = KafkaController.stateChangeLogger
  val controllerContext = new ControllerContext(zkUtils) //note: controller 的上下文信息，相关的缓存
  val partitionStateMachine = new PartitionStateMachine(this) //note: 分区状态机
  val replicaStateMachine = new ReplicaStateMachine(this) //note: 副本状态机
  private val controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover,
    onControllerResignation, config.brokerId, time) //note: controller 通过 zk 选举
  // have a separate scheduler for the controller to be able to start and stop independently of the
  // kafka server
  private val autoRebalanceScheduler = new KafkaScheduler(1)
  var deleteTopicManager: TopicDeletionManager = null //note: Topic 删除管理器
  //note: 各种 leader 选举，适用于不同的 case

  //note: partition leader 挂掉时，选举 leader
  val offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext, config)
  //note: 重新分配分区时，leader 选举
  private val reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext)
  //note: 使用最优的副本作为 leader
  private val preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext)
  //note: broker 掉线时，重新选举 leader
  private val controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext)
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(this) //note: 以批量的方式发给 broker

  //note: 副本中心分配的监听器
  private val partitionReassignedListener = new PartitionsReassignedListener(this)
  //note: 最优副本分配的监听器（选择 AR 中的第一个副本作为 leader）
  private val preferredReplicaElectionListener = new PreferredReplicaElectionListener(this)
  //note: isr 变动的监听器
  private val isrChangeNotificationListener = new IsrChangeNotificationListener(this)

  newGauge(
    "ActiveControllerCount",
    new Gauge[Int] {
      def value() = if (isActive) 1 else 0
    }
  )

  //note: 统计下线的 partition 数
  newGauge(
    "OfflinePartitionsCount",
    new Gauge[Int] {
      def value(): Int = {
        inLock(controllerContext.controllerLock) {
          if (!isActive)
            0
          else
            controllerContext.partitionLeadershipInfo.count(p => 
              (!controllerContext.liveOrShuttingDownBrokerIds.contains(p._2.leaderAndIsr.leader))
              && (!deleteTopicManager.isTopicQueuedUpForDeletion(p._1.topic))
            )
        }
      }
    }
  )

  //note: 统计不是最优 leader 的 partition 数
  newGauge(
    "PreferredReplicaImbalanceCount",
    new Gauge[Int] {
      def value(): Int = {
        inLock(controllerContext.controllerLock) {
          if (!isActive)
            0
          else
            controllerContext.partitionReplicaAssignment.count {
              case (topicPartition, replicas) => 
                (controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader != replicas.head 
                && (!deleteTopicManager.isTopicQueuedUpForDeletion(topicPartition.topic))
                )
            }
        }
      }
    }
  )

  def epoch: Int = controllerContext.epoch

  def clientId: String = {
    val controllerListener = config.listeners.find(_.listenerName == config.interBrokerListenerName).getOrElse(
      throw new IllegalArgumentException(s"No listener with name ${config.interBrokerListenerName} is configured."))
    "id_%d-host_%s-port_%d".format(config.brokerId, controllerListener.host, controllerListener.port)
  }

  /**
   * On clean shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   *
   * @param id Id of the broker to shutdown.
   * @return The number of partitions that the broker still leads.
   */
  //note: 优雅地关闭 Broker
  //note: controller 首先决定将这个 broker 上的 leader 迁移到其他可用的机器上
  //note: 返回还没有 leader 的迁移的 TopicPartition 集合
  def shutdownBroker(id: Int): Set[TopicAndPartition] = {

    if (!isActive) {
      throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
    }

    controllerContext.brokerShutdownLock synchronized { //note: 拿到 broker shutdown 的唯一锁
      info("Shutting down broker " + id)

      inLock(controllerContext.controllerLock) { //note: 拿到 controllerLock 的排它锁
        if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
          throw new BrokerNotAvailableException("Broker id %d does not exist.".format(id))

        controllerContext.shuttingDownBrokerIds.add(id) //note: 将 broker id 添加到正在关闭的 broker 列表中
        debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds.mkString(","))
        debug("Live brokers: " + controllerContext.liveBrokerIds.mkString(","))
      }

      //note: 获取这个 broker 上所有 Partition 与副本数的 map
      val allPartitionsAndReplicationFactorOnBroker: Set[(TopicAndPartition, Int)] =
        inLock(controllerContext.controllerLock) {
          controllerContext.partitionsOnBroker(id)
            .map(topicAndPartition => (topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition).size))
        }

      //note: 处理这些 TopicPartition，更新 Partition 或 Replica 的状态，必要时进行 leader 选举
      allPartitionsAndReplicationFactorOnBroker.foreach {
        case(topicAndPartition, replicationFactor) =>
          // Move leadership serially to relinquish lock.
          inLock(controllerContext.controllerLock) {
            controllerContext.partitionLeadershipInfo.get(topicAndPartition).foreach { currLeaderIsrAndControllerEpoch =>
              if (replicationFactor > 1) { //note: 副本数大于1
                if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) { //note: leader 正好是下线的节点
                  // If the broker leads the topic partition, transition the leader and update isr. Updates zk and
                  // notifies all affected brokers
                  //todo: 这种情况下 Replica 的状态不需要修改么？
                  //note: 状态变化（变为 OnlinePartition，并且进行 leader 选举，使用 controlledShutdownPartitionLeaderSelector 算法）
                  partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition,
                    controlledShutdownPartitionLeaderSelector)
                } else {
                  // Stop the replica first. The state change below initiates ZK changes which should take some time
                  // before which the stop replica request should be completed (in most cases)
                  try { //note: 要下线的机器停止副本迁移，发送 StopReplica 请求
                    brokerRequestBatch.newBatch()
                    brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), topicAndPartition.topic,
                      topicAndPartition.partition, deletePartition = false)
                    brokerRequestBatch.sendRequestsToBrokers(epoch)
                  } catch {
                    case e : IllegalStateException => {
                      // Resign if the controller is in an illegal state
                      error("Forcing the controller to resign")
                      brokerRequestBatch.clear()
                      controllerElector.resign()

                      throw e
                    }
                  }
                  // If the broker is a follower, updates the isr in ZK and notifies the current leader
                  //note: 更新这个副本的状态，变为 OfflineReplica
                  replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic,
                    topicAndPartition.partition, id)), OfflineReplica)
                }
              }
            }
          }
      }
      //note: 返回 leader 在这个要下线节点上并且副本数大于 1 的 TopicPartition 集合
      //note: 在已经进行前面 leader 迁移后
      def replicatedPartitionsBrokerLeads() = inLock(controllerContext.controllerLock) {
        trace("All leaders = " + controllerContext.partitionLeadershipInfo.mkString(","))
        controllerContext.partitionLeadershipInfo.filter {
          case (topicAndPartition, leaderIsrAndControllerEpoch) =>
            leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment(topicAndPartition).size > 1
        }.keys
      }
      replicatedPartitionsBrokerLeads().toSet
    }
  }

  /**
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   * It does the following things on the become-controller state change -
   * 1. Register controller epoch changed listener
   * 2. Increments the controller epoch
   * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 4. Starts the controller's channel manager
   * 5. Starts the replica state machine
   * 6. Starts the partition state machine
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   */
  //note: 当当前 Broker 被选为 controller 时, 当被选为 controller,它将会做以下操作
  //note: 1. 注册 controller epoch changed listener;
  //note: 2. controller epoch 自增加1;
  //note: 3. 初始化 KafkaController 的上下文信息 ControllerContext,它包含了当前的 topic、存活的 broker 以及已经存在的 partition 的 leader;
  //note: 4. 启动 controller 的 channel 管理: 建立与其他 broker 的连接的,负责与其他 broker 之间的通信;
  //note: 5. 启动 ReplicaStateMachine（副本状态机,管理副本的状态）;
  //note: 6. 启动 PartitionStateMachine（分区状态机,管理分区的状态）;
  //note: 如果在 Controller 服务初始化的过程中，出现了任何不可预期的 异常/错误，它将会退出当前的进程，这确保了可以再次触发 controller 的选举
  def onControllerFailover() {
    if(isRunning) {
      info("Broker %d starting become controller state transition".format(config.brokerId))
      readControllerEpochFromZookeeper() //note: 从 zk 获取 controllrt 的 epoch 和 zkVersion 值
      incrementControllerEpoch(zkUtils.zkClient) //note: 更新 Controller 的 epoch 和 zkVersion 值，可能会抛出异常

      // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
      //note: 再从 zk 获取数据初始化前，注册一些关于 broker/topic 的回调监听器
      registerReassignedPartitionsListener() //note: 监控路径【/admin/reassign_partitions】，分区迁移监听
      registerIsrChangeNotificationListener() //note: 监控路径【/isr_change_notification】，isr 变动监听
      registerPreferredReplicaElectionListener() //note: 监听路径【/admin/preferred_replica_election】，最优 leader 选举
      partitionStateMachine.registerListeners()//note: 监听 Topic 的创建与删除
      replicaStateMachine.registerListeners() //note: 监听 broker 的上下线

      //note: 初始化 controller 相关的变量信息:包括 alive broker 列表、partition 的详细信息等
      initializeControllerContext() //note: 初始化 controller 相关的变量信息

      // We need to send UpdateMetadataRequest after the controller context is initialized and before the state machines
      // are started. The is because brokers need to receive the list of live brokers from UpdateMetadataRequest before
      // they can process the LeaderAndIsrRequests that are generated by replicaStateMachine.startup() and
      // partitionStateMachine.startup().
      //note: 在 controller contest 初始化之后,我们需要发送 UpdateMetadata 请求在状态机启动之前,这是因为 broker 需要从 UpdateMetadata 请求
      //note: 获取当前存活的 broker list, 因为它们需要处理来自副本状态机或分区状态机启动发送的 LeaderAndIsr 请求
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

      //note: 初始化 replica 的状态信息: replica 是存活状态时是 OnlineReplica, 否则是 ReplicaDeletionIneligible
      replicaStateMachine.startup() //note: 初始化 replica 的状态信息
      //note: 初始化 partition 的状态信息:如果 leader 所在 broker 是 alive 的,那么状态为 OnlinePartition,否则为 OfflinePartition
      //note: 并状态为 OfflinePartition 的 topic 选举 leader
      partitionStateMachine.startup() //note: 初始化 partition 的状态信息

      // register the partition change listeners for all existing topics on failover
      //note: 为所有的 topic 注册 partition change 监听器
      controllerContext.allTopics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
      info("Broker %d is ready to serve as the new controller with epoch %d".format(config.brokerId, epoch))
      maybeTriggerPartitionReassignment() //note: 触发一次分区副本迁移的操作
      maybeTriggerPreferredReplicaElection() //note: 触发一次分区的最优 leader 选举操作
      if (config.autoLeaderRebalanceEnable) { //note: 如果开启自动均衡
        info("starting the partition rebalance scheduler")
        autoRebalanceScheduler.startup()
        autoRebalanceScheduler.schedule("partition-rebalance-thread", checkAndTriggerPartitionRebalance,
          5, config.leaderImbalanceCheckIntervalSeconds.toLong, TimeUnit.SECONDS) //note: 发送最新的 meta 信息
      }
      deleteTopicManager.start() //note: topic 删除线程启动
    }
    else
      info("Controller has been shut down, aborting startup/failover")
  }

  /**
   * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
   * required to clean up internal controller data structures
   * Note:We need to resign as a controller out of the controller lock to avoid potential deadlock issue
   */
  //note: 这个方法是由 zk 触发（当这个 broker 不再是 controller 时）,它需要清除 controller 相关的数据结构
  def onControllerResignation() {
    debug("Controller resigning, broker id %d".format(config.brokerId))
    // de-register listeners
    //note: 取消注册的 listener
    deregisterIsrChangeNotificationListener()
    deregisterReassignedPartitionsListener()
    deregisterPreferredReplicaElectionListener()

    // shutdown delete topic manager
    //note: 关闭删除 topic 线程
    if (deleteTopicManager != null)
      deleteTopicManager.shutdown()

    // shutdown leader rebalance scheduler
    //note: 关闭 leader rebalance 调度线程（如果允许自动 leader 均衡的话）
    if (config.autoLeaderRebalanceEnable)
      autoRebalanceScheduler.shutdown()

    inLock(controllerContext.controllerLock) {
      // de-register partition ISR listener for on-going partition reassignment task
      //note: 取消注册的 partition isr listener
      deregisterReassignedPartitionsIsrChangeListeners()
      // shutdown partition state machine
      //note: 关闭状态机
      partitionStateMachine.shutdown()
      // shutdown replica state machine
      replicaStateMachine.shutdown()
      // shutdown controller channel manager
      //note: 关闭 controller channel manager
      if(controllerContext.controllerChannelManager != null) {
        controllerContext.controllerChannelManager.shutdown()
        controllerContext.controllerChannelManager = null
      }
      // reset controller context
      controllerContext.epoch=0
      controllerContext.epochZkVersion=0
      //note: 更新 broker 的状态为 RunningAsBroker
      brokerState.newState(RunningAsBroker)

      info("Broker %d resigned as the controller".format(config.brokerId))
    }
  }

  /**
   * Returns true if this broker is the current controller.
   */
  //note: 判断当前 broker 是否为 controller，根据 controller 的 channel 来判断
  def isActive: Boolean = {
    inLock(controllerContext.controllerLock) {
      controllerContext.controllerChannelManager != null
    }
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Sends update metadata request to all live and shutting down brokers
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
   *    so, it performs the reassignment logic for each topic/partition.
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
   */
  //note: 这个是被 副本状态机触发,
  //note: 1. 发送 update-metadata 请求给所有存活的 broker;
  //note: 2. 对于所有 new/offline partition 触发选主操作,选举成功的,Partition 状态设置为 Online
  //note: 3. 检查是否有分区的重新副本分配分配到了这个台机器上,如果有,就进行相应的操作
  def onBrokerStartup(newBrokers: Seq[Int]) {
    info("New broker startup callback for %s".format(newBrokers.mkString(",")))
    val newBrokersSet = newBrokers.toSet //note: 新启动的 broker
    // send update metadata request to all live and shutting down brokers. Old brokers will get to know of the new
    // broker via this update.
    // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
    // common controlled shutdown case, the metadata will reach the new brokers faster
    //note: 发送 metadata 更新给所有的 broker, 这样的话旧的 broker 将会知道有机器新上线了
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    //note:  获取这个机器上的所有 replica 请求
    val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
    //note: 将这些副本的状态设置为 OnlineReplica
    replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers, OnlineReplica)
    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those
    //note: 新的 broker 上线也会触发所有处于 new/offline 的 partition 进行 leader 选举
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // check if reassignment of some partitions need to be restarted
    //note: 检查是否副本的重新分配分配到了这台机器上
    val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter {
      case (_, reassignmentContext) => reassignmentContext.newReplicas.exists(newBrokersSet.contains(_))
    }
    //note: 如果需要副本进行迁移的话,就执行副本迁移操作
    partitionsWithReplicasOnNewBrokers.foreach(p => onPartitionReassignment(p._1, p._2))
    // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
    // on the newly restarted brokers, there is a chance that topic deletion can resume
    //note: 检查 topic 删除操作是否需要重新启动
    val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
    if(replicasForTopicsToBeDeleted.nonEmpty) {
      info(("Some replicas %s for topics scheduled for deletion %s are on the newly restarted brokers %s. " +
        "Signaling restart of topic deletion for these topics").format(replicasForTopicsToBeDeleted.mkString(","),
        deleteTopicManager.topicsToBeDeleted.mkString(","), newBrokers.mkString(",")))
      deleteTopicManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It does the following -
   * 1. Mark partitions with dead leaders as offline
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. Invokes the OfflineReplica state change on the input list of newly started brokers
   * 4. If no partitions are effected then send UpdateMetadataRequest to live or shutting down brokers
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
   * the partition state machine will refresh our cache for us when performing leader election for all new/offline
   * partitions coming online.
   */
  //note: 这个方法会被副本状态机调用（进行 broker 节点下线操作）
  //note: 1. 将 leader 在这台机器上的分区设置为 Offline
  //note: 2. 通过 OfflinePartitionLeaderSelector 为 new/offline partition 选举新的 leader
  //note: 3. leader 选举后,发送 LeaderAndIsr 请求给该分区所有存活的副本;
  //note: 4. 分区选举 leader 后,状态更新为 Online
  //note: 5. 要下线的 broker 上的所有 replica 改为 Offline 状态
  def onBrokerFailure(deadBrokers: Seq[Int]) {
    info("Broker failure callback for %s".format(deadBrokers.mkString(",")))
    //note: 从正在下线的 broker 集合中移除已经下线的机器
    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    info("Removed %s from list of shutting down brokers.".format(deadBrokersThatWereShuttingDown))
    val deadBrokersSet = deadBrokers.toSet
    // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers
    //note: 1. 将 leader 在这台机器上的、并且未设置删除的分区状态设置为 Offline
    val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
      deadBrokersSet.contains(partitionAndLeader._2.leaderAndIsr.leader) &&
        !deleteTopicManager.isTopicQueuedUpForDeletion(partitionAndLeader._1.topic)).keySet
    partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    //note: 2. 选举 leader, 选举成功后设置为 Online 状态
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // filter out the replicas that belong to topics that are being deleted
    //note: 过滤出 replica 在这个机器上、并且没有被设置为删除的 topic 列表
    var allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokersSet)
    val activeReplicasOnDeadBrokers = allReplicasOnDeadBrokers.filterNot(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
    // handle dead replicas
    //note: 将这些 replica 状态转为 Offline
    replicaStateMachine.handleStateChanges(activeReplicasOnDeadBrokers, OfflineReplica)
    // check if topic deletion state for the dead replicas needs to be updated
    //note: 过滤设置为删除的 replica
    val replicasForTopicsToBeDeleted = allReplicasOnDeadBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
    if(replicasForTopicsToBeDeleted.nonEmpty) { //note: 将上面这个 topic 列表的 topic 标记为删除失败
      // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
      // deleted when the broker is down. This will prevent the replica from being in TopicDeletionStarted state indefinitely
      // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
      deleteTopicManager.failReplicaDeletion(replicasForTopicsToBeDeleted)
    }

    // If broker failure did not require leader re-election, inform brokers of failed broker
    // Note that during leader re-election, brokers update their metadata
    if (partitionsWithoutLeader.isEmpty) {
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    }
  }

  /**
   * This callback is invoked by the partition state machine's topic change listener with the list of new topics
   * and partitions as input. It does the following -
   * 1. Registers partition change listener. This is not required until KAFKA-347
   * 2. Invokes the new partition callback
   * 3. Send metadata request with the new topic to all brokers so they allow requests for that topic to be served
   */
  //note: 当 partition state machine 监控到有新 topic 或 partition 时,这个方法将会被调用
  //note: 1. 注册 partition change listener;
  //note: 2. 触发 the new partition callback,也即是 onNewPartitionCreation()
  //note: 3. 发送 metadata 请求给所有的 Broker
  def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]) {
    info("New topic creation callback for %s".format(newPartitions.mkString(",")))
    // subscribe to partition changes
    topics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
    onNewPartitionCreation(newPartitions)
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  //note: topic 变化时,这个方法将会被调用
  //note: 1. 将新创建的 partition 对象置为 NewPartition 状态; 2.从 NewPartition 改为 OnlinePartition 状态
  //note: 1. 将新创建的 Replica 对象置为 NewReplica 状态; 2.从 NewReplica 改为 OnlineReplica 状态
  def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]) {
    info("New partition creation callback for %s".format(newPartitions.mkString(",")))
    partitionStateMachine.handleStateChanges(newPartitions, NewPartition)
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), NewReplica)
    partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector)
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), OnlineReplica)
  }

  /**
   * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
   * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   * RAR = Reassigned replicas
   * OAR = Original list of replicas for partition
   * AR = current assigned replicas
   *
   * 1. Update AR in ZK with OAR + RAR.
   * 2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR). We do this by forcing an update
   *    of the leader epoch in zookeeper.
   * 3. Start new replicas RAR - OAR by moving replicas in RAR - OAR to NewReplica state.
   * 4. Wait until all replicas in RAR are in sync with the leader.
   * 5  Move all replicas in RAR to OnlineReplica state.
   * 6. Set AR to RAR in memory.
   * 7. If the leader is not in RAR, elect a new leader from RAR. If new leader needs to be elected from RAR, a LeaderAndIsr
   *    will be sent. If not, then leader epoch will be incremented in zookeeper and a LeaderAndIsr request will be sent.
   *    In any case, the LeaderAndIsr request will have AR = RAR. This will prevent the leader from adding any replica in
   *    RAR - OAR back in the isr.
   * 8. Move all replicas in OAR - RAR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *    isr to remove OAR - RAR in zookeeper and send a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *    After that, we send a StopReplica (delete = false) to the replicas in OAR - RAR.
   * 9. Move all replicas in OAR - RAR to NonExistentReplica state. This will send a StopReplica (delete = true) to
   *    the replicas in OAR - RAR to physically delete the replicas on disk.
   * 10. Update AR in ZK with RAR.
   * 11. Update the /admin/reassign_partitions path in ZK to remove this partition.
   * 12. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
   *
   * For example, if OAR = {1, 2, 3} and RAR = {4,5,6}, the values in the assigned replica (AR) and leader/isr path in ZK
   * may go through the following transition.
   * AR                 leader/isr
   * {1,2,3}            1/{1,2,3}           (initial state)
   * {1,2,3,4,5,6}      1/{1,2,3}           (step 2)
   * {1,2,3,4,5,6}      1/{1,2,3,4,5,6}     (step 4)
   * {1,2,3,4,5,6}      4/{1,2,3,4,5,6}     (step 7)
   * {1,2,3,4,5,6}      4/{4,5,6}           (step 8)
   * {4,5,6}            4/{4,5,6}           (step 10)
   *
   * Note that we have to update AR in ZK with RAR last since it's the only place where we store OAR persistently.
   * This way, if the controller crashes before that step, we can still recover.
   */
  //note: 这个回调方法被 reassigned partitions listener 触发,当需要进行分区副本迁移时,会在【/admin/reassign_partitions】下创建一个节点来触发操作
  //note: RAR: 重新分配的副本, OAR: 这个分区原来的副本列表, AR: 当前的分配的副本
  //note: 1. 把 AR = OAR+RAR 更新到 zk 及本地缓存中;
  //note: 2. 发送 LeaderAndIsr 给 AR 中每一个副本,并且会强制更新 zk 中 leader 的 epoch;
  //note: 3. 开始新的副本 【RAR-OAR】,状态设置为 NewReplica
  //note: 4. 等待直到 RAR 中的所有副本都在 ISR 中;
  //note: 5. 把 ARA 中的所有副本设置为 OnReplica 状态;
  //note: 6. 将缓存中 AR 更新为 RAR（重新分配的副本列表）;
  //note: 7. 如果 leader 不在 RAR 中, 就从 RAR 选择对应的 leader,然后发送 LeaderAndIsr 请求;如果不需要,那么只会更新 leader epoch,然后发送 
  //note:    LeaderAndIsr请求; 在发送 LeaderAndIsr 请求前设置了 AR=RAR, 这将确保了 leader 在 isr 中不会添加任何 【RAR-OAR】中的副本（old replica）
  //note: 8. 将【OAR-RAR】中的副本设置为 OfflineReplica 状态。OfflineReplica 状态的变化,将会从 ISR 中删除【OAR-RAR】的副本,更新到 zk 中并发送
  //note:    LeaderAndIsr 请求给 leader,通知 leader isr 变动。之后再发送 StopReplica 请求（delete=false）给【OAR-RAR】中的副本。
  //note: 9. 将【OAR-RAR】中的副本设置为 NonExistentReplica 状态。这将发送 StopReplica 请求（delete=true）给【OAR-RAR】中的副本,这些副本将会从本地上删除数据。
  //note: 10. 在 zk 中更新 AR 为 RAR。
  //note: 11. 更新 zk 中路径 【/admin/reassign_partitions】信息,移除已经成功迁移的 Partition
  //note: 12. leader 选举之后,这个 replica 和 isr 信息将会变动,发送 metadata 更新给所有的 broker
  def onPartitionReassignment(topicAndPartition: TopicAndPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    if (!areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas)) {
      //note: 新分配的并没有权限在 isr 中
      info("New replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
        "reassigned not yet caught up with the leader")
      //note: RAR-OAR
      val newReplicasNotInOldReplicaList = reassignedReplicas.toSet -- controllerContext.partitionReplicaAssignment(topicAndPartition).toSet
      //note: RAR+OAR
      val newAndOldReplicas = (reassignedPartitionContext.newReplicas ++ controllerContext.partitionReplicaAssignment(topicAndPartition)).toSet
      //1. Update AR in ZK with OAR + RAR.
      updateAssignedReplicasForPartition(topicAndPartition, newAndOldReplicas.toSeq)
      //2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR).
      updateLeaderEpochAndSendRequest(topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition),
        newAndOldReplicas.toSeq)
      //3. replicas in RAR - OAR -> NewReplica
      //note: 新分配的副本状态更新为 NewReplica（在第二步中发送 LeaderAndIsr 请求时,新的副本会开始创建并且同步数据）
      startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList)
      info("Waiting for new replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
        "reassigned to catch up with the leader")
    } else { //note: 新副本全在 isr 中了
      //4. Wait until all replicas in RAR are in sync with the leader.
     //note: 【OAR-RAR】
      val oldReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).toSet -- reassignedReplicas.toSet
      //5. replicas in RAR -> OnlineReplica
      //note: RAR 中的副本都在 isr 中了,将副本状态设置为 OnlineReplica
      reassignedReplicas.foreach { replica =>
        replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
          replica)), OnlineReplica)
      }
      //6. Set AR to RAR in memory.
      //7. Send LeaderAndIsr request with a potential new leader (if current leader not in RAR) and
      //   a new AR (using RAR) and same isr to every broker in RAR
      //note: 到这一步,新加入的 replica 已经同步完成,leader和isr都更新到最新的结果
      moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext)
      //8. replicas in OAR - RAR -> Offline (force those replicas out of isr)
      //9. replicas in OAR - RAR -> NonExistentReplica (force those replicas to be deleted)
      //note: 下线旧的副本
      stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext, oldReplicas)
      //10. Update AR in ZK with RAR.
      updateAssignedReplicasForPartition(topicAndPartition, reassignedReplicas)
      //11. Update the /admin/reassign_partitions path in ZK to remove this partition.
      //note: partition 迁移完成,从待迁移的集合中移除该 Partition
      removePartitionFromReassignedPartitions(topicAndPartition)
      info("Removed partition %s from the list of reassigned partitions in zookeeper".format(topicAndPartition))
      controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
      //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
      //note: 发送 metadata 更新请求给所有存活的 broker
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicAndPartition))
      // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
      //note: topic 删除恢复（如果当前 topic 设置了删除,之前由于无法删除）
      deleteTopicManager.resumeDeletionForTopics(Set(topicAndPartition.topic))
    }
  }

  private def watchIsrChangesForReassignedPartition(topic: String,
                                                    partition: Int,
                                                    reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val isrChangeListener = new ReassignedPartitionsIsrChangeListener(this, topic, partition,
      reassignedReplicas.toSet)
    reassignedPartitionContext.isrChangeListener = isrChangeListener
    // register listener on the leader and isr path to wait until they catch up with the current leader
    zkUtils.zkClient.subscribeDataChanges(getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener)
  }

  //note: 初始化 Topic-Partition 的副本迁移
  def initiateReassignReplicasForTopicPartition(topicAndPartition: TopicAndPartition,
                                        reassignedPartitionContext: ReassignedPartitionsContext) {
    //note: 要迁移的 topic-partition，及新的副本
    val newReplicas = reassignedPartitionContext.newReplicas
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    try {
      val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicAndPartition) //note: partition 的 AR
      assignedReplicasOpt match {
        case Some(assignedReplicas) =>
          if (assignedReplicas == newReplicas) { //note: 不需要迁移
            throw new KafkaException("Partition %s to be reassigned is already assigned to replicas".format(topicAndPartition) +
              " %s. Ignoring request for partition reassignment".format(newReplicas.mkString(",")))
          } else {
            info("Handling reassignment of partition %s to new replicas %s".format(topicAndPartition, newReplicas.mkString(",")))
            // first register ISR change listener
            //note: 首先注册 ISR 监听的变化
            watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext)
            //note: 正在迁移 Partition 添加到缓存中
            controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext)
            // mark topic ineligible for deletion for the partitions being reassigned
            //note: 设置正在迁移的副本为不能删除
            deleteTopicManager.markTopicIneligibleForDeletion(Set(topic))
            //note: 进行副本迁移
            onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
          }
        case None => throw new KafkaException("Attempt to reassign partition %s that doesn't exist"
          .format(topicAndPartition))
      }
    } catch {
      case e: Throwable => error("Error completing reassignment of partition %s".format(topicAndPartition), e)
      // remove the partition from the admin path to unblock the admin client
      removePartitionFromReassignedPartitions(topicAndPartition)
    }
  }

  //note: 对 Partition 进行最优 leader 选举（isTriggeredByAutoRebalance 默认 False）
  def onPreferredReplicaElection(partitions: Set[TopicAndPartition], isTriggeredByAutoRebalance: Boolean = false) {
    info("Starting preferred replica leader election for partitions %s".format(partitions.mkString(",")))
    try {
      controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitions //note: 添加到集合中
      deleteTopicManager.markTopicIneligibleForDeletion(partitions.map(_.topic)) //note: 标记为不能删除
      //note: 由 Partition 状态机来触发 leader 选举
      partitionStateMachine.handleStateChanges(partitions, OnlinePartition, preferredReplicaPartitionLeaderSelector)
    } catch {
      case e: Throwable => error("Error completing preferred replica leader election for partitions %s".format(partitions.mkString(",")), e)
    } finally {
      //note: 
      removePartitionsFromPreferredReplicaElection(partitions, isTriggeredByAutoRebalance)
      deleteTopicManager.resumeDeletionForTopics(partitions.map(_.topic))
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  //NOTE: 当 broker 的 controller 模块启动时触发,它比并不保证当前 broker 是 controller,它仅仅是注册 registerSessionExpirationListener 和启动 controllerElector
  def startup() = {
    inLock(controllerContext.controllerLock) {
      info("Controller starting up")
      registerSessionExpirationListener() // note: 注册回话失效的监听器
      isRunning = true
      controllerElector.startup //note: 启动选举过程
      info("Controller startup complete")
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown() = {
    inLock(controllerContext.controllerLock) {
      isRunning = false
    }
    onControllerResignation()
  }

  def sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: AbstractResponse => Unit = null) = {
    controllerContext.controllerChannelManager.sendRequest(brokerId, apiKey, request, callback)
  }

  //note: 更新 controller 的 epoch 和 zkVersion，如果更新失败返回 ControllerMoveException 异常
  def incrementControllerEpoch(zkClient: ZkClient) = {
    try {
      var newControllerEpoch = controllerContext.epoch + 1
      val (updateSucceeded, newVersion) = zkUtils.conditionalUpdatePersistentPathIfExists(
        ZkUtils.ControllerEpochPath, newControllerEpoch.toString, controllerContext.epochZkVersion)
      if(!updateSucceeded) //note: 更新失败
        throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure")
      else { //note: 更新成功后，更新缓存中的值
        controllerContext.epochZkVersion = newVersion
        controllerContext.epoch = newControllerEpoch
      }
    } catch {
      case _: ZkNoNodeException => //note: 该节点不存在时，创建路径并更新
        // if path doesn't exist, this is the first controller whose epoch should be 1
        // the following call can still fail if another controller gets elected between checking if the path exists and
        // trying to create the controller epoch path
        try {
          zkClient.createPersistent(ZkUtils.ControllerEpochPath, KafkaController.InitialControllerEpoch.toString)
          controllerContext.epoch = KafkaController.InitialControllerEpoch
          controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion
        } catch {
          case _: ZkNodeExistsException => throw new ControllerMovedException("Controller moved to another broker. " +
            "Aborting controller startup procedure")
          case oe: Throwable => error("Error while incrementing controller epoch", oe)
        }
      case oe: Throwable => error("Error while incrementing controller epoch", oe)

    }
    info("Controller %d incremented epoch to %d".format(config.brokerId, controllerContext.epoch))
  }

  private def registerSessionExpirationListener() = {
    zkUtils.zkClient.subscribeStateChanges(new SessionExpirationListener())
  }

  //note: 初始化 KafkaController 的上下文数据
  private def initializeControllerContext() {
    // update controller cache with delete topic information
    controllerContext.liveBrokers = zkUtils.getAllBrokersInCluster().toSet //note: 初始化 zk 的 broker_list 信息
    controllerContext.allTopics = zkUtils.getAllTopics().toSet //note: 初始化所有的 topic 信息
    //note: 初始化所有 topic 的所有 partition 的 replica 分配
    controllerContext.partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(controllerContext.allTopics.toSeq)
    //note: 下面两个都是新创建的空集合
    controllerContext.partitionLeadershipInfo = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache() //note: 获取 topic-partition 的详细信息,更新到 partitionLeadershipInfo 中
    // start the channel manager
    startChannelManager() //note: 启动连接所有的 broker 的线程, 根据 broker/ids 的临时去判断要连接哪些 broker
    initializePreferredReplicaElection() //note: 初始化需要进行最优 leader 选举的 partition
    initializePartitionReassignment() //note: 初始化需要进行分区副本迁移的 partition
    initializeTopicDeletion() //note: 初始化要删除的 topic 及后台的 topic 删除线程,还有不能删除的 topic 集合
    info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
    info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds))
    info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
  }

  //note: 初始化需要进行最优 leader 选举的 Partition 记录
  private def initializePreferredReplicaElection() {
    // initialize preferred replica election state
    //note: 获取需要进行 leader 最优选举的 Topic-Partition 列表
    val partitionsUndergoingPreferredReplicaElection = zkUtils.getPartitionsUndergoingPreferredReplicaElection()
    // check if they are already completed or topic was deleted
    //note: 过滤已经成功的列表
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
      val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition) //note: Partition 的 AR
      val topicDeleted = replicasOpt.isEmpty //note: 有 AR 的话代表没有删除
      val successful = //note: leader 等于最优 leader 为 true
        if(!topicDeleted) controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == replicasOpt.get.head else false
      successful || topicDeleted
    }
    //note: 先将所有的 Partition 添加到缓存中
    controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitionsUndergoingPreferredReplicaElection
    //note: 再将已经的成功的 Partition 从缓存中移除
    controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsThatCompletedPreferredReplicaElection
    info("Partitions undergoing preferred replica election: %s".format(partitionsUndergoingPreferredReplicaElection.mkString(",")))
    info("Partitions that completed preferred replica election: %s".format(partitionsThatCompletedPreferredReplicaElection.mkString(",")))
    info("Resuming preferred replica election for partitions: %s".format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
  }

  //note: 初始化需要进行 Partition 副本选举的 Partition 记录
  private def initializePartitionReassignment() {
    // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
    //note: 从 zk 的【/admin/reassign_partitions】获取 Partition 列表
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned()
    // check if they are already completed or topic was deleted
    //note: 过滤出上面列表中已经迁移完成或者已经被删除的 topic-partition 列表
    val reassignedPartitions = partitionsBeingReassigned.filter { partition =>
      val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition._1)
      val topicDeleted = replicasOpt.isEmpty
      val successful = if (!topicDeleted) replicasOpt.get == partition._2.newReplicas else false
      topicDeleted || successful
    }.keys
    //note: 将已经完成迁移的 Partition 从 zk 和缓存中清除
    reassignedPartitions.foreach(p => removePartitionFromReassignedPartitions(p))
    var partitionsToReassign: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap
    //note: 先将所有需要迁移的 topic-partition 列表添加到集合中
    partitionsToReassign ++= partitionsBeingReassigned
    //note: 再将已经成功的 Partition 从列表中移除
    partitionsToReassign --= reassignedPartitions
    //note: 将最后的结果更新到 controller 的缓存中
    controllerContext.partitionsBeingReassigned ++= partitionsToReassign
    info("Partitions being reassigned: %s".format(partitionsBeingReassigned.toString()))
    info("Partitions already reassigned: %s".format(reassignedPartitions.toString()))
    info("Resuming reassignment of partitions: %s".format(partitionsToReassign.toString()))
  }

  //note: 初始化要删除的 topic 及后台的 topic 删除线程,还有不能删除的 topic 集合
  private def initializeTopicDeletion() {
    //note: 获取需要删除的 topic 列表
    val topicsQueuedForDeletion = zkUtils.getChildrenParentMayNotExist(ZkUtils.DeleteTopicsPath).toSet
    //note: 过滤出那些副本有在 dead broker 上的 partition 列表
    val topicsWithReplicasOnDeadBrokers = controllerContext.partitionReplicaAssignment.filter { case (_, replicas) =>
      replicas.exists(r => !controllerContext.liveBrokerIds.contains(r)) }.keySet.map(_.topic)
    //note: 需要进行最优 leader 选举的 partition 列表
    val topicsForWhichPreferredReplicaElectionIsInProgress = controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic)
    //note: 需要进行副本迁移的 partition 列表
    val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsBeingReassigned.keySet.map(_.topic)
    //note: 获取不能删除的 topic 集合（topic 有副本在 dead broker、需要进行 leader 选举、需要进行副本迁移）
    val topicsIneligibleForDeletion = topicsWithReplicasOnDeadBrokers | topicsForWhichPartitionReassignmentIsInProgress |
                                  topicsForWhichPreferredReplicaElectionIsInProgress
    info("List of topics to be deleted: %s".format(topicsQueuedForDeletion.mkString(",")))
    info("List of topics ineligible for deletion: %s".format(topicsIneligibleForDeletion.mkString(",")))
    // initialize the topic deletion manager
    //note: 初始化 topic 删除线程
    deleteTopicManager = new TopicDeletionManager(this, topicsQueuedForDeletion, topicsIneligibleForDeletion)
  }

  //note: 在 controller 启动后,触发一次 Partition 副本迁移的操作
  private def maybeTriggerPartitionReassignment() {
    controllerContext.partitionsBeingReassigned.foreach { topicPartitionToReassign =>
      initiateReassignReplicasForTopicPartition(topicPartitionToReassign._1, topicPartitionToReassign._2)
    }
  }

  //note: 在 controller 启动后,触发一次分区的最优 leader 选举操作
  private def maybeTriggerPreferredReplicaElection() {
    onPreferredReplicaElection(controllerContext.partitionsUndergoingPreferredReplicaElection.toSet)
  }

  //note: 启动 ChannelManager 线程
  private def startChannelManager() {
    controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics, threadNamePrefix)
    controllerContext.controllerChannelManager.startup()
  }

  //note: 从 zk 获取最新的 LeaderIsrAndControllerEpoch 更新到缓存中
  def updateLeaderAndIsrCache(topicAndPartitions: Set[TopicAndPartition] = controllerContext.partitionReplicaAssignment.keySet) {
    //note: 从 zk 获取 LeaderAndIsr 信息
    val leaderAndIsrInfo = zkUtils.getPartitionLeaderAndIsrForTopics(zkUtils.zkClient, topicAndPartitions)
    for((topicPartition, leaderIsrAndControllerEpoch) <- leaderAndIsrInfo)
      controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch)//note: 更新到缓存中
  }

  //note: 判断 replicas 是否都在 isr 中
  private def areReplicasInIsr(topic: String, partition: Int, replicas: Seq[Int]): Boolean = {
    zkUtils.getLeaderAndIsrForPartition(topic, partition) match {
      case Some(leaderAndIsr) =>
        val replicasNotInIsr = replicas.filterNot(r => leaderAndIsr.isr.contains(r))
        replicasNotInIsr.isEmpty
      case None => false
    }
  }

  //note: 对于重新分配副本的 Partition, 如果需要重新选举 leader,那么就重新选举 leader, 发送 LeaderAndIsr 给它的副本
  private def moveReassignedPartitionLeaderIfRequired(topicAndPartition: TopicAndPartition,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas //note: RAR
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader //note: old leader
    // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
    // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
    val oldAndNewReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)//note: RAR+OAR
    //note: 将 Topic-Partition 的 AR 设置为 RAR,防止将 old replica 添加到 isr 中
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas)
    if(!reassignedPartitionContext.newReplicas.contains(currentLeader)) { //note: leader 不在 RAR 中,重新选举 leader
      info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
        "is not in the new list of replicas %s. Re-electing leader".format(reassignedReplicas.mkString(",")))
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
    } else {//note: leader 在 RAR 中
      // check if the leader is alive or not
      if (controllerContext.liveBrokerIds.contains(currentLeader)) {
        //note: leader 是存活的,那么就发送 LeaderAndIsr 请求给它所有的副本（不修改 leader 和 isr,只更新 epoch 和 zkVersion）
        info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
          "is already in the new list of replicas %s and is alive".format(reassignedReplicas.mkString(",")))
        // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
        updateLeaderEpochAndSendRequest(topicAndPartition, oldAndNewReplicas, reassignedReplicas)
      } else { //note: leader 挂掉了,重新选举 leader
        info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
          "is already in the new list of replicas %s but is dead".format(reassignedReplicas.mkString(",")))
        partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
      }
    }
  }

  //note: 发送 StopReplica 给 old replica（重新分配后需要下线的 replica）,下线一个副本的步骤
  private def stopOldReplicasOfReassignedPartition(topicAndPartition: TopicAndPartition,
                                                   reassignedPartitionContext: ReassignedPartitionsContext,
                                                   oldReplicas: Set[Int]) {
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    // first move the replica to offline state (the controller removes it from the ISR)
    val replicasToBeDeleted = oldReplicas.map(r => PartitionAndReplica(topic, partition, r))
    //note: 状态转变为 OfflineReplica
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica)
    // send stop replica command to the old replicas
    //note: 状态转变为 ReplicaDeletionStarted
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted)
    // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
    //note: 状态转变为 ReplicaDeletionSuccessful
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful)
    //note: 状态转变为 NonExistentReplica
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica)
  }

  //note: 将 TopicPartition 的副本信息更新到 zk 及本地缓存
  private def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                                 replicas: Seq[Int]) {
    //note: 获取这个 topic 所有分区的副本分区: <TopicAndPartition, Seq[Int]>
    val partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.filter(_._1.topic.equals(topicAndPartition.topic))
    partitionsAndReplicasForThisTopic.put(topicAndPartition, replicas) //note: 更新需要更新的 Partition 分区
    updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic) //note: 将结果更新到 zk
    info("Updated assigned replicas for partition %s being reassigned to %s ".format(topicAndPartition, replicas.mkString(",")))
    // update the assigned replica list after a successful zookeeper write
    //note: 更新 controller 的缓存
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, replicas)
  }

  //note: 将迁移设置的新副本状态设置为 NewReplica
  private def startNewReplicasForReassignedPartition(topicAndPartition: TopicAndPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext,
                                                     newReplicas: Set[Int]) {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), NewReplica)
    }
  }

  //note: 不改变 leader 和 isr 的情况下,更新 epoch 和 zkVersion,并发送 LeaderAndIsr 请求给它的副本
  private def updateLeaderEpochAndSendRequest(topicAndPartition: TopicAndPartition, replicasToReceiveRequest: Seq[Int], newAssignedReplicas: Seq[Int]) {
    brokerRequestBatch.newBatch()
    //note: updateLeaderEpoch() 更新 Partition 的 epoch 和 zkVersion,不改变 leader 和 isr,更新到 zk 中
    updateLeaderEpoch(topicAndPartition.topic, topicAndPartition.partition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        try {
          //note: 添加要发送的 LeaderAndIsr 请求
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, topicAndPartition.topic,
            topicAndPartition.partition, updatedLeaderIsrAndControllerEpoch, newAssignedReplicas)
          //note: 发送 LeaderAndIsr 请求
          brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        } catch {
          case e : IllegalStateException => {
            // Resign if the controller is in an illegal state
            error("Forcing the controller to resign")
            brokerRequestBatch.clear()
            controllerElector.resign()

            throw e
          }
        }
        stateChangeLogger.trace(("Controller %d epoch %d sent LeaderAndIsr request %s with new assigned replica list %s " +
          "to leader %d for partition being reassigned %s").format(config.brokerId, controllerContext.epoch, updatedLeaderIsrAndControllerEpoch,
          newAssignedReplicas.mkString(","), updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader, topicAndPartition))
      case None => // fail the reassignment
        stateChangeLogger.error(("Controller %d epoch %d failed to send LeaderAndIsr request with new assigned replica list %s " +
          "to leader for partition being reassigned %s").format(config.brokerId, controllerContext.epoch,
          newAssignedReplicas.mkString(","), topicAndPartition))
    }
  }

  private def registerIsrChangeNotificationListener() = {
    debug("Registering IsrChangeNotificationListener")
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.IsrChangeNotificationPath, isrChangeNotificationListener)
  }

  private def deregisterIsrChangeNotificationListener() = {
    debug("De-registering IsrChangeNotificationListener")
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.IsrChangeNotificationPath, isrChangeNotificationListener)
  }

  private def registerReassignedPartitionsListener() = {
    zkUtils.zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener)
  }

  private def deregisterReassignedPartitionsListener() = {
    zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener)
  }

  private def registerPreferredReplicaElectionListener() {
    zkUtils.zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
  }

  private def deregisterPreferredReplicaElectionListener() {
    zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
  }

  private def deregisterReassignedPartitionsIsrChangeListeners() {
    controllerContext.partitionsBeingReassigned.foreach {
      case (topicAndPartition, reassignedPartitionsContext) =>
        val zkPartitionPath = getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition)
        zkUtils.zkClient.unsubscribeDataChanges(zkPartitionPath, reassignedPartitionsContext.isrChangeListener)
    }
  }

  //note: 从获取 controller 的 epoch 和 zkVersion 值
  private def readControllerEpochFromZookeeper() {
    // initialize the controller epoch and zk version by reading from zookeeper
    if(controllerContext.zkUtils.pathExists(ZkUtils.ControllerEpochPath)) {
      val epochData = controllerContext.zkUtils.readData(ZkUtils.ControllerEpochPath)
      controllerContext.epoch = epochData._1.toInt
      controllerContext.epochZkVersion = epochData._2.getVersion
      info("Initialized controller epoch to %d and zk version %d".format(controllerContext.epoch, controllerContext.epochZkVersion))
    }
  }

  //note: 将已经完成迁移的 Partition 从 zk 和缓存中清除
  def removePartitionFromReassignedPartitions(topicAndPartition: TopicAndPartition) {
    if(controllerContext.partitionsBeingReassigned.get(topicAndPartition).isDefined) { //note: 这个 Partition 在迁移的集合中
      // stop watching the ISR changes for this partition
      //note: 停止监控这个这个 partition 的变化
      zkUtils.zkClient.unsubscribeDataChanges(getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
        controllerContext.partitionsBeingReassigned(topicAndPartition).isrChangeListener)
    }
    // read the current list of reassigned partitions from zookeeper
    //note: 读取需要迁移的 Partition 列表
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned()
    // remove this partition from that list
    //note: 从需要迁移的 Partition 列表移除这个 Partition
    val updatedPartitionsBeingReassigned = partitionsBeingReassigned - topicAndPartition
    // write the new list to zookeeper
    //note: 将最新的结果再写回到 zk 中
    zkUtils.updatePartitionReassignmentData(updatedPartitionsBeingReassigned.mapValues(_.newReplicas))
    // update the cache. NO-OP if the partition's reassignment was never started
    //note: 从 controller 缓存的迁移集合中移除这个 partition
    controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
  }

  //note: 将这个 topic 分区重新分配的 replica 结果更新到 zk 中（是按照 topic 粒度更新）
  def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                         newReplicaAssignmentForTopic: Map[TopicAndPartition, Seq[Int]]) {
    try {
      val zkPath = getTopicPath(topicAndPartition.topic)
      //note: 将整个 topic 所有分区的副本分区更新到 zk
      val jsonPartitionMap = zkUtils.replicaAssignmentZkData(newReplicaAssignmentForTopic.map(e => e._1.partition.toString -> e._2))
      zkUtils.updatePersistentPath(zkPath, jsonPartitionMap)
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionMap))
    } catch {
      case _: ZkNoNodeException => throw new IllegalStateException("Topic %s doesn't exist".format(topicAndPartition.topic))
      case e2: Throwable => throw new KafkaException(e2.toString)
    }
  }

  //note: 最优 leader 选举后,触发的操作,删除触发记录和缓存信息
  def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicAndPartition],
                                                   isTriggeredByAutoRebalance : Boolean) {
    for(partition <- partitionsToBeRemoved) {
      // check the status
      //note: 当前的 leader
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
      //note: 最优的 leader（replica 集合中第一个 replica）
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if(currentLeader == preferredReplica) { //note: 选举成功
        info("Partition %s completed preferred replica leader election. New leader is %d".format(partition, preferredReplica))
      } else { //note: 选举失败
        warn("Partition %s failed to complete preferred replica leader election. Leader is %d".format(partition, currentLeader))
      }
    }
    if (!isTriggeredByAutoRebalance) //note: 如果不是自动 rebalance,那就是通过 zk 触发的,需要手动删除 zk 上触发的节点信息
      zkUtils.deletePath(ZkUtils.PreferredReplicaLeaderElectionPath)
    //note: 从 controller 的缓存中移除
    controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsToBeRemoved
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   *
   * @param brokers The brokers that the update metadata request should be sent to
   */
  //note: 将选中的 Partition 的 leader 信息发送给选定的 Broker,以 metadata 更新的形式发送 
  def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {
    try {
      brokerRequestBatch.newBatch()
      brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e : IllegalStateException => {
        // Resign if the controller is in an illegal state
        error("Forcing the controller to resign")
        brokerRequestBatch.clear()
        controllerElector.resign()

        throw e
      }
    }
  }

  /**
   * Removes a given partition replica from the ISR; if it is not the current
   * leader and there are sufficient remaining replicas in ISR.
   *
   * @param topic topic
   * @param partition partition
   * @param replicaId replica Id
   * @return the new leaderAndIsr (with the replica removed if it was present),
   *         or None if leaderAndIsr is empty.
   */
  //note: 从 isr 中将副本移除（前提 isr 中除了这个副本还有其他的有效副本）
  def removeReplicaFromIsr(topic: String, partition: Int, replicaId: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    debug("Removing replica %d from ISR %s for partition %s.".format(replicaId,
      controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.isr.mkString(","), topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false //note: 没有完成,或者写入 zk 失败
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      //note: 从 zk 上获取最新的 leader 和 isr 信息
      val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) => // increment the leader epoch even if the ISR changes
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          if (leaderAndIsr.isr.contains(replicaId)) { //note: isr 中包含了这个副本
            // if the replica to be removed from the ISR is also the leader, set the new leader value to -1
            //note: 如果这个replica 是 leader,那么将新的 leader 设置为-1
            val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
            var newIsr = leaderAndIsr.isr.filter(b => b != replicaId) //note: 从 isr 中移除这个 replica

            // if the replica to be removed from the ISR is the last surviving member of the ISR and unclean leader election
            // is disallowed for the corresponding topic, then we must preserve the ISR membership so that the replica can
            // eventually be restored as the leader.
            //note: 如果 newIsr 是空并且集群禁止脏选举,那么还将 newIsr 设置为原来的 isr（但 leader 可能已经设置为-1了）
            if (newIsr.isEmpty && !LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(zkUtils,
              ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
              info("Retaining last ISR %d of partition %s since unclean leader election is disabled".format(replicaId, topicAndPartition))
              newIsr = leaderAndIsr.isr
            }

            //note: LeaderAndIsr
            val newLeaderAndIsr = new LeaderAndIsr(newLeader, leaderAndIsr.leaderEpoch + 1,
              newIsr, leaderAndIsr.zkVersion + 1)
            // update the new leadership decision in zookeeper or retry
            //note: 将结果更新到 zk 中
            val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
              newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

            //note: 更新到 controller 的缓存中
            newLeaderAndIsr.zkVersion = newVersion
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            if (updateSucceeded)
              info("New leader and ISR for partition %s is %s".format(topicAndPartition, newLeaderAndIsr.toString()))
            updateSucceeded
          } else { //note: isr 中没有这个 replica,那么就不需要做上面的操作了
            warn("Cannot remove replica %d from ISR of partition %s since it is not in the ISR. Leader = %d ; ISR = %s"
                 .format(replicaId, topicAndPartition, leaderAndIsr.leader, leaderAndIsr.isr))
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            true
          }
        case None =>
          warn("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.".format(replicaId, topicAndPartition))
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   *
   * @param topic topic
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  //note: 不改变 leader 或 isr,仅仅增加 leader 的 epoch
  private def updateLeaderEpoch(topic: String, partition: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    debug("Updating leader epoch for partition %s.".format(topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) =>
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch) //note: Partition 的 controller epoch 大于当前的 epoch,证明 controller 已经切换,抛出异常
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          //note: 更新 epoch 和 zkVersion
          val newLeaderAndIsr = new LeaderAndIsr(leaderAndIsr.leader, leaderAndIsr.leaderEpoch + 1,
                                                 leaderAndIsr.isr, leaderAndIsr.zkVersion + 1)
          // update the new leadership decision in zookeeper or retry
          //note: 将结果更新到 zk 中,更新失败的话一直重试
          val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic,
            partition, newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

          newLeaderAndIsr.zkVersion = newVersion
          finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch))
          if (updateSucceeded)
            info("Updated leader epoch for partition %s to %d".format(topicAndPartition, newLeaderAndIsr.leaderEpoch))
          updateSucceeded
        case None =>
          throw new IllegalStateException(("Cannot update leader epoch for partition %s as leaderAndIsr path is empty. " +
            "This could mean we somehow tried to reassign a partition that doesn't exist").format(topicAndPartition))
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  class SessionExpirationListener() extends IZkStateListener with Logging {
    this.logIdent = "[SessionExpirationListener on " + config.brokerId + "], "

    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception On any error.
     */
    @throws[Exception]
    def handleNewSession() {
      info("ZK expired; shut down all controller components and try to re-elect")
      if (controllerElector.getControllerID() != config.brokerId) { //note: 会话超时,重新参与选举
        onControllerResignation()
        inLock(controllerContext.controllerLock) {
          controllerElector.elect
        }
      } else {
        // This can happen when there are multiple consecutive session expiration and handleNewSession() are called multiple
        // times. The first call may already register the controller path using the newest ZK session. Therefore, the
        // controller path will exist in subsequent calls to handleNewSession().
        info("ZK expired, but the current controller id %d is the same as this broker id, skip re-elect".format(config.brokerId))
      }
    }

    def handleSessionEstablishmentError(error: Throwable): Unit = {
      //no-op handleSessionEstablishmentError in KafkaHealthCheck should handle this error in its handleSessionEstablishmentError
    }
  }

  //note: 遍历所有 Partition,检查是否需要进行 leader 切换
  private def checkAndTriggerPartitionRebalance(): Unit = {
    if (isActive) {
      trace("checking need to trigger partition rebalance")
      // get all the active brokers
      var preferredReplicasForTopicsByBrokers: Map[Int, Map[TopicAndPartition, Seq[Int]]] = null
      inLock(controllerContext.controllerLock) {
        //note: 过滤那些没有设置删除的 Partition,得到的是 preferredReplica 与 topic-partition 的对应关系
        preferredReplicasForTopicsByBrokers =
          controllerContext.partitionReplicaAssignment.filterNot(p => deleteTopicManager.isTopicQueuedUpForDeletion(p._1.topic)).groupBy {
            case (_, assignedReplicas) => assignedReplicas.head
          }
      }
      debug("preferred replicas by broker " + preferredReplicasForTopicsByBrokers)
      // for each broker, check if a preferred replica election needs to be triggered
      //note: 遍历上面过滤的 Partition,检查是否需要触发最优 leader 选举
      preferredReplicasForTopicsByBrokers.foreach {
        case(leaderBroker, topicAndPartitionsForBroker) => {
          var imbalanceRatio: Double = 0
          var topicsNotInPreferredReplica: Map[TopicAndPartition, Seq[Int]] = null
          inLock(controllerContext.controllerLock) {
            //note: 找到当前的 leaderBroker 下需要进行最优 leader 选举的 topicPartition 列表
            topicsNotInPreferredReplica = topicAndPartitionsForBroker.filter { case (topicPartition, _) =>
              controllerContext.partitionLeadershipInfo.contains(topicPartition) &&
                controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader != leaderBroker
            }
            debug("topics not in preferred replica " + topicsNotInPreferredReplica)
            val totalTopicPartitionsForBroker = topicAndPartitionsForBroker.size
            val totalTopicPartitionsNotLedByBroker = topicsNotInPreferredReplica.size
            //note: 需要进行 leader 选举的比例
            imbalanceRatio = totalTopicPartitionsNotLedByBroker.toDouble / totalTopicPartitionsForBroker
            trace("leader imbalance ratio for broker %d is %f".format(leaderBroker, imbalanceRatio))
          }
          // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
          // that need to be on this broker
          //note: 如果需要进行 leader 选举的比例超过设置的阈值,才会进行 leader 切换
          if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
            topicsNotInPreferredReplica.keys.foreach { topicPartition =>
              inLock(controllerContext.controllerLock) {
                // do this check only if the broker is live and there are no partitions being reassigned currently
                // and preferred replica election is not in progress
                //note: 只有在 broker 存活、没有 partition 的副本正在迁移、最优 leader 选举没有正在进行时,才会触发操作
                if (controllerContext.liveBrokerIds.contains(leaderBroker) &&
                    controllerContext.partitionsBeingReassigned.isEmpty &&
                    controllerContext.partitionsUndergoingPreferredReplicaElection.isEmpty &&
                    !deleteTopicManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&
                    controllerContext.allTopics.contains(topicPartition.topic)) {
                  onPreferredReplicaElection(Set(topicPartition), true)
                }
              }
            }
          }
        }
      }
    }
  }
}

/**
 * Starts the partition reassignment process unless -
 * 1. Partition previously existed
 * 2. New replicas are the same as existing replicas
 * 3. Any replica in the new set of replicas are dead
 * If any of the above conditions are satisfied, it logs an error and removes the partition from list of reassigned
 * partitions.
 */
//note: 开始进行 partition reassignment 除非这三种情况发生:
//note: 1. 这个 partition 的 reassignment 之前已经存在;
//note: 2. new replica 与已经存在的 replicas 相同
//note: 3. replicas 的所有 replica 都已经 dead;
//note: 这种情况发生时,会输出一条日志,并移除该 reassignment
class PartitionsReassignedListener(protected val controller: KafkaController) extends ControllerZkDataListener {
  private val controllerContext = controller.controllerContext

  protected def logName = "PartitionsReassignedListener"

  /**
   * Invoked when some partitions are reassigned by the admin command
   *
   * @throws Exception On any error.
   */
  //note: 当一些分区需要进行迁移时
  @throws[Exception]
  def doHandleDataChange(dataPath: String, data: AnyRef) {
    debug("Partitions reassigned listener fired for path %s. Record partitions to be reassigned %s"
      .format(dataPath, data))
    val partitionsReassignmentData = ZkUtils.parsePartitionReassignmentData(data.toString)
    val partitionsToBeReassigned = inLock(controllerContext.controllerLock) { //note: 需要迁移的新副本
      partitionsReassignmentData.filterNot(p => controllerContext.partitionsBeingReassigned.contains(p._1))
    }
    partitionsToBeReassigned.foreach { partitionToBeReassigned =>
      inLock(controllerContext.controllerLock) {
        if(controller.deleteTopicManager.isTopicQueuedUpForDeletion(partitionToBeReassigned._1.topic)) {
          //note: 如果这个 topic 已经设置了删除，那么就不会进行迁移了（从需要副本迁移的集合中移除）
          error("Skipping reassignment of partition %s for topic %s since it is currently being deleted"
            .format(partitionToBeReassigned._1, partitionToBeReassigned._1.topic))
          controller.removePartitionFromReassignedPartitions(partitionToBeReassigned._1)
        } else { //note: 添加到需要迁移的副本集合中
          val context = new ReassignedPartitionsContext(partitionToBeReassigned._2)
          controller.initiateReassignReplicasForTopicPartition(partitionToBeReassigned._1, context)
        }
      }
    }
  }

  def doHandleDataDeleted(dataPath: String) {}
}

//note: ISR 变动的监听器（这个不是由 leader 主动触发的，而是 controller 自己触发的，主要用于 partition 迁移时，isr 变动的监听处理）
class ReassignedPartitionsIsrChangeListener(protected val controller: KafkaController, topic: String, partition: Int,
                                            reassignedReplicas: Set[Int]) extends ControllerZkDataListener {
  private val zkUtils = controller.controllerContext.zkUtils
  private val controllerContext = controller.controllerContext

  protected def logName = "ReassignedPartitionsIsrChangeListener"

  /**
   * Invoked when some partitions need to move leader to preferred replica
   */
  def doHandleDataChange(dataPath: String, data: AnyRef) {
    inLock(controllerContext.controllerLock) {
      debug("Reassigned partitions isr change listener fired for path %s with children %s".format(dataPath, data))
      val topicAndPartition = TopicAndPartition(topic, partition)
      try {
        // check if this partition is still being reassigned or not
        controllerContext.partitionsBeingReassigned.get(topicAndPartition) match {
          case Some(reassignedPartitionContext) =>
            // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object
            val newLeaderAndIsrOpt = zkUtils.getLeaderAndIsrForPartition(topic, partition)
            newLeaderAndIsrOpt match {
              case Some(leaderAndIsr) => // check if new replicas have joined ISR
                val caughtUpReplicas = reassignedReplicas & leaderAndIsr.isr.toSet
                if(caughtUpReplicas == reassignedReplicas) {
                  // resume the partition reassignment process
                  info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                    .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                    "Resuming partition reassignment")
                  controller.onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
                }
                else {
                  info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                    .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                    "Replica(s) %s still need to catch up".format((reassignedReplicas -- leaderAndIsr.isr.toSet).mkString(",")))
                }
              case None => error("Error handling reassignment of partition %s to replicas %s as it was never created"
                .format(topicAndPartition, reassignedReplicas.mkString(",")))
            }
          case None =>
        }
      } catch {
        case e: Throwable => error("Error while handling partition reassignment", e)
      }
    }
  }

  def doHandleDataDeleted(dataPath: String) {}

}

/**
 * Called when leader intimates of isr change
 *
 * @param controller
 */
//note: ISR 变动时触发,监控【/isr_change_notification】节点
class IsrChangeNotificationListener(protected val controller: KafkaController) extends ControllerZkChildListener {

  protected def logName = "IsrChangeNotificationListener"

  //note: Topic-Partition 的 isr 变化时,从 zk 获取最新的 LeaderAndIsr 信息,更新到缓存中,并将结果以 metadata 更新的形式发给所有的 broker
  def doHandleChildChange(parentPath: String, currentChildren: Seq[String]): Unit = {
    inLock(controller.controllerContext.controllerLock) {
      debug("ISR change notification listener fired")
      try {
        val topicAndPartitions = currentChildren.flatMap(getTopicAndPartition).toSet
        if (topicAndPartitions.nonEmpty) {
          controller.updateLeaderAndIsrCache(topicAndPartitions) //note:从 zk 获取最新的 leaderAndIsr,更新到缓存中
          processUpdateNotifications(topicAndPartitions)
        }
      } finally {
        // delete processed children
        currentChildren.map(x => controller.controllerContext.zkUtils.deletePath(
          ZkUtils.IsrChangeNotificationPath + "/" + x))
      }
    }
  }

  //note: 发送 metadata update
  private def processUpdateNotifications(topicAndPartitions: immutable.Set[TopicAndPartition]) {
    val liveBrokers: Seq[Int] = controller.controllerContext.liveOrShuttingDownBrokerIds.toSeq
    debug("Sending MetadataRequest to Brokers:" + liveBrokers + " for TopicAndPartitions:" + topicAndPartitions)
    controller.sendUpdateMetadataRequest(liveBrokers, topicAndPartitions)
  }

  //note: 获取 isr 变动的 Topic-Partition 列表
  private def getTopicAndPartition(child: String): Set[TopicAndPartition] = {
    val changeZnode: String = ZkUtils.IsrChangeNotificationPath + "/" + child
    val (jsonOpt, _) = controller.controllerContext.zkUtils.readDataMaybeNull(changeZnode)
    if (jsonOpt.isDefined) {
      val json = Json.parseFull(jsonOpt.get)

      json match {
        case Some(m) =>
          val topicAndPartitions: mutable.Set[TopicAndPartition] = new mutable.HashSet[TopicAndPartition]()
          val isrChanges = m.asInstanceOf[Map[String, Any]]
          val topicAndPartitionList = isrChanges("partitions").asInstanceOf[List[Any]]
          topicAndPartitionList.foreach {
            case tp =>
              val topicAndPartition = tp.asInstanceOf[Map[String, Any]]
              val topic = topicAndPartition("topic").asInstanceOf[String]
              val partition = topicAndPartition("partition").asInstanceOf[Int]
              topicAndPartitions += TopicAndPartition(topic, partition)
          }
          topicAndPartitions
        case None =>
          error("Invalid topic and partition JSON: " + jsonOpt.get + " in ZK: " + changeZnode)
          Set.empty
      }
    } else {
      Set.empty
    }
  }
}

object IsrChangeNotificationListener {
  val version: Long = 1L
}

/**
 * Starts the preferred replica leader election for the list of partitions specified under
 * /admin/preferred_replica_election -
 */
//note: 最优 leader 选举监听器, 监听【/admin/preferred_replica_election】
class PreferredReplicaElectionListener(protected val controller: KafkaController) extends ControllerZkDataListener {
  private val controllerContext = controller.controllerContext

  protected def logName = "PreferredReplicaElectionListener"

  /**
   * Invoked when some partitions are reassigned by the admin command
   *
   * @throws Exception On any error.
   */
  @throws[Exception]
  def doHandleDataChange(dataPath: String, data: AnyRef) {
    debug("Preferred replica election listener fired for path %s. Record partitions to undergo preferred replica election %s"
            .format(dataPath, data.toString))
    inLock(controllerContext.controllerLock) {
      //note: 下面的 Partition 需要进行最优 leader 选举
      val partitionsForPreferredReplicaElection = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(data.toString)
      if (controllerContext.partitionsUndergoingPreferredReplicaElection.nonEmpty)
        info("These partitions are already undergoing preferred replica election: %s"
          .format(controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
      //note: 获取已经在进行选举,剩下的 Partition 集合
      val partitions = partitionsForPreferredReplicaElection -- controllerContext.partitionsUndergoingPreferredReplicaElection
      //note: 过滤出需要删除的 topic 列表
      val partitionsForTopicsToBeDeleted = partitions.filter(p => controller.deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
      if (partitionsForTopicsToBeDeleted.nonEmpty) {
        error("Skipping preferred replica election for partitions %s since the respective topics are being deleted"
          .format(partitionsForTopicsToBeDeleted))
      }
      //note: 经过前面两步的过滤,对下面剩余的 topic 进行最优 leader 选举操作
      controller.onPreferredReplicaElection(partitions -- partitionsForTopicsToBeDeleted)
    }
  }

  def doHandleDataDeleted(dataPath: String) {}
}

//note: 分区副本迁移<新副本, ISR 变动监听器>
case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       var isrChangeListener: ReassignedPartitionsIsrChangeListener = null)

case class PartitionAndReplica(topic: String, partition: Int, replica: Int) {
  override def toString: String = {
    "[Topic=%s,Partition=%d,Replica=%d]".format(topic, partition, replica)
  }
}

//note: Partition 的 LeaderAndIsr 信息
case class LeaderIsrAndControllerEpoch(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString: String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

object ControllerStats extends KafkaMetricsGroup {

  private val _uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)
  private val _leaderElectionTimer = new KafkaTimer(newTimer("LeaderElectionRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))

  // KafkaServer needs to initialize controller metrics during startup. We perform initialization
  // through method calls to avoid Scala compiler warnings.
  def uncleanLeaderElectionRate: Meter = _uncleanLeaderElectionRate

  def leaderElectionTimer: KafkaTimer = _leaderElectionTimer
}
