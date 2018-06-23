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
package kafka.server

import java.io.{File, IOException}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.cluster.{Partition, Replica}
import kafka.common._
import kafka.controller.KafkaController
import kafka.log.{Log, LogAppendInfo, LogManager}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.utils._
import org.apache.kafka.common.errors.{ControllerMovedException, CorruptRecordException, InvalidTimestampException, InvalidTopicException, NotLeaderForPartitionException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException, ReplicaNotAvailableException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, PartitionState, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.requests.FetchRequest.PartitionData

import scala.collection._
import scala.collection.JavaConverters._

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/*
 * Result metadata of a log read operation on the log
 * @param info @FetchDataInfo returned by the @Log read
 * @param hw high watermark of the local replica
 * @param readSize amount of data that was read from the log i.e. size of the fetch
 * @param isReadFromLogEnd true if the request read up to the log end offset snapshot
 *                         when the read was initiated, false otherwise
 * @param error Exception if error encountered while reading from the log
 */
case class LogReadResult(info: FetchDataInfo,
                         hw: Long,
                         leaderLogEndOffset: Long,
                         fetchTimeMs: Long,
                         readSize: Int,
                         exception: Option[Throwable] = None) {

  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }

  override def toString =
    s"Fetch Data: [$info], HW: [$hw], leaderLogEndOffset: [$leaderLogEndOffset], readSize: [$readSize], error: [$error]"

}

case class FetchPartitionData(error: Errors = Errors.NONE, hw: Long = -1L, records: Records)

object LogReadResult {
  val UnknownLogReadResult = LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                                           hw = -1L,
                                           leaderLogEndOffset = -1L,
                                           fetchTimeMs = -1L,
                                           readSize = -1)
}

case class BecomeLeaderOrFollowerResult(responseMap: collection.Map[TopicPartition, Short], errorCode: Short) {

  override def toString = {
    "update results: [%s], global error: [%d]".format(responseMap, errorCode)
  }
}

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L
}

class ReplicaManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     val zkUtils: ZkUtils,
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean,
                     quotaManager: ReplicationQuotaManager,
                     threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  /* epoch of the controller that last changed the leader */
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  private val localBrokerId = config.brokerId
  //note: 管理所有的 partition 对象
  private val allPartitions = new Pool[TopicPartition, Partition](valueFactory = Some(tp =>
    new Partition(tp.topic, tp.partition, time, this)))
  private val replicaStateChangeLock = new Object
  val replicaFetcherManager = new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  val highWatermarkCheckpoints = config.logDirs.map(dir => (new File(dir).getAbsolutePath, new OffsetCheckpoint(new File(dir, ReplicaManager.HighWatermarkFilename)))).toMap
  private var hwThreadInitialized = false
  this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: "
  val stateChangeLogger = KafkaController.stateChangeLogger
  private val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  private val lastIsrChangeMs = new AtomicLong(System.currentTimeMillis())
  private val lastIsrPropagationMs = new AtomicLong(System.currentTimeMillis())

  val delayedProducePurgatory = DelayedOperationPurgatory[DelayedProduce](
    purgatoryName = "Produce", localBrokerId, config.producerPurgatoryPurgeIntervalRequests)
  val delayedFetchPurgatory = DelayedOperationPurgatory[DelayedFetch](
    purgatoryName = "Fetch", localBrokerId, config.fetchPurgatoryPurgeIntervalRequests)

  val leaderCount = newGauge(
    "LeaderCount",
    new Gauge[Int] {
      def value = {
          getLeaderPartitions().size
      }
    }
  )
  val partitionCount = newGauge(
    "PartitionCount",
    new Gauge[Int] {
      def value = allPartitions.size
    }
  )
  val underReplicatedPartitions = newGauge(
    "UnderReplicatedPartitions",
    new Gauge[Int] {
      def value = underReplicatedPartitionCount()
    }
  )
  val isrExpandRate = newMeter("IsrExpandsPerSec",  "expands", TimeUnit.SECONDS)
  val isrShrinkRate = newMeter("IsrShrinksPerSec",  "shrinks", TimeUnit.SECONDS)

  //note: 查看当 broker 的 leader 有多少是 under replica
  def underReplicatedPartitionCount(): Int = {
      getLeaderPartitions().count(_.isUnderReplicated)
  }

  def startHighWaterMarksCheckPointThread() = {
    if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  def recordIsrChange(topicPartition: TopicPartition) {
    isrChangeSet synchronized {
      isrChangeSet += topicPartition
      lastIsrChangeMs.set(System.currentTimeMillis())
    }
  }
  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   */
  //note: 这个方法是周期性的运行,来判断 partition 的 isr 是否需要更新,
  def maybePropagateIsrChanges() {
    val now = System.currentTimeMillis()
    isrChangeSet synchronized {
      if (isrChangeSet.nonEmpty && //note:  有 topic-partition 的 isr 需要更新
        (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now || //note: 5s 内没有触发过
          lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) { //note: 距离上次触发有60s
        ReplicationUtils.propagateIsrChanges(zkUtils, isrChangeSet) //note: 在 zk 创建 isr 变动的提醒
        isrChangeSet.clear() //note: 清空 isrChangeSet,它记录着 isr 变动的 topic-partition 信息
        lastIsrPropagationMs.set(now) //note: 最近一次触发这个方法的时间
      }
    }
  }

  def getLog(topicPartition: TopicPartition): Option[Log] = logManager.getLog(topicPartition)

  /**
   * Try to complete some delayed produce requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for acks = -1)
   * 2. A follower replica's fetch operation is received (for acks > 1)
   */
  //note: 完成延迟的 produce 请求，会被以下两种情况触发：
  //note: 1.对于 acks=-1 的 partition，其 HW 变化；
  //note: 2.acks>1, follower replica 同步请求收到返回；
  def tryCompleteDelayedProduce(key: DelayedOperationKey) {
    val completed = delayedProducePurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d producer requests.".format(key.keyLabel, completed))
  }

  /**
   * Try to complete some delayed fetch requests with the request key;
   * this can be triggered when:
   *
   * 1. The partition HW has changed (for regular fetch)
   * 2. A new message set is appended to the local log (for follower fetch)
   */
  def tryCompleteDelayedFetch(key: DelayedOperationKey) {
    val completed = delayedFetchPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d fetch requests.".format(key.keyLabel, completed))
  }

  def startup() {
    // start ISR expiration thread
    // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
    //note: 周期性检查 isr 是否有 replica 过期需要从 isr 中移除
    scheduler.schedule("isr-expiration", maybeShrinkIsr, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
    //note: 周期性检查是不是有 topic-partition 的 isr 需要变动,如果需要,就更新到 zk 上,来触发 controller
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges, period = 2500L, unit = TimeUnit.MILLISECONDS)
  }

  //note: 停止副本同步，这个方法主要是删除副本文件
  def stopReplica(topicPartition: TopicPartition, deletePartition: Boolean): Short  = {
    stateChangeLogger.trace(s"Broker $localBrokerId handling stop replica (delete=$deletePartition) for partition $topicPartition")
    val errorCode = Errors.NONE.code
    getPartition(topicPartition) match {
      case Some(_) =>
        if (deletePartition) { //note: delete = true，从物理上删除文件的情况
          val removedPartition = allPartitions.remove(topicPartition)
          if (removedPartition != null) {
            removedPartition.delete() // this will delete the local log
            val topicHasPartitions = allPartitions.keys.exists(tp => topicPartition.topic == tp.topic)
            if (!topicHasPartitions)
              BrokerTopicStats.removeMetrics(topicPartition.topic)
          }
        }
      case None =>
        // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
        // This could happen when topic is being deleted while broker is down and recovers.
        if (deletePartition && logManager.getLog(topicPartition).isDefined)
          logManager.asyncDelete(topicPartition)
        stateChangeLogger.trace(s"Broker $localBrokerId ignoring stop replica (delete=$deletePartition) for partition $topicPartition as replica doesn't exist on broker")
    }
    stateChangeLogger.trace(s"Broker $localBrokerId finished handling stop replica (delete=$deletePartition) for partition $topicPartition")
    errorCode
  }

  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicPartition, Short], Short) = {
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[TopicPartition, Short]
      if(stopReplicaRequest.controllerEpoch() < controllerEpoch) {
        stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d. Latest known controller epoch is %d"
          .format(localBrokerId, stopReplicaRequest.controllerEpoch, controllerEpoch))
        (responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
      } else {
        val partitions = stopReplicaRequest.partitions.asScala //note: 要停止同步的 topic-partiiton 列表
        controllerEpoch = stopReplicaRequest.controllerEpoch
        // First stop fetchers for all partitions, then stop the corresponding replicas
        replicaFetcherManager.removeFetcherForPartitions(partitions) //note: 停止副本同步
        for (topicPartition <- partitions){
          val errorCode = stopReplica(topicPartition, stopReplicaRequest.deletePartitions)
          responseMap.put(topicPartition, errorCode)
        }
        (responseMap, Errors.NONE.code)
      }
    }
  }

  def getOrCreatePartition(topicPartition: TopicPartition): Partition =
    allPartitions.getAndMaybePut(topicPartition)

  def getPartition(topicPartition: TopicPartition): Option[Partition] =
    Option(allPartitions.get(topicPartition))

  def getReplicaOrException(topicPartition: TopicPartition): Replica = {
    getReplica(topicPartition).getOrElse {
      throw new ReplicaNotAvailableException(s"Replica $localBrokerId is not available for partition $topicPartition")
    }
  }

  //note: 获取 tp 的 leader replica
  def getLeaderReplicaIfLocal(topicPartition: TopicPartition): Replica =  {
    val partitionOpt = getPartition(topicPartition) //note: 获取对应的 Partiion 对象
    partitionOpt match {
      case None =>
        throw new UnknownTopicOrPartitionException(s"Partition $topicPartition doesn't exist on $localBrokerId")
      case Some(partition) =>
        partition.leaderReplicaIfLocal match {
          case Some(leaderReplica) => leaderReplica //note: 返回 leader 对应的副本
          case None =>
            throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
        }
    }
  }

  def getReplica(topicPartition: TopicPartition, replicaId: Int = localBrokerId): Option[Replica] =
    getPartition(topicPartition).flatMap(_.getReplica(replicaId))

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied
   */
  //note: 向 partition 的 leader 写入数据
  def appendRecords(timeout: Long,
                    requiredAcks: Short,
                    internalTopicsAllowed: Boolean,
                    entriesPerPartition: Map[TopicPartition, MemoryRecords],
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit) {

    if (isValidRequiredAcks(requiredAcks)) { //note: acks 设置有效
      val sTime = time.milliseconds
      //note: 向本地的副本 log 追加数据
      val localProduceResults = appendToLocalLog(internalTopicsAllowed, entriesPerPartition, requiredAcks)
      debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  result.info.lastOffset + 1, // required offset
                  new PartitionResponse(result.error, result.info.firstOffset, result.info.logAppendTime)) // response status
      }

      if (delayedRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
        //note: 处理 ack=-1 的情况,需要等到 isr 的 follower 都写入成功的话,才能返回最后结果
        // create delayed produce operation
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        //note: 延迟 produce 请求
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback)

        // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
        val producerRequestKeys = entriesPerPartition.keys.map(new TopicPartitionOperationKey(_)).toSeq

        // try to complete the request immediately, otherwise put it into the purgatory
        // this is because while the delayed produce operation is being created, new
        // requests may arrive and hence make this operation completable.
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else {
        // we can respond immediately
        //note: 通过回调函数直接返回结果
        val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
        responseCallback(produceResponseStatus)
      }
    } else {
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      //note: 返回 INVALID_REQUIRED_ACKS 错误
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,
          LogAppendInfo.UnknownLogAppendInfo.firstOffset, Record.NO_TIMESTAMP)
      }
      responseCallback(responseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  //note: 是否需要延迟 produce 返回结果,下面三个条件都满足时,才为 true
  //note: 1. acks=-1; 2. 发送消息的有数据; 3. 至少有一个写入到 leader 是成功的（如果写入 leader 都失败了,那就没必要了）
  private def delayedRequestRequired(requiredAcks: Short,
                                     entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                     localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    entriesPerPartition.nonEmpty &&
    localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   */
  //note: 向本地的 replica 写入数据
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               entriesPerPartition: Map[TopicPartition, MemoryRecords],
                               requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {
    trace("Append [%s] to local log ".format(entriesPerPartition))
    entriesPerPartition.map { case (topicPartition, records) => //note: 遍历要写的所有 topic-partition
      BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).totalProduceRequestRate.mark()
      BrokerTopicStats.getBrokerAllTopicsStats().totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      //note: 不能向 kafka 内部使用的 topic 追加数据
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
      } else {
        try {
          //note: 查找对应的 Partition,并向分区对应的副本写入数据文件
          val partitionOpt = getPartition(topicPartition) //note: 获取 topic-partition 的 Partition 对象
          val info = partitionOpt match {
            case Some(partition) =>
              partition.appendRecordsToLeader(records, requiredAcks) //note: 如果找到了这个对象,就开始追加日志
            case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
              .format(topicPartition, localBrokerId)) //note: 没有找到的话,返回异常
          }

          //note: 追加的 msg 数
          val numAppendedMessages =
            if (info.firstOffset == -1L || info.lastOffset == -1L)
              0
            else
              info.lastOffset - info.firstOffset + 1

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          //note:  更新 metrics
          BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
          BrokerTopicStats.getBrokerAllTopicsStats.bytesInRate.mark(records.sizeInBytes)
          BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          BrokerTopicStats.getBrokerAllTopicsStats.messagesInRate.mark(numAppendedMessages)

          trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
            .format(records.sizeInBytes, topicPartition.topic, topicPartition.partition, info.firstOffset, info.lastOffset))
          (topicPartition, LogAppendResult(info))
        } catch { //note: 处理追加过程中出现的异常
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e: KafkaStorageException =>
            fatal("Halting due to unrecoverable I/O error while handling produce request: ", e)
            Runtime.getRuntime.halt(1)
            (topicPartition, null)
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: InvalidTimestampException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case t: Throwable =>
            BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).failedProduceRequestRate.mark()
            BrokerTopicStats.getBrokerAllTopicsStats.failedProduceRequestRate.mark()
            error("Error processing append operation on partition %s".format(topicPartition), t)
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(t)))
        }
      }
    }
  }

  /**
   * Fetch messages from the leader replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied
   */
  //note: 从 leader 拉取数据,等待拉取到足够的数据或者达到 timeout 时间后返回拉取的结果
  def fetchMessages(timeout: Long,
                    replicaId: Int, //note: 副本的编号，如果来自 consumer，则没有该编号
                    fetchMinBytes: Int, //note: 最小拉取字节数
                    fetchMaxBytes: Int, //note: 最大拉取字节数
                    hardMaxBytesLimit: Boolean,
                    fetchInfos: Seq[(TopicPartition, PartitionData)],
                    quota: ReplicaQuota = UnboundedQuota,
                    responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit) {
    val isFromFollower = replicaId >= 0 //note: 判断请求是来自 consumer （这个值为 -1）还是副本同步
    //note: 默认都是从 leader 拉取，推测这个值只是为了后续能从 follower 消费数据而设置的
    val fetchOnlyFromLeader: Boolean = replicaId != Request.DebuggingConsumerId
    //note: 如果拉取请求来自 consumer（true）,只拉取 HW 以内的数据,如果是来自 Replica 同步,则没有该限制（false）。
    val fetchOnlyCommitted: Boolean = ! Request.isValidBrokerId(replicaId)

    // read from local logs
    //note：获取本地日志
    val logReadResults = readFromLocalLog(
      replicaId = replicaId,
      fetchOnlyFromLeader = fetchOnlyFromLeader,
      readOnlyCommitted = fetchOnlyCommitted,
      fetchMaxBytes = fetchMaxBytes,
      hardMaxBytesLimit = hardMaxBytesLimit, //note: version<=2
      readPartitionInfo = fetchInfos,
      quota = quota)

    // if the fetch comes from the follower,
    // update its corresponding log end offset
    //note: 如果 fetch 来自 broker 的副本同步,那么就更新相关的 log end offset
    if(Request.isValidBrokerId(replicaId))
      updateFollowerLogReadResults(replicaId, logReadResults)

    // check if this fetch request can be satisfied right away
    val logReadResultValues = logReadResults.map { case (_, v) => v } //note: 获取结果的 value 值
    val bytesReadable = logReadResultValues.map(_.info.records.sizeInBytes).sum //note: 获取 bytes 大小
    val errorReadingData = logReadResultValues.foldLeft(false) ((errorIncurred, readResult) =>
      errorIncurred || (readResult.error != Errors.NONE))

    // respond immediately if 1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    //note: 如果满足以下条件的其中一个,将会立马返回结果:
    //note: 1. timeout 达到; 2. 拉取结果为空; 3. 拉取到足够的数据; 4. 拉取是遇到 error
    if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        tp -> FetchPartitionData(result.error, result.hw, result.info.records)
      }
      responseCallback(fetchPartitionData)
    } else {
      //note： 其他情况下,延迟发送结果
      // construct the fetch results from the read results
      val fetchPartitionStatus = logReadResults.map { case (topicPartition, result) =>
        val fetchInfo = fetchInfos.collectFirst {
          case (tp, v) if tp == topicPartition => v
        }.getOrElse(sys.error(s"Partition $topicPartition not found in fetchInfos"))
        (topicPartition, FetchPartitionStatus(result.info.fetchOffsetMetadata, fetchInfo))
      }
      val fetchMetadata = FetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit, fetchOnlyFromLeader,
        fetchOnlyCommitted, isFromFollower, replicaId, fetchPartitionStatus)
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => new TopicPartitionOperationKey(tp) }

      // try to complete the request immediately, otherwise put it into the purgatory;
      // this is because while the delayed fetch operation is being created, new requests
      // may arrive and hence make this operation completable.
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  //note: 按 offset 从 tp 列表中读取相应的数据
  def readFromLocalLog(replicaId: Int,
                       fetchOnlyFromLeader: Boolean,
                       readOnlyCommitted: Boolean,
                       fetchMaxBytes: Int,
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)],
                       quota: ReplicaQuota): Seq[(TopicPartition, LogReadResult)] = {

    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      val offset = fetchInfo.offset
      val partitionFetchSize = fetchInfo.maxBytes

      BrokerTopicStats.getBrokerTopicStats(tp.topic).totalFetchRequestRate.mark()
      BrokerTopicStats.getBrokerAllTopicsStats().totalFetchRequestRate.mark()

      try {
        trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
          s"remaining response limit $limitBytes" +
          (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        // decide whether to only fetch from leader
        //note: 根据决定 [是否只从 leader 读取数据] 来获取相应的副本
        //note: 根据 tp 获取 Partition 对象, 在获取相应的 Replica 对象
        val localReplica = if (fetchOnlyFromLeader)
          getLeaderReplicaIfLocal(tp)
        else
          getReplicaOrException(tp)

        // decide whether to only fetch committed data (i.e. messages below high watermark)
        //note: 获取 max_offset 值（如果 readOnlyCommitted 为 true，返回 HW）
        //note: 获取 hw 位置,副本同步不需要
        val maxOffsetOpt = if (readOnlyCommitted)
          Some(localReplica.highWatermark.messageOffset)
        else
          None

        /* Read the LogOffsetMetadata prior to performing the read from the log.
         * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not.
         * Using the log end offset after performing the read can lead to a race condition
         * where data gets appended to the log immediately after the replica has consumed from it
         * This can cause a replica to always be out of sync.
         */
        //note: Replica 的相关 offset 信息
        val initialLogEndOffset = localReplica.logEndOffset.messageOffset //note: the end offset
        val initialHighWatermark = localReplica.highWatermark.messageOffset //note: hw
        val fetchTimeMs = time.milliseconds
        val logReadInfo = localReplica.log match {
          case Some(log) =>
            //note: partition fetch size 的最大值
            val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)

            // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
            //note: 从指定的 offset 位置开始读取数据
            val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage)

            // If the partition is being throttled, simply return an empty set.
            if (shouldLeaderThrottle(quota, tp, replicaId)) //note: 如果被限速了,那么返回 空 集合
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
            // progress in such cases and don't need to report a `RecordTooLargeException`
            else if (!hardMaxBytesLimit && fetch.firstEntryIncomplete)
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            else fetch

          case None =>
            error(s"Leader for partition $tp does not have a local log")
            FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY)
        }

        //note: 返回最后的结果,返回的都是 LogReadResult 对象
        LogReadResult(info = logReadInfo,
                      hw = initialHighWatermark,
                      leaderLogEndOffset = initialLogEndOffset,
                      fetchTimeMs = fetchTimeMs,
                      readSize = partitionFetchSize,
                      exception = None)
      } catch {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderForPartitionException |
                 _: ReplicaNotAvailableException |
                 _: OffsetOutOfRangeException) =>
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                        hw = -1L,
                        leaderLogEndOffset = -1L,
                        fetchTimeMs = -1L,
                        readSize = partitionFetchSize,
                        exception = Some(e))
        case e: Throwable =>
          BrokerTopicStats.getBrokerTopicStats(tp.topic).failedFetchRequestRate.mark()
          BrokerTopicStats.getBrokerAllTopicsStats().failedFetchRequestRate.mark()
          error(s"Error processing fetch operation on partition $tp, offset $offset", e)
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                        hw = -1L,
                        leaderLogEndOffset = -1L,
                        fetchTimeMs = -1L,
                        readSize = partitionFetchSize,
                        exception = Some(e))
      }
    }

    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    var minOneMessage = !hardMaxBytesLimit
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage) //note: 读取该 tp 的数据
      val messageSetSize = readResult.info.records.sizeInBytes
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      if (messageSetSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - messageSetSize)
      result += (tp -> readResult)
    }
    result
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, topicPartition: TopicPartition, replicaId: Int): Boolean = {
    val isReplicaInSync = getPartition(topicPartition).flatMap { partition =>
      partition.getReplica(replicaId).map(partition.inSyncReplicas.contains)
    }.getOrElse(false)
    quota.isThrottled(topicPartition) && quota.isQuotaExceeded && !isReplicaInSync
  }

  def getMagicAndTimestampType(topicPartition: TopicPartition): Option[(Byte, TimestampType)] =
    getReplica(topicPartition).flatMap { replica =>
      replica.log.map(log => (log.config.messageFormatVersion.messageFormatVersion, log.config.messageTimestampType))
    }

  //note: Controller 向所有的 Broker 发送请求,让它们去更新各自的 meta 信息
  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest, metadataCache: MetadataCache) : Seq[TopicPartition] =  {
    replicaStateChangeLock synchronized {
      if(updateMetadataRequest.controllerEpoch < controllerEpoch) { //note: 来自过期的 controller
        val stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +
          "old controller %d with epoch %d. Latest known controller epoch is %d").format(localBrokerId,
          correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch,
          controllerEpoch)
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateControllerEpochErrorMessage)
      } else {
        //note: 更新 metadata 信息,并返回需要删除的 Partition 信息
        val deletedPartitions = metadataCache.updateCache(correlationId, updateMetadataRequest)
        controllerEpoch = updateMetadataRequest.controllerEpoch
        deletedPartitions
      }
    }
  }

  //note: 处理 LeaderAndIsr 请求
  def becomeLeaderOrFollower(correlationId: Int,leaderAndISRRequest: LeaderAndIsrRequest,
                             metadataCache: MetadataCache,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): BecomeLeaderOrFollowerResult = {
    leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
      stateChangeLogger.trace("Broker %d received LeaderAndIsr request %s correlation id %d from controller %d epoch %d for partition [%s,%d]"
                                .format(localBrokerId, stateInfo, correlationId,
                                        leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch, topicPartition.topic, topicPartition.partition))
    }
    replicaStateChangeLock synchronized {
      val responseMap = new mutable.HashMap[TopicPartition, Short]
      if (leaderAndISRRequest.controllerEpoch < controllerEpoch) { //note: 验证 controller 的 epoch，如果是来自旧的 controller，就拒绝这个请求
        stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d since " +
          "its controller epoch %d is old. Latest known controller epoch is %d").format(localBrokerId, leaderAndISRRequest.controllerId,
          correlationId, leaderAndISRRequest.controllerEpoch, controllerEpoch))
        BecomeLeaderOrFollowerResult(responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
      } else { //note: 当前 controller 的请求
        val controllerId = leaderAndISRRequest.controllerId
        controllerEpoch = leaderAndISRRequest.controllerEpoch

        // First check partition's leader epoch
        //note: 检查 leader epoch，得到一个 partitionState map，epoch 满足条件并且有副本在本地的集合
        val partitionState = new mutable.HashMap[Partition, PartitionState]()
        leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
          val partition = getOrCreatePartition(topicPartition) //note: 对应的 tp 如果没有 Partition 实例的话,就新建一个
          val partitionLeaderEpoch = partition.getLeaderEpoch //note: 更新 leader epoch
          // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
          // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
          if (partitionLeaderEpoch < stateInfo.leaderEpoch) {
            if(stateInfo.replicas.contains(localBrokerId))
              partitionState.put(partition, stateInfo)  //note: 更新 replica 的 stateInfo
            else {
              stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
                "epoch %d for partition [%s,%d] as itself is not in assigned replica list %s")
                .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                  topicPartition.topic, topicPartition.partition, stateInfo.replicas.asScala.mkString(",")))
              responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
            }
          } else {  //note: 忽略这个请求，因为请求的 leader epoch 小于缓存的 epoch
            // Otherwise record the error code in response
            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
              "epoch %d for partition [%s,%d] since its associated leader epoch %d is not higher than the current leader epoch %d")
              .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                topicPartition.topic, topicPartition.partition, stateInfo.leaderEpoch, partitionLeaderEpoch))
            responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH.code)
          }
        }

        //note: 过滤出本地副本设置为 leader 的 Partition 列表
        val partitionsTobeLeader = partitionState.filter { case (_, stateInfo) =>
          stateInfo.leader == localBrokerId
        }
        //note: 过滤出本地副本设置为 follower 的 Partition 列表
        val partitionsToBeFollower = partitionState -- partitionsTobeLeader.keys //note: 这些 tp 设置为了 follower

        //note: 将该副本设置为 leader
        val partitionsBecomeLeader = if (partitionsTobeLeader.nonEmpty)
          makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, correlationId, responseMap)
        else
          Set.empty[Partition]

        //note: 将该副本设置为 follower
        val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
          makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap, metadataCache)
        else
          Set.empty[Partition]

        //note: 如果 hw checkpoint 的线程没有初始化，这里需要进行一次初始化
        // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions
        if (!hwThreadInitialized) {
          startHighWaterMarksCheckPointThread()
          hwThreadInitialized = true
        }
        //note: 检查 replica fetcher 是否需要关闭（有些副本需要关闭因为可能从 follower 变为 leader）
        replicaFetcherManager.shutdownIdleFetcherThreads()

        //note: 检查是否有 GroupMetadataTopicName 的 leaderAndIsr 发生了切换
        onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
        BecomeLeaderOrFollowerResult(responseMap, Errors.NONE.code)
      }
    }
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   *
   *  TODO: the above may need to be fixed later
   */
  //note: 选举当前副本作为 partition 的 leader，处理过程：
  //note: 1. 停止这些 partition 的 副本同步请求；
  //note: 2. 更新缓存中的 partition metadata；
  //note: 3. 将这些 partition 添加到 leader partition 集合中。
  private def makeLeaders(controllerId: Int,
                          epoch: Int,
                          partitionState: Map[Partition, PartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Short]): Set[Partition] = {
    partitionState.keys.foreach { partition =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
    }

    for (partition <- partitionState.keys)
      responseMap.put(partition.topicPartition, Errors.NONE.code)

    val partitionsToMakeLeaders: mutable.Set[Partition] = mutable.Set()

    try {
      // First stop fetchers for all the partitions
      //note: 停止这些副本同步请求
      replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(_.topicPartition))
      // Update the partition information to be the leader
      //note: 更新这些 partition 的信息（这些 partition 成为 leader 了）
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        //note: 在 partition 对象将本地副本设置为 leader
        if (partition.makeLeader(controllerId, partitionStateInfo, correlationId))
          partitionsToMakeLeaders += partition //note: 成功选为 leader 的 partition 集合
        else
          //note: 本地 replica 已经是 leader replica，可能是接收了重试的请求
          stateChangeLogger.info(("Broker %d skipped the become-leader state change after marking its partition as leader with correlation id %d from " +
            "controller %d epoch %d for partition %s since it is already the leader for the partition.")
            .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
      }
      partitionsToMakeLeaders.foreach { partition =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-leader request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
      }
    } catch {
      case e: Throwable =>
        partitionState.keys.foreach { partition =>
          val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d" +
            " epoch %d for partition %s").format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition)
          stateChangeLogger.error(errorMsg, e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    //note: LeaderAndIsr 请求处理完成
    partitionState.keys.foreach { partition =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-leader transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
    }

    partitionsToMakeLeaders
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. Clear the produce and fetch requests in the purgatory
   * 6. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   */
  //note: 对于给定的这些副本，将本地副本设置为 follower
  //note: 1. 从 leader partition 集合移除这些 partition；
  //note: 2. 将这些 partition 标记为 follower，之后这些 partition 就不会再接收 produce 的请求了；
  //note: 3. 停止对这些 partition 的副本同步，这样这些副本就不会再有（来自副本请求线程）的数据进行追加了；
  //note: 4. 对这些 partition 的 offset 进行 checkpoint，如果日志需要截断就进行截断操作；
  //note: 5. 清空 purgatory 中的 produce 和 fetch 请求；
  //note: 6. 如果 broker 没有掉线，向这些 partition 的新 leader 启动副本同步线程；
  //note: 上面这些操作的顺序性，保证了这些副本在 offset checkpoint 之前将不会接收新的数据，这样的话，在 checkpoint 之前这些数据都可以保证刷到磁盘
  private def makeFollowers(controllerId: Int,
                            epoch: Int,
                            partitionState: Map[Partition, PartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Short],
                            metadataCache: MetadataCache) : Set[Partition] = {
    partitionState.keys.foreach { partition =>
      stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "starting the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
    }

    for (partition <- partitionState.keys)
      responseMap.put(partition.topicPartition, Errors.NONE.code)

    //note: 统计 follower 的集合
    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()

    try {

      // TODO: Delete leaders from LeaderAndIsrRequest
      partitionState.foreach{ case (partition, partitionStateInfo) =>
        val newLeaderBrokerId = partitionStateInfo.leader
        metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match { //note: leader 是可用的
          // Only change partition state when the leader is available
          case Some(_) => //note: partition 的本地副本设置为 follower
            if (partition.makeFollower(controllerId, partitionStateInfo, correlationId))
              partitionsToMakeFollower += partition
            else //note: 这个 partition 的本地副本已经是 follower 了
              stateChangeLogger.info(("Broker %d skipped the become-follower state change after marking its partition as follower with correlation id %d from " +
                "controller %d epoch %d for partition %s since the new leader %d is the same as the old leader")
                .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
                partition.topicPartition, newLeaderBrokerId))
          case None =>
            // The leader broker should always be present in the metadata cache.
            // If not, we should record the error message and abort the transition process for this partition
            stateChangeLogger.error(("Broker %d received LeaderAndIsrRequest with correlation id %d from controller" +
              " %d epoch %d for partition %s but cannot become follower since the new leader %d is unavailable.")
              .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
              partition.topicPartition, newLeaderBrokerId))
            // Create the local replica even if the leader is unavailable. This is required to ensure that we include
            // the partition's high watermark in the checkpoint file (see KAFKA-1647)
            partition.getOrCreateReplica()
        }
      }

      //note: 删除对这些 partition 的副本同步线程
      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
      }

      //note: Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
      logManager.truncateTo(partitionsToMakeFollower.map { partition =>
        (partition.topicPartition, partition.getOrCreateReplica().highWatermark.messageOffset)
      }.toMap)
      //note: 完成那些延迟请求的处理
      partitionsToMakeFollower.foreach { partition =>
        val topicPartitionOperationKey = new TopicPartitionOperationKey(partition.topicPartition)
        tryCompleteDelayedProduce(topicPartitionOperationKey)
        tryCompleteDelayedFetch(topicPartitionOperationKey)
      }

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d truncated logs and checkpointed recovery boundaries for partition %s as part of " +
          "become-follower request with correlation id %d from controller %d epoch %d").format(localBrokerId,
          partition.topicPartition, correlationId, controllerId, epoch))
      }

      if (isShuttingDown.get()) {
        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d skipped the adding-fetcher step of the become-follower state change with correlation id %d from " +
            "controller %d epoch %d for partition %s since it is shutting down").format(localBrokerId, correlationId,
            controllerId, epoch, partition.topicPartition))
        }
      }
      else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        //note: 启动副本同步线程
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
          partition.topicPartition -> BrokerAndInitialOffset(
            metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get.getBrokerEndPoint(config.interBrokerListenerName),
            partition.getReplica().get.logEndOffset.messageOffset)).toMap //note: leader 信息+本地 replica 的 offset
        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)

        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(("Broker %d started fetcher to new leader as part of become-follower request from controller " +
            "%d epoch %d with correlation id %d for partition %s")
            .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
        }
      }
    } catch {
      case e: Throwable =>
        val errorMsg = ("Error on broker %d while processing LeaderAndIsr request with correlationId %d received from controller %d " +
          "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
        stateChangeLogger.error(errorMsg, e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionState.keys.foreach { partition =>
      stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
        "for the become-follower transition for partition %s")
        .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
    }

    partitionsToMakeFollower
  }

  //note: 遍历所有的 partition 对象,检查其 isr 是否需要抖动
  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
    allPartitions.values.foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs))
  }

  private def updateFollowerLogReadResults(replicaId: Int, readResults: Seq[(TopicPartition, LogReadResult)]) {
    debug("Recording follower broker %d log read results: %s ".format(replicaId, readResults))
    readResults.foreach { case (topicPartition, readResult) =>
      getPartition(topicPartition) match {
        case Some(partition) =>
          //note: 更新副本的相关信息
          partition.updateReplicaLogReadResult(replicaId, readResult)

          // for producer requests with ack > 1, we need to check
          // if they can be unblocked after some follower's log end offsets have moved
          tryCompleteDelayedProduce(new TopicPartitionOperationKey(topicPartition))
        case None =>
          warn("While recording the replica LEO, the partition %s hasn't been created.".format(topicPartition))
      }
    }
  }

  private def getLeaderPartitions(): List[Partition] = {
    allPartitions.values.filter(_.leaderReplicaIfLocal.isDefined).toList
  }

  def getHighWatermark(topicPartition: TopicPartition): Option[Long] = {
    getPartition(topicPartition).flatMap { partition =>
      partition.leaderReplicaIfLocal.map(_.highWatermark.messageOffset)
    }
  }

  // Flushes the highwatermark value for all partitions to the highwatermark file
  //note: 对所有 replica hw 做 checkpoint
  def checkpointHighWatermarks() {
    val replicas = allPartitions.values.flatMap(_.getReplica(localBrokerId))
    val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParentFile.getAbsolutePath)
    for ((dir, reps) <- replicasByDir) {
      val hwms = reps.map(r => r.partition.topicPartition -> r.highWatermark.messageOffset).toMap
      try {
        highWatermarkCheckpoints(dir).write(hwms)
      } catch {
        case e: IOException =>
          fatal("Error writing to highwatermark file: ", e)
          Runtime.getRuntime.halt(1)
      }
    }
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true) {
    info("Shutting down")
    replicaFetcherManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    info("Shut down completely")
  }
}
