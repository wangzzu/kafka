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

import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.BrokerEndPoint
import kafka.consumer.PartitionTopicInfo
import kafka.utils.{DelayedItem, Pool, ShutdownableThread}
import kafka.common.{ClientIdAndBroker, KafkaException}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.protocol.Errors
import AbstractFetcherThread._

import scala.collection.{Map, Set, mutable}
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.PartitionStates
import org.apache.kafka.common.record.MemoryRecords

/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class AbstractFetcherThread(name: String,
                                     clientId: String,
                                     sourceBroker: BrokerEndPoint,
                                     fetchBackOffMs: Int = 0,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {

  type REQ <: FetchRequest
  type PD <: PartitionData

  //note: 记录 fetch 相关的信息
  private val partitionStates = new PartitionStates[PartitionFetchState]
  private val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()

  private val metricId = new ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)

  /* callbacks to be defined in subclass */

  // process fetched data
  def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PD)

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topicPartition: TopicPartition): Long

  // deal with partitions with errors, potentially due to leadership changes
  def handlePartitionsWithErrors(partitions: Iterable[TopicPartition])

  protected def buildFetchRequest(partitionMap: Seq[(TopicPartition, PartitionFetchState)]): REQ

  protected def fetch(fetchRequest: REQ): Seq[(TopicPartition, PD)]

  override def shutdown(){
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // we don't need the lock since the thread has finished shutdown and metric removal is safe
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }

  override def doWork() {

    //note: 构造 fetch request
    val fetchRequest = inLock(partitionMapLock) {
      val fetchRequest = buildFetchRequest(partitionStates.partitionStates.asScala.map { state =>
        state.topicPartition -> state.value
      })
      if (fetchRequest.isEmpty) { //note: 如果没有活跃的 partition，在下次调用之前，sleep fetchBackOffMs 时间
        trace("There are no active partitions. Back off for %d ms before sending a fetch request".format(fetchBackOffMs))
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }
      fetchRequest
    }
    if (!fetchRequest.isEmpty)
      processFetchRequest(fetchRequest) //note: 发送 fetch 请求，处理 fetch 的结果
  }

  private def processFetchRequest(fetchRequest: REQ) {
    val partitionsWithError = mutable.Set[TopicPartition]()

    def updatePartitionsWithError(partition: TopicPartition): Unit = {
      partitionsWithError += partition
      partitionStates.moveToEnd(partition)
    }

    var responseData: Seq[(TopicPartition, PD)] = Seq.empty

    try {
      trace("Issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
      responseData = fetch(fetchRequest) //note: 发送 fetch 请求，获取 fetch 结果
    } catch {
      case t: Throwable =>
        if (isRunning.get) {
          warn(s"Error in fetch $fetchRequest", t)
          inLock(partitionMapLock) { //note: fetch 时发生错误，sleep 一会
            partitionStates.partitionSet.asScala.foreach(updatePartitionsWithError)
            // there is an error occurred while fetching partitions, sleep a while
            // note that `ReplicaFetcherThread.handlePartitionsWithError` will also introduce the same delay for every
            // partition with error effectively doubling the delay. It would be good to improve this.
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
          }
        }
    }
    fetcherStats.requestRate.mark()

    if (responseData.nonEmpty) { //note: fetch 结果不为空
      // process fetched data
      inLock(partitionMapLock) {

        responseData.foreach { case (topicPartition, partitionData) =>
          val topic = topicPartition.topic
          val partitionId = topicPartition.partition
          Option(partitionStates.stateValue(topicPartition)).foreach(currentPartitionFetchState =>
            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch
            //note: 如果 fetch 的 offset 与返回结果的 offset 相同，并且返回没有异常，那么就将拉取的数据追加到对应的 partition 上
            if (fetchRequest.offset(topicPartition) == currentPartitionFetchState.offset) {
              Errors.forCode(partitionData.errorCode) match {
                case Errors.NONE =>
                  try {
                    val records = partitionData.toRecords
                    val newOffset = records.shallowEntries.asScala.lastOption.map(_.nextOffset).getOrElse(
                      currentPartitionFetchState.offset)

                    fetcherLagStats.getAndMaybePut(topic, partitionId).lag = Math.max(0L, partitionData.highWatermark - newOffset)
                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                    //note: 将 fetch 的数据追加到日志文件中
                    processPartitionData(topicPartition, currentPartitionFetchState.offset, partitionData)

                    val validBytes = records.validBytes
                    if (validBytes > 0) {
                      // Update partitionStates only if there is no exception during processPartitionData
                      //note: 更新 fetch 的 offset 位置
                      partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset))
                      fetcherStats.byteRate.mark(validBytes) //note: 更新 metrics
                    }
                  } catch {
                    case ime: CorruptRecordException =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                      // should get fixed in the subsequent fetches
                      //note: CRC 验证失败时，打印日志，并继续进行（这个线程还会有其他的 tp 拉取，防止影响其他副本同步）
                      logger.error("Found invalid messages during fetch for partition [" + topic + "," + partitionId + "] offset " + currentPartitionFetchState.offset  + " error " + ime.getMessage)
                      updatePartitionsWithError(topicPartition);
                    case e: Throwable =>
                      //note: 这里还会抛出异常，是 RUNTimeException
                      throw new KafkaException("error processing data for partition [%s,%d] offset %d"
                        .format(topic, partitionId, currentPartitionFetchState.offset), e)
                  }
                case Errors.OFFSET_OUT_OF_RANGE => //note: Out-of-range 的情况处理
                  try {
                    val newOffset = handleOffsetOutOfRange(topicPartition)
                    partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset))
                    error("Current offset %d for partition [%s,%d] out of range; reset offset to %d"
                      .format(currentPartitionFetchState.offset, topic, partitionId, newOffset))
                  } catch { //note: 处理 out-of-range 是抛出的异常
                    case e: Throwable =>
                      error("Error getting offset for partition [%s,%d] to broker %d".format(topic, partitionId, sourceBroker.id), e)
                      updatePartitionsWithError(topicPartition)
                  }
                case _ => //note: 其他的异常情况
                  if (isRunning.get) {
                    error("Error for partition [%s,%d] to broker %d:%s".format(topic, partitionId, sourceBroker.id,
                      partitionData.exception.get))
                    updatePartitionsWithError(topicPartition)
                  }
              }
            })
        }
      }
    }

    //note: 处理拉取遇到的错误读的 tp
    if (partitionsWithError.nonEmpty) {
      debug("handling partitions with error for %s".format(partitionsWithError))
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  //note: 为该 fetcher 线程添加要拉取的 topic-partition 列表
  def addPartitions(partitionAndOffsets: Map[TopicPartition, Long]) {
    partitionMapLock.lockInterruptibly()
    try {
      // If the partitionMap already has the topic/partition, then do not update the map with the old offset
      val newPartitionToState = partitionAndOffsets.filter { case (tp, _) =>
        !partitionStates.contains(tp)
      }.map { case (tp, offset) =>
        val fetchState =
          if (PartitionTopicInfo.isOffsetInvalid(offset)) new PartitionFetchState(handleOffsetOutOfRange(tp))
          else new PartitionFetchState(offset)
        tp -> fetchState
      }
      val existingPartitionToState = partitionStates.partitionStates.asScala.map { state =>
        state.topicPartition -> state.value
      }.toMap
      //note: 新增的 partition 的 offset 信息记录到 partitionStates
      partitionStates.set((existingPartitionToState ++ newPartitionToState).asJava)
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  //note: 对指定的 partition 设置延迟时间，默认是1s
  def delayPartitions(partitions: Iterable[TopicPartition], delay: Long) {
    partitionMapLock.lockInterruptibly()
    try {
      for (partition <- partitions) {
        Option(partitionStates.stateValue(partition)).foreach (currentPartitionFetchState =>
          if (currentPartitionFetchState.isActive)//note: 设置延迟时间
            partitionStates.updateAndMoveToEnd(partition, new PartitionFetchState(currentPartitionFetchState.offset, new DelayedItem(delay)))
        )
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def removePartitions(topicPartitions: Set[TopicPartition]) {
    partitionMapLock.lockInterruptibly()
    try {
      topicPartitions.foreach { topicPartition =>
        partitionStates.remove(topicPartition)
        fetcherLagStats.unregister(topicPartition.topic, topicPartition.partition)
      }
    } finally partitionMapLock.unlock()
  }

  def partitionCount() = {
    partitionMapLock.lockInterruptibly()
    try partitionStates.size
    finally partitionMapLock.unlock()
  }

}

object AbstractFetcherThread {

  trait FetchRequest {
    def isEmpty: Boolean
    def offset(topicPartition: TopicPartition): Long
  }

  trait PartitionData {
    def errorCode: Short
    def exception: Option[Throwable]
    def toRecords: MemoryRecords
    def highWatermark: Long
  }

}

object FetcherMetrics {
  val ConsumerLag = "ConsumerLag"
  val RequestsPerSec = "RequestsPerSec"
  val BytesPerSec = "BytesPerSec"
}

class FetcherLagMetrics(metricId: ClientIdTopicPartition) extends KafkaMetricsGroup {

  private[this] val lagVal = new AtomicLong(-1L)
  private[this] val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topic,
    "partition" -> metricId.partitionId.toString)

  newGauge(FetcherMetrics.ConsumerLag,
    new Gauge[Long] {
      def value = lagVal.get
    },
    tags
  )

  def lag_=(newLag: Long) {
    lagVal.set(newLag)
  }

  def lag = lagVal.get

  def unregister() {
    removeMetric(FetcherMetrics.ConsumerLag, tags)
  }
}

class FetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: ClientIdTopicPartition) => new FetcherLagMetrics(k)
  val stats = new Pool[ClientIdTopicPartition, FetcherLagMetrics](Some(valueFactory))

  def getAndMaybePut(topic: String, partitionId: Int): FetcherLagMetrics = {
    stats.getAndMaybePut(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
  }

  def isReplicaInSync(topic: String, partitionId: Int): Boolean = {
    val fetcherLagMetrics = stats.get(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (fetcherLagMetrics != null)
      fetcherLagMetrics.lag <= 0
    else
      false
  }

  def unregister(topic: String, partitionId: Int) {
    val lagMetrics = stats.remove(new ClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (lagMetrics != null) lagMetrics.unregister()
  }

  def unregister() {
    stats.keys.toBuffer.foreach { key: ClientIdTopicPartition =>
      unregister(key.topic, key.partitionId)
    }
  }
}

class FetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString)

  val requestRate = newMeter(FetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  val byteRate = newMeter(FetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags)

  def unregister() {
    removeMetric(FetcherMetrics.RequestsPerSec, tags)
    removeMetric(FetcherMetrics.BytesPerSec, tags)
  }

}

case class ClientIdTopicPartition(clientId: String, topic: String, partitionId: Int) {
  override def toString = "%s-%s-%d".format(clientId, topic, partitionId)
}

/**
  * case class to keep partition offset and its state(active, inactive)
  */
case class PartitionFetchState(offset: Long, delay: DelayedItem) {

  def this(offset: Long) = this(offset, new DelayedItem(0))

  //note: 延迟操作设置，如果该 partition 已经满足延迟时间的设置，那么就返回 true
  def isActive: Boolean = delay.getDelay(TimeUnit.MILLISECONDS) == 0

  override def toString = "%d-%b".format(offset, isActive)
}
