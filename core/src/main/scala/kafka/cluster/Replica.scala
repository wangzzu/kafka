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

package kafka.cluster

import kafka.log.Log
import kafka.utils.Logging
import kafka.server.{LogOffsetMetadata, LogReadResult}
import kafka.common.KafkaException
import java.util.concurrent.atomic.AtomicLong

import org.apache.kafka.common.utils.Time

class Replica(val brokerId: Int,
              val partition: Partition,
              time: Time = Time.SYSTEM,
              initialHighWatermarkValue: Long = 0L,
              val log: Option[Log] = None) extends Logging {
  // the high watermark offset value, in non-leader replicas only its message offsets are kept
  @volatile private[this] var highWatermarkMetadata = new LogOffsetMetadata(initialHighWatermarkValue) //note: 水位
  // the log end offset value, kept in all replicas;
  // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch
  @volatile private[this] var logEndOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata //note: 偏移量元数据

  // The log end offset value at the time the leader received the last FetchRequest from this follower
  // This is used to determine the lastCaughtUpTimeMs of the follower
  @volatile private[this] var lastFetchLeaderLogEndOffset = 0L

  // The time when the leader received the last FetchRequest from this follower
  // This is used to determine the lastCaughtUpTimeMs of the follower
  @volatile private[this] var lastFetchTimeMs = 0L

  // lastCaughtUpTimeMs is the largest time t such that the offset of most recent FetchRequest from this follower >=
  // the LEO of leader at time t. This is used to determine the lag of this follower and ISR of this partition.
  @volatile private[this] var _lastCaughtUpTimeMs = 0L

  val topicPartition = partition.topicPartition

  def isLocal: Boolean = log.isDefined

  def lastCaughtUpTimeMs = _lastCaughtUpTimeMs

  /*
   * If the FetchRequest reads up to the log end offset of the leader when the current fetch request is received,
   * set `lastCaughtUpTimeMs` to the time when the current fetch request was received.
   *
   * Else if the FetchRequest reads up to the log end offset of the leader when the previous fetch request was received,
   * set `lastCaughtUpTimeMs` to the time when the previous fetch request was received.
   *
   * This is needed to enforce the semantics of ISR, i.e. a replica is in ISR if and only if it lags behind leader's LEO
   * by at most `replicaLagTimeMaxMs`. These semantics allow a follower to be added to the ISR even if the offset of its
   * fetch request is always smaller than the leader's LEO, which can happen if small produce requests are received at
   * high frequency.
   */
  //note: 更新数据读取的结果,针对远程副本,主要是更新远程副本的 logEndOffset 和 lastFetchTimeMs
  def updateLogReadResult(logReadResult : LogReadResult) {
    if (logReadResult.info.fetchOffsetMetadata.messageOffset >= logReadResult.leaderLogEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, logReadResult.fetchTimeMs)
    else if (logReadResult.info.fetchOffsetMetadata.messageOffset >= lastFetchLeaderLogEndOffset)
      _lastCaughtUpTimeMs = math.max(_lastCaughtUpTimeMs, lastFetchTimeMs)

    logEndOffset = logReadResult.info.fetchOffsetMetadata //note: 更新 LEO
    lastFetchLeaderLogEndOffset = logReadResult.leaderLogEndOffset
    lastFetchTimeMs = logReadResult.fetchTimeMs
  }

  //note: 更新 replica 上次拉取时的时间（完全追得上 leader leo 时才触发的）
  def resetLastCaughtUpTime(curLeaderLogEndOffset: Long, curTimeMs: Long, lastCaughtUpTimeMs: Long) {
    lastFetchLeaderLogEndOffset = curLeaderLogEndOffset
    lastFetchTimeMs = curTimeMs
    _lastCaughtUpTimeMs = lastCaughtUpTimeMs
  }

  //note: 更新副本的偏移量元数据,只有远程副本可以更新（类似于 java 的 set 方法）
  private def logEndOffset_=(newLogEndOffset: LogOffsetMetadata) {
    if (isLocal) {
      throw new KafkaException(s"Should not set log end offset on partition $topicPartition's local replica $brokerId")
    } else {
      logEndOffsetMetadata = newLogEndOffset
      trace(s"Setting log end offset for replica $brokerId for partition $topicPartition to [$logEndOffsetMetadata]")
    }
  }

  //note: 获取副本的偏移量, 本地副本的话通过 Log 实例获取（get 方法）
  def logEndOffset =
    if (isLocal)
      log.get.logEndOffsetMetadata
    else
      logEndOffsetMetadata

  //note: 设置副本的最高水位, 只有是本地副本才能设置
  def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
    if (isLocal) {
      highWatermarkMetadata = newHighWatermark
      trace(s"Setting high watermark for replica $brokerId partition $topicPartition to [$newHighWatermark]")
    } else {
      throw new KafkaException(s"Should not set high watermark on partition $topicPartition's non-local replica $brokerId")
    }
  }

  def highWatermark = highWatermarkMetadata //note: 获取副本的 HW

  //note: 以最新的 HW 的 offset 读取数据
  def convertHWToLocalOffsetMetadata() = {
    if (isLocal) {
      highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset)
    } else {
      throw new KafkaException(s"Should not construct complete high watermark on partition $topicPartition's non-local replica $brokerId")
    }
  }

  override def equals(that: Any): Boolean = that match {
    case other: Replica => brokerId == other.brokerId && topicPartition == other.topicPartition
    case _ => false
  }

  override def hashCode: Int = 31 + topicPartition.hashCode + 17 * brokerId

  override def toString: String = {
    val replicaString = new StringBuilder
    replicaString.append("ReplicaId: " + brokerId)
    replicaString.append("; Topic: " + partition.topic)
    replicaString.append("; Partition: " + partition.partitionId)
    replicaString.append("; isLocal: " + isLocal)
    replicaString.append("; lastCaughtUpTimeMs: " + lastCaughtUpTimeMs)
    if (isLocal) replicaString.append("; Highwatermark: " + highWatermark)
    replicaString.toString
  }
}
