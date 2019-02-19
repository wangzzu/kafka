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

import kafka.common._
import kafka.utils._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.server._
import kafka.metrics.KafkaMetricsGroup
import kafka.controller.KafkaController
import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.errors.{NotEnoughReplicasException, NotLeaderForPartitionException}
import org.apache.kafka.common.protocol.Errors

import scala.collection.JavaConverters._
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.PartitionState
import org.apache.kafka.common.utils.Time

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
class Partition(val topic: String,
                val partitionId: Int,
                time: Time,
                replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {
  val topicPartition = new TopicPartition(topic, partitionId)

  private val localBrokerId = replicaManager.config.brokerId //note: 本地 broker 的 id
  private val logManager = replicaManager.logManager //note: 日志管理器
  private val zkUtils = replicaManager.zkUtils
  private val assignedReplicaMap = new Pool[Int, Replica] //note: 它代表的是AR，表示该 Partition 所有的副本
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  @volatile var leaderReplicaIdOpt: Option[Int] = None //note: leader 信息
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica] //note: ISR 列表,同步的副本

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

  //note: 判断是不是本地副本
  private def isReplicaLocal(replicaId: Int) : Boolean = replicaId == localBrokerId
  val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  newGauge("UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    },
    tags
  )

  newGauge("InSyncReplicasCount",
    new Gauge[Int] {
      def value = {
        if (isLeaderReplicaLocal) inSyncReplicas.size else 0
      }
    },
    tags
  )

  newGauge("ReplicasCount",
    new Gauge[Int] {
      def value = {
        if (isLeaderReplicaLocal) assignedReplicas.size else 0
      }
    },
    tags
  )

  private def isLeaderReplicaLocal: Boolean = leaderReplicaIfLocal.isDefined

  //note: 验证当前副本是否都在 isr 中,仅对 leader 有效
  def isUnderReplicated: Boolean =
    isLeaderReplicaLocal && inSyncReplicas.size < assignedReplicas.size

  //note: 分区根据指定的副本编号创建副本（replica）
  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    assignedReplicaMap.getAndMaybePut(replicaId, {
      if (isReplicaLocal(replicaId)) { //note: 本地的副本
        val config = LogConfig.fromProps(logManager.defaultConfig.originals,
                                         AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic))
        val log = logManager.createLog(topicPartition, config) //note: 获取 Log 对象,没有的话新建该实例
        val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath) //note: 检查点文件
        val offsetMap = checkpoint.read
        if (!offsetMap.contains(topicPartition))
          info(s"No checkpointed highwatermark is found for partition $topicPartition")
        val offset = math.min(offsetMap.getOrElse(topicPartition, 0L), log.logEndOffset)
        new Replica(replicaId, this, time, offset, Some(log))
      } else new Replica(replicaId, this, time) //note: 远程的副本,不需要创建 Log 对象
    })
  }

  //note: 获取指定编号的副本,默认是获取本地副本
  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = Option(assignedReplicaMap.get(replicaId))

  //note: 获取分区的 leader 副本,如果是不是本地副本,返回 None
  def leaderReplicaIfLocal: Option[Replica] =
    leaderReplicaIdOpt.filter(_ == localBrokerId).flatMap(getReplica)

  def addReplicaIfNotExists(replica: Replica): Replica =
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)

  def assignedReplicas: Set[Replica] =
    assignedReplicaMap.values.toSet

  private def removeReplica(replicaId: Int) {
    assignedReplicaMap.remove(replicaId)
  }

  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      assignedReplicaMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      try {
        logManager.asyncDelete(topicPartition)
        removePartitionMetrics()
      } catch {
        case e: IOException =>
          fatal(s"Error deleting the log for partition $topicPartition", e)
          Runtime.getRuntime.halt(1)
      }
    }
  }

  def getLeaderEpoch: Int = this.leaderEpoch

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  //note: 将本地副本设置为 leader, 如果 leader 不变,向 ReplicaManager 返回 false
  def makeLeader(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      //note: 为了新的 replica 创建副本实例
      allReplicas.foreach(replica => getOrCreateReplica(replica))
      //note: 获取新的 isr 列表
      val newInSyncReplicas = partitionStateInfo.isr.asScala.map(r => getOrCreateReplica(r)).toSet
      // remove assigned replicas that have been removed by the controller
      //note: 将已经在不在 AR 中的副本移除
      (assignedReplicas.map(_.brokerId) -- allReplicas).foreach(removeReplica)
      inSyncReplicas = newInSyncReplicas
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion
      //note: 判断是否是新的 leader
      val isNewLeader =
        if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == localBrokerId) {//note: leader 没有更新
          false
        } else {
          leaderReplicaIdOpt = Some(localBrokerId)
          true
        }
      val leaderReplica = getReplica().get //note: 获取在当前上的副本,也就是 leader replica
      val curLeaderLogEndOffset = leaderReplica.logEndOffset.messageOffset //note: 获取 leader replica 的 the end offset
      val curTimeMs = time.milliseconds
      // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
      (assignedReplicas - leaderReplica).foreach { replica => //note: 对于 isr 中的 replica,更新 LastCaughtUpTime
        val lastCaughtUpTimeMs = if (inSyncReplicas.contains(replica)) curTimeMs else 0L
        replica.resetLastCaughtUpTime(curLeaderLogEndOffset, curTimeMs, lastCaughtUpTimeMs)
      }
      // we may need to increment high watermark since ISR could be down to 1
      if (isNewLeader) {  //note: 如果是新的 leader,那么需要
        // construct the high watermark metadata for the new leader replica
        //note: 为新的 leader 构造 replica 的 HW metadata
        leaderReplica.convertHWToLocalOffsetMetadata()
        // reset log end offset for remote replicas
        //note: 更新远程副本的副本同步信息（设置为 unKnown）
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      //note: 如果满足更新 isr 的条件,就更新 HW 信息
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented) //note: HW 更新的情况下
      tryCompleteDelayedRequests()
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change, return false to indicate the replica manager
   */
  //note: 通过更新 leader 和 isr 信息来使本地 replica 成为 follower,但是如果 leader 信息没有改变那么返回 false
  def makeFollower(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId: Int = partitionStateInfo.leader //note: 新的 leader 信息
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      //note: 为新的 replica 创建新的对象实例
      allReplicas.foreach(r => getOrCreateReplica(r))
      // remove assigned replicas that have been removed by the controller
      //note: 删除被 controller 已经移除的副本信息
      (assignedReplicas.map(_.brokerId) -- allReplicas).foreach(removeReplica)
      inSyncReplicas = Set.empty[Replica] //note: 清空 isr 列表
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion

      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {//note: leader 没有更新返回 false
        false
      }
      else {
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the log end offset of a certain replica of this partition
   */
  //note: 更新这个 partition replica 的 the end offset
  def updateReplicaLogReadResult(replicaId: Int, logReadResult: LogReadResult) {
    getReplica(replicaId) match {
      case Some(replica) =>
        //note: 更新副本的信息
        replica.updateLogReadResult(logReadResult)
        // check if we need to expand ISR to include this replica
        // if it is not in the ISR yet
        //note: 如果该副本不在 isr 中,检查是否需要进行更新
        maybeExpandIsr(replicaId, logReadResult)

        debug("Recorded replica %d log end offset (LEO) position %d for partition %s."
          .format(replicaId, logReadResult.info.fetchOffsetMetadata.messageOffset, topicPartition))
      case None =>
        throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +
          " is not recognized to be one of the assigned replicas %s for partition %s.")
          .format(localBrokerId,
                  replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  assignedReplicas.map(_.brokerId).mkString(","),
                  topicPartition))
    }
  }

  /**
   * Check and maybe expand the ISR of the partition.
   * A replica will be added to ISR if its LEO >= current hw of the partition.
   *
   * Technically, a replica shouldn't be in ISR if it hasn't caught up for longer than replicaLagTimeMaxMs,
   * even if its log end offset is >= HW. However, to be consistent with how the follower determines
   * whether a replica is in-sync, we only check HW.
   *
   * This function can be triggered when a replica's LEO has incremented
   */
  //note: 检查当前 Partition 是否需要扩充 ISR, 副本的 LEO 大于等于 hw 的副本将会被添加到 isr 中
  def maybeExpandIsr(replicaId: Int, logReadResult: LogReadResult) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val replica = getReplica(replicaId).get
          val leaderHW = leaderReplica.highWatermark
          if(!inSyncReplicas.contains(replica) &&
             assignedReplicas.map(_.brokerId).contains(replicaId) &&
             replica.logEndOffset.offsetDiff(leaderHW) >= 0) { //note: replica LEO 大于 HW 的情况下,加入 isr 列表
            val newInSyncReplicas = inSyncReplicas + replica
            info(s"Expanding ISR for partition $topicPartition from ${inSyncReplicas.map(_.brokerId).mkString(",")} " +
              s"to ${newInSyncReplicas.map(_.brokerId).mkString(",")}")
            // update ISR in ZK and cache
            updateIsr(newInSyncReplicas) //note: 更新到 zk
            replicaManager.isrExpandRate.mark()
          }

          // check if the HW of the partition can now be incremented
          // since the replica may already be in the ISR and its LEO has just incremented
          //note: 检查 HW 是否需要更新
          maybeIncrementLeaderHW(leaderReplica, logReadResult.fetchTimeMs)

        case None => false // nothing to do if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  /*
   * Returns a tuple where the first element is a boolean indicating whether enough replicas reached `requiredOffset`
   * and the second element is an error (which would be `Errors.NONE` for no error).
   *
   * Note that this method will only be called if requiredAcks = -1 and we are waiting for all replicas in ISR to be
   * fully caught up to the (local) leader's offset corresponding to this produce request before we acknowledge the
   * produce request.
   */
  //note: ack=-1时, 检查副本同步是否满足要求(足够的副本达到 requiredOffset)
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
    leaderReplicaIfLocal match {
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference
        val curInSyncReplicas = inSyncReplicas

        def numAcks = curInSyncReplicas.count { r =>
          if (!r.isLocal)
            if (r.logEndOffset.messageOffset >= requiredOffset) {
              trace(s"Replica ${r.brokerId} of ${topic}-${partitionId} received offset $requiredOffset")
              true
            }
            else
              false
          else
            true /* also count the local (leader) replica */
        }

        trace(s"$numAcks acks satisfied for ${topic}-${partitionId} with acks = -1")

        val minIsr = leaderReplica.log.get.config.minInSyncReplicas

        if (leaderReplica.highWatermark.messageOffset >= requiredOffset) { //note: 根据 leader 的 HW 来判断
          /*
           * The topic may be configured not to accept messages if there are not enough replicas in ISR
           * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
           */
          if (minIsr <= curInSyncReplicas.size) //note: min.isr 满足要求
            (true, Errors.NONE)
          else
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
        } else
          (false, Errors.NONE)
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
   *
   * The HW is determined by the smallest log end offset among all replicas that are in sync or are considered caught-up.
   * This way, if a replica is considered caught-up, but its log end offset is smaller than HW, we will wait for this
   * replica to catch up to the HW before advancing the HW. This helps the situation when the ISR only includes the
   * leader replica and a follower tries to catch up. If we don't wait for the follower when advancing the HW, the
   * follower's log end offset may keep falling behind the HW (determined by the leader's log end offset) and therefore
   * will never be added to ISR.
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  //note: 检查是否需要更新 partition 的 HW,这个方法将在两种情况下触发:
  //note: 1.Partition ISR 变动; 2. 任何副本的 LEO 改变;
  //note: 在获取 HW 时,是从 isr 和认为能追得上的副本中选择最小的 LEO,之所以也要从能追得上的副本中选择,是为了等待 follower 追上 HW,否则可能没机会追上了
  private def maybeIncrementLeaderHW(leaderReplica: Replica, curTime: Long = time.milliseconds): Boolean = {
    //note: 获取 isr 以及能够追上 isr （认为最近一次 fetch 的时间在 replica.lag.time.max.time 之内） 副本的 LEO 信息。
    val allLogEndOffsets = assignedReplicas.filter { replica =>
      curTime - replica.lastCaughtUpTimeMs <= replicaManager.config.replicaLagTimeMaxMs || inSyncReplicas.contains(replica)
    }.map(_.logEndOffset)
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering) //note: 新的 HW
    val oldHighWatermark = leaderReplica.highWatermark
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset || oldHighWatermark.onOlderSegment(newHighWatermark)) {
      leaderReplica.highWatermark = newHighWatermark
      debug("High watermark for partition [%s,%d] updated to %s".format(topic, partitionId, newHighWatermark))
      true
    } else {
      debug("Skipping update high watermark since Old hw %s is larger than new hw %s for partition [%s,%d]. All leo's are %s"
        .format(oldHighWatermark, newHighWatermark, topic, partitionId, allLogEndOffsets.mkString(",")))
      false
    }
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(topicPartition)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
  }

  //note: 检查这个 isr 中每个 replica
  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match { //note: 只有本地副本是 leader, 才会做这个操作
        case Some(leaderReplica) =>
          //note: 检查当前 isr 的副本是否需要从 isr 中移除
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.nonEmpty) {
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas //note: new isr
            assert(newInSyncReplicas.nonEmpty)
            info("Shrinking ISR for partition [%s,%d] from %s to %s".format(topic, partitionId,
              inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache
            updateIsr(newInSyncReplicas) //note: 更新 isr 到 zk
            // we may need to increment high watermark since ISR could be down to 1

            replicaManager.isrShrinkRate.mark() //note: 更新 metrics
            maybeIncrementLeaderHW(leaderReplica) //note: isr 变动了,判断是否需要更新 partition 的 hw
          } else {
            false
          }

        case None => false // do nothing if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  //note: 检查 isr 中的副本是否需要从 isr 中移除
  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
     * there are two cases that will be handled here -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
     *                     the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
     *                    then the follower is lagging and should be removed from the ISR
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
     * the last time when the replica was fully caught up. If either of the above conditions
     * is violated, that replica is considered to be out of sync
     *
     **/
    //note: 获取那些不应该在 isr 中副本的列表
    //note: 1. hang 住的 replica: replica 的 LEO 超过 maxLagMs 没有更新, 那么这个 replica 将会被从 isr 中移除;
    //note: 2. 数据同步慢的 replica: 副本在 maxLagMs 内没有追上 leader 当前的 LEO, 那么这个 replica 讲会从 isr 中移除;
    //note: 都是通过 lastCaughtUpTimeMs 来判断的
    val candidateReplicas = inSyncReplicas - leaderReplica

    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if (laggingReplicas.nonEmpty)
      debug("Lagging replicas for partition %s are %s".format(topicPartition, laggingReplicas.map(_.brokerId).mkString(",")))

    laggingReplicas
  }

  def appendRecordsToLeader(records: MemoryRecords, requiredAcks: Int = 0) = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val log = leaderReplica.log.get //note: 获取对应的 Log 对象
          val minIsr = log.config.minInSyncReplicas
          val inSyncSize = inSyncReplicas.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          //note: 如果 ack 设置为-1, isr 数小于设置的 min.isr 时,就会向 producer 抛出相应的异常
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException("Number of insync replicas for partition %s is [%d], below required minimum [%d]"
              .format(topicPartition, inSyncSize, minIsr))
          }

          //note: 向副本对应的 log 追加响应的数据
          val info = log.append(records, assignOffsets = true)
          // probably unblock some follower fetch requests since log end offset has been updated
          replicaManager.tryCompleteDelayedFetch(TopicPartitionOperationKey(this.topic, this.partitionId))
          // we may need to increment high watermark since ISR could be down to 1
          //note: 判断是否需要增加 HHW（追加日志后会进行一次判断）
          (info, maybeIncrementLeaderHW(leaderReplica))

        case None =>
          //note: leader 不在本台机器上
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    info
  }

  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(r => r.brokerId).toList, zkVersion)
    val (updateSucceeded,newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partitionId,
      newLeaderAndIsr, controllerEpoch, zkVersion) //note: 执行更新操作

    if(updateSucceeded) { //note: 成功更新到 zk 上
      replicaManager.recordIsrChange(topicPartition) //note: 告诉 replicaManager 这个 partition 的 isr 需要更新
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  private def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
    removeMetric("InSyncReplicasCount", tags)
    removeMetric("ReplicasCount", tags)
  }

  override def equals(that: Any): Boolean = that match {
    case other: Partition => partitionId == other.partitionId && topic == other.topic
    case _ => false
  }

  override def hashCode: Int =
    31 + topic.hashCode + 17 * partitionId

  override def toString: String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString
  }
}
