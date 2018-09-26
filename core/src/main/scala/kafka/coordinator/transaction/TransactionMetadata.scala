/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.transaction

import java.util.concurrent.locks.ReentrantLock

import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.RecordBatch

import scala.collection.{immutable, mutable}

private[transaction] sealed trait TransactionState { def byte: Byte }

/**
 * Transaction has not existed yet
 *
 * transition: received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private[transaction] case object Empty extends TransactionState { val byte: Byte = 0 }

/**
 * Transaction has started and ongoing
 *
 * transition: received EndTxnRequest with commit => PrepareCommit
 *             received EndTxnRequest with abort => PrepareAbort
 *             received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private[transaction] case object Ongoing extends TransactionState { val byte: Byte = 1 }

/**
 * Group is preparing to commit
 *
 * transition: received acks from all partitions => CompleteCommit
 */
private[transaction] case object PrepareCommit extends TransactionState { val byte: Byte = 2}

/**
 * Group is preparing to abort
 *
 * transition: received acks from all partitions => CompleteAbort
 */
private[transaction] case object PrepareAbort extends TransactionState { val byte: Byte = 3 }

/**
 * Group has completed commit
 *
 * Will soon be removed from the ongoing transaction cache
 */
private[transaction] case object CompleteCommit extends TransactionState { val byte: Byte = 4 }

/**
 * Group has completed abort
 *
 * Will soon be removed from the ongoing transaction cache
 */
private[transaction] case object CompleteAbort extends TransactionState { val byte: Byte = 5 }

/**
  * TransactionalId has expired and is about to be removed from the transaction cache
  */
private[transaction] case object Dead extends TransactionState { val byte: Byte = 6 }

/**
  * We are in the middle of bumping the epoch and fencing out older producers.
  */

private[transaction] case object PrepareEpochFence extends TransactionState { val byte: Byte = 7}

private[transaction] object TransactionMetadata {
  def apply(transactionalId: String, producerId: Long, producerEpoch: Short, txnTimeoutMs: Int, timestamp: Long) =
    new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, Empty,
      collection.mutable.Set.empty[TopicPartition], timestamp, timestamp)

  def apply(transactionalId: String, producerId: Long, producerEpoch: Short, txnTimeoutMs: Int,
            state: TransactionState, timestamp: Long) =
    new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, state,
      collection.mutable.Set.empty[TopicPartition], timestamp, timestamp)

  def byteToState(byte: Byte): TransactionState = {
    byte match {
      case 0 => Empty
      case 1 => Ongoing
      case 2 => PrepareCommit
      case 3 => PrepareAbort
      case 4 => CompleteCommit
      case 5 => CompleteAbort
      case 6 => Dead
      case 7 => PrepareEpochFence
      case unknown => throw new IllegalStateException("Unknown transaction state byte " + unknown + " from the transaction status message")
    }
  }

  def isValidTransition(oldState: TransactionState, newState: TransactionState): Boolean =
    TransactionMetadata.validPreviousStates(newState).contains(oldState)

  //note: 有效的状态转移
  private val validPreviousStates: Map[TransactionState, Set[TransactionState]] =
    Map(Empty -> Set(Empty, CompleteCommit, CompleteAbort),
      Ongoing -> Set(Ongoing, Empty, CompleteCommit, CompleteAbort),
      PrepareCommit -> Set(Ongoing),
      PrepareAbort -> Set(Ongoing, PrepareEpochFence),
      CompleteCommit -> Set(PrepareCommit),
      CompleteAbort -> Set(PrepareAbort),
      Dead -> Set(Empty, CompleteAbort, CompleteCommit),
      PrepareEpochFence -> Set(Ongoing)
    )
}

// this is a immutable object representing the target transition of the transaction metadata
private[transaction] case class TxnTransitMetadata(producerId: Long,
                                                   producerEpoch: Short,
                                                   txnTimeoutMs: Int,
                                                   txnState: TransactionState,
                                                   topicPartitions: immutable.Set[TopicPartition],
                                                   txnStartTimestamp: Long,
                                                   txnLastUpdateTimestamp: Long) {
  override def toString: String = {
    "TxnTransitMetadata(" +
      s"producerId=$producerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"txnTimeoutMs=$txnTimeoutMs, " +
      s"txnState=$txnState, " +
      s"topicPartitions=$topicPartitions, " +
      s"txnStartTimestamp=$txnStartTimestamp, " +
      s"txnLastUpdateTimestamp=$txnLastUpdateTimestamp)"
  }
}

/**
  *
  * @param producerId            producer id
  * @param producerEpoch         current epoch of the producer
  * @param txnTimeoutMs          timeout to be used to abort long running transactions //note: 事务超时的时间
  * @param state                 current state of the transaction
  * @param topicPartitions       current set of partitions that are part of this transaction
  * @param txnStartTimestamp     time the transaction was started, i.e., when first partition is added //note: 事务开始的时间
  * @param txnLastUpdateTimestamp   updated when any operation updates the TransactionMetadata. To be used for expiration //note: 事务最近更新的时间
  */
@nonthreadsafe
private[transaction] class TransactionMetadata(val transactionalId: String,
                                               var producerId: Long,
                                               var producerEpoch: Short,
                                               var txnTimeoutMs: Int,
                                               var state: TransactionState,
                                               val topicPartitions: mutable.Set[TopicPartition],
                                               @volatile var txnStartTimestamp: Long = -1,
                                               @volatile var txnLastUpdateTimestamp: Long) extends Logging {

  // pending state is used to indicate the state that this transaction is going to
  // transit to, and for blocking future attempts to transit it again if it is not legal;
  // initialized as the same as the current state
  //note: 事务将转移到的状态，如果状态是非法的，在转移的过程中它将会试图 block
  var pendingState: Option[TransactionState] = None

  private[transaction] val lock = new ReentrantLock

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  def addPartitions(partitions: collection.Set[TopicPartition]): Unit = {
    topicPartitions ++= partitions
  }

  def removePartition(topicPartition: TopicPartition): Unit = {
    if (state != PrepareCommit && state != PrepareAbort)
      throw new IllegalStateException(s"Transaction metadata's current state is $state, and its pending state is $pendingState " +
        s"while trying to remove partitions whose txn marker has been sent, this is not expected")

    topicPartitions -= topicPartition
  }

  // this is visible for test only
  def prepareNoTransit(): TxnTransitMetadata = {
    // do not call transitTo as it will set the pending state, a follow-up call to abort the transaction will set its pending state
    TxnTransitMetadata(producerId, producerEpoch, txnTimeoutMs, state, topicPartitions.toSet, txnStartTimestamp, txnLastUpdateTimestamp)
  }

  //note: 下面这些都是状态转移的函数
  def prepareFenceProducerEpoch(): TxnTransitMetadata = {
    if (producerEpoch == Short.MaxValue) //note: 这里有个判断
      throw new IllegalStateException(s"Cannot fence producer with epoch equal to Short.MaxValue since this would overflow")

    prepareTransitionTo(PrepareEpochFence, producerId, (producerEpoch + 1).toShort, txnTimeoutMs, topicPartitions.toSet,
      txnStartTimestamp, txnLastUpdateTimestamp)
  }

  //note: 增加 pid.epoch，并转移状态
  def prepareIncrementProducerEpoch(newTxnTimeoutMs: Int, updateTimestamp: Long): TxnTransitMetadata = {
    if (isProducerEpochExhausted)
      throw new IllegalStateException(s"Cannot allocate any more producer epochs for producerId $producerId")

    val nextEpoch = if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH) 0 else producerEpoch + 1
    prepareTransitionTo(Empty, producerId, nextEpoch.toShort, newTxnTimeoutMs, immutable.Set.empty[TopicPartition], -1,
      updateTimestamp)
  }

  //note: 对于一个新的 PID，开启新的循环，并转移相应的状态
  def prepareProducerIdRotation(newProducerId: Long, newTxnTimeoutMs: Int, updateTimestamp: Long): TxnTransitMetadata = {
    if (hasPendingTransaction)
      throw new IllegalStateException("Cannot rotate producer ids while a transaction is still pending")
    prepareTransitionTo(Empty, newProducerId, 0, newTxnTimeoutMs, immutable.Set.empty[TopicPartition], -1, updateTimestamp)
  }

  //note: meta 中添加相应的 partition
  def prepareAddPartitions(addedTopicPartitions: immutable.Set[TopicPartition], updateTimestamp: Long): TxnTransitMetadata = {
    val newTxnStartTimestamp = state match {
      case Empty | CompleteAbort | CompleteCommit => updateTimestamp
      case _ => txnStartTimestamp
    }

    prepareTransitionTo(Ongoing, producerId, producerEpoch, txnTimeoutMs, (topicPartitions ++ addedTopicPartitions).toSet,
      newTxnStartTimestamp, updateTimestamp)
  }

  def prepareAbortOrCommit(newState: TransactionState, updateTimestamp: Long): TxnTransitMetadata = {
    prepareTransitionTo(newState, producerId, producerEpoch, txnTimeoutMs, topicPartitions.toSet, txnStartTimestamp,
      updateTimestamp)
  }

  def prepareComplete(updateTimestamp: Long): TxnTransitMetadata = {
    val newState = if (state == PrepareCommit) CompleteCommit else CompleteAbort
    prepareTransitionTo(newState, producerId, producerEpoch, txnTimeoutMs, Set.empty[TopicPartition], txnStartTimestamp,
      updateTimestamp)
  }

  def prepareDead(): TxnTransitMetadata = {
    prepareTransitionTo(Dead, producerId, producerEpoch, txnTimeoutMs, Set.empty[TopicPartition], txnStartTimestamp,
      txnLastUpdateTimestamp)
  }

  /**
   * Check if the epochs have been exhausted for the current producerId. We do not allow the client to use an
   * epoch equal to Short.MaxValue to ensure that the coordinator will always be able to fence an existing producer.
   */
  //note: 检查当前的 PID 是否超过了限制
  def isProducerEpochExhausted: Boolean = producerEpoch >= Short.MaxValue - 1

  //note: 如果处在 Ongoing/PrepareAbort/PrepareCommit 状态，返回 true
  private def hasPendingTransaction: Boolean = {
    state match {
      case Ongoing | PrepareAbort | PrepareCommit => true
      case _ => false
    }
  }

  private def prepareTransitionTo(newState: TransactionState,
                                  newProducerId: Long,
                                  newEpoch: Short,
                                  newTxnTimeoutMs: Int,
                                  newTopicPartitions: immutable.Set[TopicPartition],
                                  newTxnStartTimestamp: Long,
                                  updateTimestamp: Long): TxnTransitMetadata = {
    if (pendingState.isDefined)
      throw new IllegalStateException(s"Preparing transaction state transition to $newState " +
        s"while it already a pending state ${pendingState.get}")

    if (newProducerId < 0)
      throw new IllegalArgumentException(s"Illegal new producer id $newProducerId")

    if (newEpoch < 0)
      throw new IllegalArgumentException(s"Illegal new producer epoch $newEpoch")

    // check that the new state transition is valid and update the pending state if necessary
    if (TransactionMetadata.validPreviousStates(newState).contains(state)) { //note: 状态转移合法
      val transitMetadata = TxnTransitMetadata(newProducerId, newEpoch, newTxnTimeoutMs, newState,
        newTopicPartitions, newTxnStartTimestamp, updateTimestamp)
      debug(s"TransactionalId $transactionalId prepare transition from $state to $transitMetadata")
      pendingState = Some(newState)
      transitMetadata
    } else { //note: 状态转移不合法
      throw new IllegalStateException(s"Preparing transaction state transition to $newState failed since the target state" +
        s" $newState is not a valid previous state of the current state $state")
    }
  }

  def completeTransitionTo(transitMetadata: TxnTransitMetadata): Unit = {
    // metadata transition is valid only if all the following conditions are met:
    //
    // 1. the new state is already indicated in the pending state.
    // 2. the epoch should be either the same value, the old value + 1, or 0 if we have a new producerId.
    // 3. the last update time is no smaller than the old value.
    // 4. the old partitions set is a subset of the new partitions set.
    //
    // plus, we should only try to update the metadata after the corresponding log entry has been successfully
    // written and replicated (see TransactionStateManager#appendTransactionToLog)
    //
    // if valid, transition is done via overwriting the whole object to ensure synchronization
    //note: metadata 转移只要在满足下面的条件时才是合法的：
    //note: 1. new state 已经在 pendingState 中指明了；
    //note: 2. epoch 应该是 same value、old value+1 或则 0 （如果是新的 PID） 其中一个；
    //note: 3. the last update time 要大于旧的值；
    //note: 4. old partition set 是新的子集；
    //note: 最后，我们应该更新这个 metadata 在相关的日志已经被成功持久化及备份完成后。
    //note: 如果是有效的，事务转移就完成了。

    val toState = pendingState.getOrElse {
      fatal(s"$this's transition to $transitMetadata failed since pendingState is not defined: this should not happen")

      throw new IllegalStateException(s"TransactionalId $transactionalId " +
        "completing transaction state transition while it does not have a pending state")
    }

    if (toState != transitMetadata.txnState) {
      throwStateTransitionFailure(transitMetadata)
    } else {
      toState match {
        case Empty => // from initPid
          if ((producerEpoch != transitMetadata.producerEpoch && !validProducerEpochBump(transitMetadata)) ||
            transitMetadata.topicPartitions.nonEmpty ||
            transitMetadata.txnStartTimestamp != -1) {

            throwStateTransitionFailure(transitMetadata) //note: 抛出相应的异常
          } else {
            txnTimeoutMs = transitMetadata.txnTimeoutMs
            producerEpoch = transitMetadata.producerEpoch
            producerId = transitMetadata.producerId
          }

        case Ongoing => // from addPartitions
          if (!validProducerEpoch(transitMetadata) ||
            !topicPartitions.subsetOf(transitMetadata.topicPartitions) ||
            txnTimeoutMs != transitMetadata.txnTimeoutMs ||
            txnStartTimestamp > transitMetadata.txnStartTimestamp) {

            throwStateTransitionFailure(transitMetadata)
          } else {
            txnStartTimestamp = transitMetadata.txnStartTimestamp
            addPartitions(transitMetadata.topicPartitions)
          }

        case PrepareAbort | PrepareCommit => // from endTxn
          if (!validProducerEpoch(transitMetadata) ||
            !topicPartitions.toSet.equals(transitMetadata.topicPartitions) ||
            txnTimeoutMs != transitMetadata.txnTimeoutMs ||
            txnStartTimestamp != transitMetadata.txnStartTimestamp) {

            throwStateTransitionFailure(transitMetadata)
          }

        case CompleteAbort | CompleteCommit => // from write markers
          if (!validProducerEpoch(transitMetadata) ||
            txnTimeoutMs != transitMetadata.txnTimeoutMs ||
            transitMetadata.txnStartTimestamp == -1) {

            throwStateTransitionFailure(transitMetadata)
          } else {
            txnStartTimestamp = transitMetadata.txnStartTimestamp
            topicPartitions.clear()
          }

        case PrepareEpochFence =>
          // We should never get here, since once we prepare to fence the epoch, we immediately set the pending state
          // to PrepareAbort, and then consequently to CompleteAbort after the markers are written.. So we should never
          // ever try to complete a transition to PrepareEpochFence, as it is not a valid previous state for any other state, and hence
          // can never be transitioned out of.
          throwStateTransitionFailure(transitMetadata)


        case Dead =>
          // The transactionalId was being expired. The completion of the operation should result in removal of the
          // the metadata from the cache, so we should never realistically transition to the dead state.
          throw new IllegalStateException(s"TransactionalId $transactionalId is trying to complete a transition to " +
            s"$toState. This means that the transactionalId was being expired, and the only acceptable completion of " +
            s"this operation is to remove the transaction metadata from the cache, not to persist the $toState in the log.")
      }

      debug(s"TransactionalId $transactionalId complete transition from $state to $transitMetadata")
      txnLastUpdateTimestamp = transitMetadata.txnLastUpdateTimestamp
      pendingState = None
      state = toState //note: 状态改变成功
    }
  }

  private def validProducerEpoch(transitMetadata: TxnTransitMetadata): Boolean = {
    val transitEpoch = transitMetadata.producerEpoch
    val transitProducerId = transitMetadata.producerId
    transitEpoch == producerEpoch && transitProducerId == producerId
  }

  private def validProducerEpochBump(transitMetadata: TxnTransitMetadata): Boolean = {
    val transitEpoch = transitMetadata.producerEpoch
    val transitProducerId = transitMetadata.producerId
    transitEpoch == producerEpoch + 1 || (transitEpoch == 0 && transitProducerId != producerId)
  }

  private def throwStateTransitionFailure(txnTransitMetadata: TxnTransitMetadata): Unit = {
    fatal(s"${this.toString}'s transition to $txnTransitMetadata failed: this should not happen")

    throw new IllegalStateException(s"TransactionalId $transactionalId failed transition to state $txnTransitMetadata " +
      "due to unexpected metadata")
  }

  def pendingTransitionInProgress: Boolean = pendingState.isDefined

  override def toString: String = {
    "TransactionMetadata(" +
      s"transactionalId=$transactionalId, " +
      s"producerId=$producerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"txnTimeoutMs=$txnTimeoutMs, " +
      s"state=$state, " +
      s"pendingState=$pendingState, " +
      s"topicPartitions=$topicPartitions, " +
      s"txnStartTimestamp=$txnStartTimestamp, " +
      s"txnLastUpdateTimestamp=$txnLastUpdateTimestamp)"
  }

  //note: 判断多个指标，多个指标都相等时，才认为是 equal
  override def equals(that: Any): Boolean = that match {
    case other: TransactionMetadata =>
      transactionalId == other.transactionalId &&
      producerId == other.producerId &&
      producerEpoch == other.producerEpoch &&
      txnTimeoutMs == other.txnTimeoutMs &&
      state.equals(other.state) &&
      topicPartitions.equals(other.topicPartitions) &&
      txnStartTimestamp == other.txnStartTimestamp &&
      txnLastUpdateTimestamp == other.txnLastUpdateTimestamp
    case _ => false
  }

  override def hashCode(): Int = {
    val fields = Seq(transactionalId, producerId, producerEpoch, txnTimeoutMs, state, topicPartitions,
      txnStartTimestamp, txnLastUpdateTimestamp)
    fields.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
