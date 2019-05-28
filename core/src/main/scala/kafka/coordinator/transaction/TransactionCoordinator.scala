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

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache, ReplicaManager}
import kafka.utils.{Logging, Scheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{LogContext, Time}

object TransactionCoordinator {

  def apply(config: KafkaConfig,
            replicaManager: ReplicaManager,
            scheduler: Scheduler,
            zkClient: KafkaZkClient,
            metrics: Metrics,
            metadataCache: MetadataCache,
            time: Time): TransactionCoordinator = {

    val txnConfig = TransactionConfig(config.transactionalIdExpirationMs,
      config.transactionMaxTimeoutMs,
      config.transactionTopicPartitions,
      config.transactionTopicReplicationFactor,
      config.transactionTopicSegmentBytes,
      config.transactionsLoadBufferSize,
      config.transactionTopicMinISR,
      config.transactionAbortTimedOutTransactionCleanupIntervalMs,
      config.transactionRemoveExpiredTransactionalIdCleanupIntervalMs,
      config.requestTimeoutMs)

    val producerIdManager = new ProducerIdManager(config.brokerId, zkClient)
    // we do not need to turn on reaper thread since no tasks will be expired and there are no completed tasks to be purged
    val txnMarkerPurgatory = DelayedOperationPurgatory[DelayedTxnMarker]("txn-marker-purgatory", config.brokerId,
      reaperEnabled = false, timerEnabled = false)
    val txnStateManager = new TransactionStateManager(config.brokerId, zkClient, scheduler, replicaManager, txnConfig, time)

    val logContext = new LogContext(s"[TransactionCoordinator id=${config.brokerId}] ")
    val txnMarkerChannelManager = TransactionMarkerChannelManager(config, metrics, metadataCache, txnStateManager,
      txnMarkerPurgatory, time, logContext)

    new TransactionCoordinator(config.brokerId, txnConfig, scheduler, producerIdManager, txnStateManager, txnMarkerChannelManager,
      time, logContext)
  }

  private def initTransactionError(error: Errors): InitProducerIdResult = {
    InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, error)
  }

  private def initTransactionMetadata(txnMetadata: TxnTransitMetadata): InitProducerIdResult = {
    InitProducerIdResult(txnMetadata.producerId, txnMetadata.producerEpoch, Errors.NONE)
  }
}

/**
 * Transaction coordinator handles message transactions sent by producers and communicate with brokers
 * to update ongoing transaction's status.
 *
 * Each Kafka server instantiates a transaction coordinator which is responsible for a set of
 * producers. Producers with specific transactional ids are assigned to their corresponding coordinators;
 * Producers with no specific transactional id may talk to a random broker as their coordinators.
 */
class TransactionCoordinator(brokerId: Int,
                             txnConfig: TransactionConfig,
                             scheduler: Scheduler,
                             producerIdManager: ProducerIdManager,
                             txnManager: TransactionStateManager,
                             txnMarkerChannelManager: TransactionMarkerChannelManager,
                             time: Time,
                             logContext: LogContext) extends Logging {
  this.logIdent = logContext.logPrefix

  import TransactionCoordinator._

  type InitProducerIdCallback = InitProducerIdResult => Unit
  type AddPartitionsCallback = Errors => Unit
  type EndTxnCallback = Errors => Unit
  type ApiResult[T] = Either[Errors, T]

  /* Active flag of the coordinator */
  private val isActive = new AtomicBoolean(false)

  def handleInitProducerId(transactionalId: String,
                           transactionTimeoutMs: Int,
                           responseCallback: InitProducerIdCallback): Unit = {

    if (transactionalId == null) { //note: 只设置幂等性时，直接分配 pid 并返回
      // if the transactional id is null, then always blindly accept the request
      // and return a new producerId from the producerId manager
      val producerId = producerIdManager.generateProducerId()
      responseCallback(InitProducerIdResult(producerId, producerEpoch = 0, Errors.NONE)) //note: Epoch 设置为 0
    } else if (transactionalId.isEmpty) { //note: transaction.id 为空时，直接返回异常
      // if transactional id is empty then return error as invalid request. This is
      // to make TransactionCoordinator's behavior consistent with producer client
      responseCallback(initTransactionError(Errors.INVALID_REQUEST))
    } else if (!txnManager.validateTransactionTimeoutMs(transactionTimeoutMs)) { //note: 检查设置的 transaction.timeout.ms 设置是否有效
      // check transactionTimeoutMs is not larger than the broker configured maximum allowed value
      responseCallback(initTransactionError(Errors.INVALID_TRANSACTION_TIMEOUT))
    } else { //note: 事务性时的处理
      val coordinatorEpochAndMetadata = txnManager.getTransactionState(transactionalId).right.flatMap {
        case None => //note: 没有这个 txn.id 记录的情况下
          val producerId = producerIdManager.generateProducerId() //note: 分配相应的 pid
          val createdMetadata = new TransactionMetadata(transactionalId = transactionalId,
            producerId = producerId,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH, //note: producer epoch 初始值为 -1
            txnTimeoutMs = transactionTimeoutMs,
            state = Empty,
            topicPartitions = collection.mutable.Set.empty[TopicPartition],
            txnLastUpdateTimestamp = time.milliseconds()) //note: 构造 txn.id 的 txnMetadata（state 为 Empty）
          txnManager.putTransactionStateIfNotExists(transactionalId, createdMetadata)

        case Some(epochAndTxnMetadata) => Right(epochAndTxnMetadata)
      }

      val result: ApiResult[(Int, TxnTransitMetadata)] = coordinatorEpochAndMetadata.right.flatMap {
        existingEpochAndMetadata =>
          val coordinatorEpoch = existingEpochAndMetadata.coordinatorEpoch
          val txnMetadata = existingEpochAndMetadata.transactionMetadata

          txnMetadata.inLock { //note: 检查当前事务的状态信息
            prepareInitProduceIdTransit(transactionalId, transactionTimeoutMs, coordinatorEpoch, txnMetadata)
          }
      }

      //note: 检查当前事务的状态，并做相应的处理
      result match {
        case Left(error) =>
          responseCallback(initTransactionError(error))

        case Right((coordinatorEpoch, newMetadata)) => //note: 检查对应的事务状态
          if (newMetadata.txnState == PrepareEpochFence) { //note: 先 abort 正在进行事务，然后返回一个 CONCURRENT_TRANSACTIONS 异常
            // abort the ongoing transaction and then return CONCURRENT_TRANSACTIONS to let client wait and retry
            def sendRetriableErrorCallback(error: Errors): Unit = {
              if (error != Errors.NONE) {
                responseCallback(initTransactionError(error))
              } else {
                responseCallback(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))
              }
            }

            handleEndTransaction(transactionalId,
              newMetadata.producerId,
              newMetadata.producerEpoch,
              TransactionResult.ABORT,
              sendRetriableErrorCallback) //note: 取消当前的事务操作
          } else {
            def sendPidResponseCallback(error: Errors): Unit = {
              if (error == Errors.NONE) {
                info(s"Initialized transactionalId $transactionalId with producerId ${newMetadata.producerId} and producer " +
                  s"epoch ${newMetadata.producerEpoch} on partition " +
                  s"${Topic.TRANSACTION_STATE_TOPIC_NAME}-${txnManager.partitionFor(transactionalId)}")
                responseCallback(initTransactionMetadata(newMetadata))
              } else {
                info(s"Returning $error error code to client for $transactionalId's InitProducerId request")
                responseCallback(initTransactionError(error))
              }
            }

            //note: 追加相应的日志信息（日志追加成功，状态转移才会生效）
            txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendPidResponseCallback)
          }
      }
    }
  }

  //note: producer 启用事务性的情况下，检测此时事务的状态信息
  private def prepareInitProduceIdTransit(transactionalId: String,
                                          transactionTimeoutMs: Int,
                                          coordinatorEpoch: Int,
                                          txnMetadata: TransactionMetadata): ApiResult[(Int, TxnTransitMetadata)] = {
    if (txnMetadata.pendingTransitionInProgress) {
      // return a retriable exception to let the client backoff and retry
      Left(Errors.CONCURRENT_TRANSACTIONS)
    } else {
      // caller should have synchronized on txnMetadata already
      txnMetadata.state match {
        case PrepareAbort | PrepareCommit =>
          // reply to client and let it backoff and retry
          Left(Errors.CONCURRENT_TRANSACTIONS)

        case CompleteAbort | CompleteCommit | Empty => //note: 此时需要将状态转移到 Empty（此时状态并没有转移，只是在 PendingState 记录了将要转移的状态）
          val transitMetadata = if (txnMetadata.isProducerEpochExhausted) {
            val newProducerId = producerIdManager.generateProducerId()
            txnMetadata.prepareProducerIdRotation(newProducerId, transactionTimeoutMs, time.milliseconds())
          } else { //note: 增加 producer 的 epoch 值
            txnMetadata.prepareIncrementProducerEpoch(transactionTimeoutMs, time.milliseconds())
          }

          Right(coordinatorEpoch, transitMetadata)

        case Ongoing => //note: abort 当前的事务，并返回一个 CONCURRENT_TRANSACTIONS 异常，强制 client 去重试
          // indicate to abort the current ongoing txn first. Note that this epoch is never returned to the
          // user. We will abort the ongoing transaction and return CONCURRENT_TRANSACTIONS to the client.
          // This forces the client to retry, which will ensure that the epoch is bumped a second time. In
          // particular, if fencing the current producer exhausts the available epochs for the current producerId,
          // then when the client retries, we will generate a new producerId.
          Right(coordinatorEpoch, txnMetadata.prepareFenceProducerEpoch())

        case Dead | PrepareEpochFence => //note: 返回错误
          val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
            s"This is illegal as we should never have transitioned to this state."
          fatal(errorMsg)
          throw new IllegalStateException(errorMsg)

      }
    }
  }

  //note: AddPartitionsToTxnRequest 的处理，这里只是记录相应的日志信息
  def handleAddPartitionsToTransaction(transactionalId: String,
                                       producerId: Long,
                                       producerEpoch: Short,
                                       partitions: collection.Set[TopicPartition],
                                       responseCallback: AddPartitionsCallback): Unit = {
    if (transactionalId == null || transactionalId.isEmpty) {
      debug(s"Returning ${Errors.INVALID_REQUEST} error code to client for $transactionalId's AddPartitions request")
      responseCallback(Errors.INVALID_REQUEST)
    } else {
      // try to update the transaction metadata and append the updated metadata to txn log;
      // if there is no such metadata treat it as invalid producerId mapping error.
      val result: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).right.flatMap {
        case None => Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndMetadata) =>
          val coordinatorEpoch = epochAndMetadata.coordinatorEpoch
          val txnMetadata = epochAndMetadata.transactionMetadata

          // generate the new transaction metadata with added partitions
          txnMetadata.inLock { //note: 这些在操作时，都会对 txn.id 的 meta 信息加锁
            if (txnMetadata.producerId != producerId) {
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            } else if (txnMetadata.producerEpoch != producerEpoch) {
              Left(Errors.INVALID_PRODUCER_EPOCH)
            } else if (txnMetadata.pendingTransitionInProgress) {
              // return a retriable exception to let the clie nt backoff and retry
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == PrepareCommit || txnMetadata.state == PrepareAbort) {
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == Ongoing && partitions.subsetOf(txnMetadata.topicPartitions)) {
              //note: 如果 partition 已经在对应的 meta 中，立即返回 ok
              // this is an optimization: if the partitions are already in the metadata reply OK immediately
              Left(Errors.NONE)
            } else {
              Right(coordinatorEpoch, txnMetadata.prepareAddPartitions(partitions.toSet, time.milliseconds())) //note: 这里会更新 txn 的 meta，并更新状态为 Ongoing
            }
          }
      }

      result match {
        case Left(err) =>
          debug(s"Returning $err error code to client for $transactionalId's AddPartitions request")
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, responseCallback)
      }
    }
  }

  def handleTxnImmigration(txnTopicPartitionId: Int, coordinatorEpoch: Int) {
    txnManager.loadTransactionsForTxnTopicPartition(txnTopicPartitionId, coordinatorEpoch, txnMarkerChannelManager.addTxnMarkersToSend)
  }

  def handleTxnEmigration(txnTopicPartitionId: Int, coordinatorEpoch: Int) {
      txnManager.removeTransactionsForTxnTopicPartition(txnTopicPartitionId, coordinatorEpoch)
      txnMarkerChannelManager.removeMarkersForTxnTopicPartition(txnTopicPartitionId)
  }

  //note: 无效的 txn 状态，返回相应的错误
  private def logInvalidStateTransitionAndReturnError(transactionalId: String,
                                                      transactionState: TransactionState,
                                                      transactionResult: TransactionResult) = {
    debug(s"TransactionalId: $transactionalId's state is $transactionState, but received transaction " +
      s"marker result to send: $transactionResult")
    Left(Errors.INVALID_TXN_STATE)
  }

  //note: 结束当前的事务操作
  def handleEndTransaction(transactionalId: String,
                           producerId: Long,
                           producerEpoch: Short,
                           txnMarkerResult: TransactionResult,
                           responseCallback: EndTxnCallback): Unit = {
    if (transactionalId == null || transactionalId.isEmpty)
      responseCallback(Errors.INVALID_REQUEST)
    else {
      //note: 将要添加的结果
      val preAppendResult: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).right.flatMap {
        case None =>
          Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndTxnMetadata) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val coordinatorEpoch = epochAndTxnMetadata.coordinatorEpoch

          txnMetadata.inLock { //note: 这个加锁是 txn Metadata 的级别，也就是事务级别
            if (txnMetadata.producerId != producerId)
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            else if (producerEpoch < txnMetadata.producerEpoch)
              Left(Errors.INVALID_PRODUCER_EPOCH)  //note:  当前事务的状态正在转移
            else if (txnMetadata.pendingTransitionInProgress && txnMetadata.pendingState.get != PrepareEpochFence)
              Left(Errors.CONCURRENT_TRANSACTIONS)
            else txnMetadata.state match {
              case Ongoing => //note: commit or abort，只有这个状态才是正常返回的
                val nextState = if (txnMarkerResult == TransactionResult.COMMIT)
                  PrepareCommit
                else
                  PrepareAbort

                if (nextState == PrepareAbort && txnMetadata.pendingState.contains(PrepareEpochFence)) {
                  //note: 将正在等待事务的状态转移成 None
                  // We should clear the pending state to make way for the transition to PrepareAbort and also bump
                  // the epoch in the transaction metadata we are about to append.
                  txnMetadata.pendingState = None
                  txnMetadata.producerEpoch = producerEpoch
                }

                Right(coordinatorEpoch, txnMetadata.prepareAbortOrCommit(nextState, time.milliseconds()))
              case CompleteCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case CompleteAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case PrepareCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  Left(Errors.CONCURRENT_TRANSACTIONS) //note: 当前事务正在进行
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case PrepareAbort =>
                if (txnMarkerResult == TransactionResult.ABORT) //note: 当前事务正在进行
                  Left(Errors.CONCURRENT_TRANSACTIONS)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case Empty =>
                logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case Dead | PrepareEpochFence =>
                val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
                  s"This is illegal as we should never have transitioned to this state."
                fatal(errorMsg)
                throw new IllegalStateException(errorMsg)

            }
          }
      }

      preAppendResult match {
        case Left(err) => //note: 有错误时，返回异常
          debug(s"Aborting append of $txnMarkerResult to transaction log with coordinator and returning $err error to client for $transactionalId's EndTransaction request")
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          def sendTxnMarkersCallback(error: Errors): Unit = { //note: WriteTxnMarkerRequest 结果的回调函数
            if (error == Errors.NONE) {
              val preSendResult: ApiResult[(TransactionMetadata, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).right.flatMap {
                case None =>
                  val errorMsg = s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                    s"no metadata in the cache; this is not expected"
                  fatal(errorMsg)
                  throw new IllegalStateException(errorMsg)

                case Some(epochAndMetadata) =>
                  if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                    val txnMetadata = epochAndMetadata.transactionMetadata
                    txnMetadata.inLock {
                      if (txnMetadata.producerId != producerId)
                        Left(Errors.INVALID_PRODUCER_ID_MAPPING)
                      else if (txnMetadata.producerEpoch != producerEpoch)
                        Left(Errors.INVALID_PRODUCER_EPOCH)
                      else if (txnMetadata.pendingTransitionInProgress)
                        Left(Errors.CONCURRENT_TRANSACTIONS)
                      else txnMetadata.state match {
                        case Empty| Ongoing | CompleteCommit | CompleteAbort =>
                          logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                        case PrepareCommit =>  //note: commit 成功
                          if (txnMarkerResult != TransactionResult.COMMIT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case PrepareAbort => //note: abort 成功
                          if (txnMarkerResult != TransactionResult.ABORT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case Dead | PrepareEpochFence =>
                          val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
                            s"This is illegal as we should never have transitioned to this state."
                          fatal(errorMsg)
                          throw new IllegalStateException(errorMsg)

                      }
                    }
                  } else { //note: coordinatorEpoch 发生了变化
                    debug(s"The transaction coordinator epoch has changed to ${epochAndMetadata.coordinatorEpoch} after $txnMarkerResult was " +
                      s"successfully appended to the log for $transactionalId with old epoch $coordinatorEpoch")
                    Left(Errors.NOT_COORDINATOR)
                  }
              }

              preSendResult match {
                case Left(err) =>
                  info(s"Aborting sending of transaction markers after appended $txnMarkerResult to transaction log and returning $err error to client for $transactionalId's EndTransaction request")
                  responseCallback(err)

                case Right((txnMetadata, newPreSendMetadata)) => //note: 如果日志追加成功，这里会立刻向 client 返回，并且添加一个 txn marker
                  // we can respond to the client immediately and continue to write the txn markers if
                  // the log append was successful
                  // note: 本地日志追加成功后，就向 client 返回结果，后面就是 kafka 本身需要保证的问题了
                  responseCallback(Errors.NONE)


                  txnMarkerChannelManager.addTxnMarkersToSend(transactionalId, coordinatorEpoch, txnMarkerResult, txnMetadata, newPreSendMetadata)
              }
            } else { //note: 因为追加事务日志失败，这里会丢弃发送事务 marker，并且向用户返回一个 error
              info(s"Aborting sending of transaction markers and returning $error error to client for $transactionalId's EndTransaction request of $txnMarkerResult, " +
                s"since appending $newMetadata to transaction log with coordinator epoch $coordinatorEpoch failed")

              responseCallback(error)
            }
          }

          //note: 添加事务日志信息，并且通过 WriteTxnMarkerRequest 请求发送一个 COMMIT 或者 ABORT 的 marker 给用户的 topic
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendTxnMarkersCallback)
      }
    }
  }

  def transactionTopicConfigs: Properties = txnManager.transactionTopicConfigs

  def partitionFor(transactionalId: String): Int = txnManager.partitionFor(transactionalId)

  private def abortTimedOutTransactions(): Unit = {
    txnManager.timedOutTransactions().foreach { txnIdAndPidEpoch =>
      txnManager.getTransactionState(txnIdAndPidEpoch.transactionalId).right.flatMap {
        case None =>
          error(s"Could not find transaction metadata when trying to timeout transaction with transactionalId " +
            s"${txnIdAndPidEpoch.transactionalId}. ProducerId: ${txnIdAndPidEpoch.producerId}. ProducerEpoch: " +
            s"${txnIdAndPidEpoch.producerEpoch}")
          Left(Errors.INVALID_TXN_STATE)

        case Some(epochAndTxnMetadata) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val transitMetadata = txnMetadata.inLock {
            if (txnMetadata.producerId != txnIdAndPidEpoch.producerId) {
              error(s"Found incorrect producerId when expiring transactionalId: ${txnIdAndPidEpoch.transactionalId}. " +
                s"Expected producerId: ${txnIdAndPidEpoch.producerId}. Found producerId: " +
                s"${txnMetadata.producerId}")
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            } else if (txnMetadata.pendingTransitionInProgress) {
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else {
              Right(txnMetadata.prepareFenceProducerEpoch())
            }
          }
          transitMetadata match {
            case Right(txnTransitMetadata) =>
              handleEndTransaction(txnMetadata.transactionalId,
                txnTransitMetadata.producerId,
                txnTransitMetadata.producerEpoch,
                TransactionResult.ABORT,
                {
                  case Errors.NONE =>
                    info(s"Completed rollback ongoing transaction of transactionalId: ${txnIdAndPidEpoch.transactionalId} due to timeout")
                  case e @ (Errors.INVALID_PRODUCER_ID_MAPPING |
                            Errors.INVALID_PRODUCER_EPOCH |
                            Errors.CONCURRENT_TRANSACTIONS) =>
                    debug(s"Rolling back ongoing transaction of transactionalId: ${txnIdAndPidEpoch.transactionalId} has aborted due to ${e.exceptionName}")
                  case e =>
                    warn(s"Rolling back ongoing transaction of transactionalId: ${txnIdAndPidEpoch.transactionalId} failed due to ${e.exceptionName}")
                })
              Right(txnTransitMetadata)
            case (error) =>
              Left(error)
          }
      }
    }
  }

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableTransactionalIdExpiration: Boolean = true) {
    info("Starting up.")
    scheduler.startup()
    //note: 定时移除超时的事务
    scheduler.schedule("transaction-abort",
      abortTimedOutTransactions,
      txnConfig.abortTimedOutTransactionsIntervalMs,
      txnConfig.abortTimedOutTransactionsIntervalMs
    )
    if (enableTransactionalIdExpiration) //note: 默认是允许 txn.id 超时的
      txnManager.enableTransactionalIdExpiration()
    txnMarkerChannelManager.start() //note: 启动发送 WriteTxnMarkersRequest 线程
    isActive.set(true)

    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    scheduler.shutdown()
    producerIdManager.shutdown()
    txnManager.shutdown()
    txnMarkerChannelManager.shutdown()
    info("Shutdown complete.")
  }
}

case class InitProducerIdResult(producerId: Long, producerEpoch: Short, error: Errors)
