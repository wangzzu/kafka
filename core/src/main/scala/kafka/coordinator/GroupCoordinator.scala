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
package kafka.coordinator

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{OffsetFetchResponse, JoinGroupRequest}

import scala.collection.{Map, Seq, immutable}


/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 */
//NOTE: GroupCoordinator 用来处理一般的 group 成员关系和 offset 管理
//NOTE: 每个 Broker Server 都有一个 Coordinator 实例,它负责一个或者多个 group,group 根据其名字进行分配到具体的 GroupCoordinator 上
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = (Array[Byte], Short) => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableMetadataExpiration: Boolean = true) {
    info("Starting up.")
    if (enableMetadataExpiration)
      groupManager.enableMetadataExpiration()
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
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  def handleJoinGroup(groupId: String,
                      memberId: String,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback) {
    if (!isActive.get) {//NOTE: GroupCoordinator 关闭或者没有启动
      responseCallback(joinError(memberId, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!validGroupId(groupId)) {//NOTE: groupId 无效（为空,或 null）
      responseCallback(joinError(memberId, Errors.INVALID_GROUP_ID.code))
    } else if (!isCoordinatorForGroup(groupId)) {//NOTE: 如果这个 group 按照算法不应该这个 GroupCoordinator 上时
      responseCallback(joinError(memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
    } else if (isCoordinatorLoadingInProgress(groupId)) {//NOTE: 如果这个 group 的信息正在被加载
      responseCallback(joinError(memberId, Errors.GROUP_LOAD_IN_PROGRESS.code))
    } else if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
               sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {//NOTE: session 定时超出范围
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT.code))
    } else {//NOTE:
      // only try to create the group if the group is not unknown AND
      // the member id is UNKNOWN, if member is specified but group does not
      // exist we should reject the request
      groupManager.getGroup(groupId) match {
          //NOTE: group 是未知的并且其 memberId 为空,则创建该 group
        case None =>
          if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {//NOTE: group 不存在,但 member 不为空,拒绝该请求
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
          } else {//NOTE: member 为空,group 不存在,这个是首次创建该 group 时的状态
            val group = groupManager.addGroup(new GroupMetadata(groupId))//NOTE: 将 group 相关信息存入到 groupManager 中
            //NOTE:加入 group
            doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
          }

        //NOTE: 如果 group 存在,直接加入 group
        case Some(group) =>
          doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
      }
    }
  }

  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback) {
    group synchronized {
      if (!group.is(Empty) && (group.protocolType != Some(protocolType) || !group.supportsProtocols(protocols.map(_._1).toSet))) {
        // if the new member does not support the group protocol, reject it
        //note: 协议不支持的情况
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL.code))
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // if the member trying to register with a un-recognized id, send the response to let
        // it reset its member id and retry
        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
      } else {//note: 根据 group 不同的状态信息将 member 加入到 group 中
        group.currentState match {
          case Dead => //note: group 已经 dead 的话,就直接返回错误: UNKNOWN_MEMBER_ID
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))

          case PreparingRebalance => //note: member 要么是新加入的,要么是重新加入的,分别调用对应的方法
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            }

          case AwaitingSync =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {//note: 新加入的 member,进行 rebalance
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              if (member.matches(protocols)) {//note: 旧的 member,向其返回 group 的状态信息,group 状态不变
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                responseCallback(JoinGroupResult(
                  members = if (memberId == group.leaderId) {
                    group.currentMemberMetadata
                  } else {
                    Map.empty
                  },
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocol,
                  leaderId = group.leaderId,
                  errorCode = Errors.NONE.code))
              } else {//note: 旧的 member, 但改变了 metadata,进行 rebalance
                // member has changed metadata, so force a rebalance
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              }
            }

          case Empty | Stable => //note: stable 状态或 Empty （刚初始化,还没有成员）状态
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              //note: client 首次发送 joinGroup 请求时,直接加入 group 并进行 rebalance
              // if the member id is unknown, register the member to the group
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {//note: 该 member 已经在 group 中
              val member = group.get(memberId)
              if (memberId == group.leaderId || !member.matches(protocols)) {
                //note: 如果是 leader 重新发送了 join 请求,或者是一个 member 的 meta 信息变化
                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                // The latter allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              } else {
                //note: 已经存在的 follower 请求,而且状态信息并未变化
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup
                responseCallback(JoinGroupResult(
                  members = Map.empty,
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocol,
                  leaderId = group.leaderId,
                  errorCode = Errors.NONE.code))
              }
            }
        }

        if (group.is(PreparingRebalance))//note: 如果进行 rebalance 的话,完成那些延迟的操作
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))
      }
    }
  }

  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback) {
    if (!isActive.get) {//NOTE: GroupCoordinator 关闭或没有启动
      responseCallback(Array.empty, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {//NOTE: group 不在这个 GroupCoordinator 上
      responseCallback(Array.empty, Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else {
      groupManager.getGroup(groupId) match {
        //NOTE: group 还不存在,证明 group 还未创建,返回 UNKNOWN_MEMBER_ID 错误
        case None => responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
        //NOTE: 同步 group assignment 信息
        case Some(group) => doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
      }
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback) {
    var delayedGroupStore: Option[DelayedStore] = None

    group synchronized {
      if (!group.has(memberId)) {//NOTE: group 中不包含这个 member,返回 UNKNOWN_MEMBER_ID 错误
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
      } else if (generationId != group.generationId) {//NOTE: 与 group 不在同一个更新代下
        responseCallback(Array.empty, Errors.ILLEGAL_GENERATION.code)
      } else {
        group.currentState match {
          case Empty | Dead => //NOTE: 返回 UNKNOWN_MEMBER_ID
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)

          case PreparingRebalance => //NOTE: 正在进行 rebalance
            responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS.code)

          case AwaitingSync => //NOTE: 只有 group 是在 AwaitingSync 状态下,才能正确处理
            group.get(memberId).awaitingSyncCallback = responseCallback//NOTE: sendResponseCallback

            // if this is the leader, then we can attempt to persist state and transition to stable
            if (memberId == group.leaderId) {//NOTE: 该 member 为 leader,从其获取 assignment
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              val missing = group.allMembers -- groupAssignment.keySet
              //NOTE:  将 missing 的 member 的 assignment 设置为空
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              delayedGroupStore = groupManager.prepareStoreGroup(group, assignment, (error: Errors) => {
                group synchronized {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the AwaitingSync state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  if (group.is(AwaitingSync) && generationId == group.generationId) {//NOTE: 保证 group 状态及代数没变
                    if (error != Errors.NONE) {
                      resetAndPropagateAssignmentError(group, error)//NOTE: 将 member 的 assignment 置为空,并发送错误
                      maybePrepareRebalance(group)//NOTE: rebalance
                    } else {
                      setAndPropagateAssignment(group, assignment)//NOTE: 进行 assignment
                      group.transitionTo(Stable)//NOTE: group 的状态由 awaitSync 变为 stable
                    }
                  }
                }
              })
            }

          case Stable => //NOTE: 直接向 client 返回其 member 对应的 assignment
            // if the group is stable, we just return the current assignment
            val memberMetadata = group.get(memberId)
            responseCallback(memberMetadata.assignment, Errors.NONE.code)
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }

    // store the group metadata without holding the group lock to avoid the potential
    // for deadlock if the callback is invoked holding other locks (e.g. the replica
    // state change lock)
    delayedGroupStore.foreach(groupManager.store)
  }

  def handleLeaveGroup(groupId: String, memberId: String, responseCallback: Short => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      responseCallback(Errors.GROUP_LOAD_IN_PROGRESS.code)
    } else {
      groupManager.getGroup(groupId) match {
        case None =>
          // if the group is marked as dead, it means some other thread has just removed the group
          // from the coordinator metadata; this is likely that the group has migrated to some other
          // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
          // joining without specified consumer id,
          responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

        case Some(group) =>
          group synchronized {
            if (group.is(Dead) || !group.has(memberId)) {
              responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
            } else {
              val member = group.get(memberId)
              removeHeartbeatForLeavingMember(group, member)//NOTE: 认为心跳完成
              onMemberFailure(group, member)//NOTE: 从 group 移除当前 member,并进行 rebalance
              responseCallback(Errors.NONE.code)
            }
          }
      }
    }
  }

  //NOTE: Server 端处理心跳请求
  def handleHeartbeat(groupId: String,
                      memberId: String,
                      generationId: Int,
                      responseCallback: Short => Unit) {
    if (!isActive.get) {//NOTE: GroupCoordinator 已经失败
      responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {//NOTE: 当前的 GroupCoordinator 不包含这个 group
      responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (isCoordinatorLoadingInProgress(groupId)) {//NOTE: group 的状态信息正在 loading,直接返回成功结果
      // the group is still loading, so respond just blindly
      responseCallback(Errors.NONE.code)
    } else {
      groupManager.getGroup(groupId) match {
        case None => //NOTE: 当前 GroupCoordinator 不包含这个 group
          responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

        case Some(group) => //NOTE: 包含这个 group
          group synchronized {
            group.currentState match {
              case Dead => //NOTE: group 的状态已经变为 dead,意味着 group 的 meta 已经被清除,返回 UNKNOWN_MEMBER_ID 错误
                // if the group is marked as dead, it means some other thread has just removed the group
                // from the coordinator metadata; this is likely that the group has migrated to some other
                // coordinator OR the group is in a transient unstable phase. Let the member retry
                // joining without the specified member id,
                responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

              case Empty => //NOTE: group 的状态为 Empty, 意味着 group 的成员为空,返回 UNKNOWN_MEMBER_ID 错误
                responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

              case AwaitingSync => //NOTE: group 状态为 AwaitingSync, 意味着 group 刚 rebalance 结束
                if (!group.has(memberId)) //NOTE: group 不包含这个 member,返回 UNKNOWN_MEMBER_ID 错误
                  responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
                else //NOTE: 返回当前 group 正在进行 rebalance,要求 client rejoin 这个 group
                  responseCallback(Errors.REBALANCE_IN_PROGRESS.code)

              case PreparingRebalance => //NOTE: group 状态为 PreparingRebalance
                if (!group.has(memberId)) { //NOTE: group 不包含这个 member,返回 UNKNOWN_MEMBER_ID 错误
                  responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
                } else if (generationId != group.generationId) {
                  responseCallback(Errors.ILLEGAL_GENERATION.code)
                } else { //NOTE: 正常处理心跳信息,并返回 REBALANCE_IN_PROGRESS 错误
                  val member = group.get(memberId)
                  //note: 更新心跳时间,认为心跳完成,并监控下次的调度情况（超时的话,会把这个 member 从 group 中移除）
                  completeAndScheduleNextHeartbeatExpiration(group, member)
                  responseCallback(Errors.REBALANCE_IN_PROGRESS.code)
                }

              case Stable =>
                if (!group.has(memberId)) {
                  responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
                } else if (generationId != group.generationId) {
                  responseCallback(Errors.ILLEGAL_GENERATION.code)
                } else { //NOTE: 正确处理心跳信息
                  val member = group.get(memberId)
                  //note: 更新心跳时间,认为心跳完成,并监控下次的调度情况（超时的话,会把这个 member 从 group 中移除）
                  completeAndScheduleNextHeartbeatExpiration(group, member)
                  responseCallback(Errors.NONE.code)
                }
            }
          }
      }
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Short] => Unit) {
    if (!isActive.get) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.NOT_COORDINATOR_FOR_GROUP.code))
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_LOAD_IN_PROGRESS.code))
    } else {
      groupManager.getGroup(groupId) match {
        case None =>
          if (generationId < 0) {
            // the group is not relying on Kafka for group management, so allow the commit
            //note: 不使用 group-coordinator 管理的情况
            //note: 如果 groupID不存在,就新建一个 GroupMetadata, 其group 状态为 Empty,否则就返回已有的 groupid
            //note: 如果 simple 的 groupId 与一个 active 的 group 重复了,这里就有可能被覆盖掉了
            val group = groupManager.addGroup(new GroupMetadata(groupId))
            doCommitOffsets(group, memberId, generationId, offsetMetadata, responseCallback)
          } else {
            // or this is a request coming from an older generation. either way, reject the commit
            //note: 过期的 offset-commit
            responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
          }

        case Some(group) =>
          doCommitOffsets(group, memberId, generationId, offsetMetadata, responseCallback)
      }
    }
  }

  private def doCommitOffsets(group: GroupMetadata,
                      memberId: String,
                      generationId: Int,
                      offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                      responseCallback: immutable.Map[TopicPartition, Short] => Unit) {
    var delayedOffsetStore: Option[DelayedStore] = None

    group synchronized {
      if (group.is(Dead)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
      } else if (generationId < 0 && group.is(Empty)) {//note: 来自 assign 的情况
        // the group is only using Kafka to store offsets
        delayedOffsetStore = groupManager.prepareStoreOffsets(group, memberId, generationId,
          offsetMetadata, responseCallback)
      } else if (group.is(AwaitingSync)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS.code))
      } else if (!group.has(memberId)) {//note: 有可能 simple 与 high level 的冲突了,这里就直接拒绝相应的请求
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
      } else {
        val member = group.get(memberId)
        completeAndScheduleNextHeartbeatExpiration(group, member)//note: 更新下次需要的心跳时间
        delayedOffsetStore = groupManager.prepareStoreOffsets(group, memberId, generationId,
          offsetMetadata, responseCallback) //note: commite offset
      }
    }

    // store the offsets without holding the group lock
    delayedOffsetStore.foreach(groupManager.store)
  }

  def handleFetchOffsets(groupId: String,
                         partitions: Option[Seq[TopicPartition]] = None): (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {
    if (!isActive.get)
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, Map())
    else if (!isCoordinatorForGroup(groupId)) {
      debug("Could not fetch offsets for group %s (not group coordinator).".format(groupId))
      (Errors.NOT_COORDINATOR_FOR_GROUP, Map())
    } else if (isCoordinatorLoadingInProgress(groupId))
      (Errors.GROUP_LOAD_IN_PROGRESS, Map())
    else {
      // return offsets blindly regardless the current group state since the group may be using
      // Kafka commit storage without automatic group management
      (Errors.NONE, groupManager.getOffsets(groupId, partitions))
    }
  }

  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading()) Errors.GROUP_LOAD_IN_PROGRESS else Errors.NONE
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    if (!isActive.get) {
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, GroupCoordinator.EmptyGroup)
    } else if (!isCoordinatorForGroup(groupId)) {
      (Errors.NOT_COORDINATOR_FOR_GROUP, GroupCoordinator.EmptyGroup)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      (Errors.GROUP_LOAD_IN_PROGRESS, GroupCoordinator.EmptyGroup)
    } else {
      groupManager.getGroup(groupId) match { //note: 返回 group 详细信息,主要是 member 的详细信息
        case None => (Errors.NONE, GroupCoordinator.DeadGroup)
        case Some(group) =>
          group synchronized {
            (Errors.NONE, group.summary)
          }
      }
    }
  }

  def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]) {
    groupManager.cleanupGroupMetadata(Some(topicPartitions))
  }

  private def onGroupUnloaded(group: GroupMetadata) {
    group synchronized {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState
      group.transitionTo(Dead)

      previousState match {
        case Empty | Dead =>
        case PreparingRebalance =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingJoinCallback != null) {
              member.awaitingJoinCallback(joinError(member.memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
              member.awaitingJoinCallback = null
            }
          }
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | AwaitingSync =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingSyncCallback != null) {
              member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR_FOR_GROUP.code)
              member.awaitingSyncCallback = null
            }
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  private def onGroupLoaded(group: GroupMetadata) {
    group synchronized {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable) || group.is(Empty))
      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  def handleGroupImmigration(offsetTopicPartitionId: Int) {
    groupManager.loadGroupsForPartition(offsetTopicPartitionId, onGroupLoaded)
  }

  def handleGroupEmigration(offsetTopicPartitionId: Int) {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  //NOTE: 根据 leader 发送过来的 assignment,更新 group 的每一个 member（确保 group 状态是 AwaitingSync)
  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
    assert(group.is(AwaitingSync))
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))//NOTE: assignment
    propagateAssignment(group, Errors.NONE)
  }

  //NOTE: 重置 group 的 assignment,将 group 的每个 member 置为空
  private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors) {
    assert(group.is(AwaitingSync))
    group.allMemberMetadata.foreach(_.assignment = Array.empty[Byte])
    propagateAssignment(group, error)
  }

  //NOTE: 调用回调函数,处理 group 所有的 assignment,将每个 member 的 assignment 分别更新到其 member 的 meta 上
  private def propagateAssignment(group: GroupMetadata, error: Errors) {
    for (member <- group.allMemberMetadata) {
      if (member.awaitingSyncCallback != null) {//note: 如果有相应的回调函数,就返回其 assign 结果
        member.awaitingSyncCallback(member.assignment, error.code)
        member.awaitingSyncCallback = null

        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        //note: 启动下一次的心跳监控
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  //NOTE: 判断 groupId 命名是否有效（不为空或 null）
  private def validGroupId(groupId: String): Boolean = {
    groupId != null && !groupId.isEmpty
  }

  private def joinError(memberId: String, errorCode: Short): JoinGroupResult = {
    JoinGroupResult(
      members=Map.empty,
      memberId=memberId,
      generationId=0,
      subProtocol=GroupCoordinator.NoProtocol,
      leaderId=GroupCoordinator.NoLeader,
      errorCode=errorCode)
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  //note: 更新心跳时间,认为心跳完成,并监控下次的调度情况
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
    // complete current heartbeat expectation
    member.latestHeartbeat = time.milliseconds()//NOTE: 更新上次心跳时间
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)//NOTE: 完成这次心跳的调度

    // reschedule the next heartbeat expiration deadline
    val newHeartbeatDeadline = member.latestHeartbeat + member.sessionTimeoutMs//NOTE: 计算下次心跳的 deadline
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
    //NOTE: 进行调度,会对心跳监控（如果超时,会把 member 移除 group）
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata) {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  //NOTE: 向 group 注册这个 member,并将 group 进行 rebalance
  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                    sessionTimeoutMs: Int,
                                    clientId: String,
                                    clientHost: String,
                                    protocolType: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback) = {
    // use the client-id with a random id suffix as the member-id
    val memberId = clientId + "-" + group.generateMemberIdSuffix//NOTE: 为客户端生成一个随机 id
    val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, protocols)
    member.awaitingJoinCallback = callback //NOTE: 回调函数传入
    group.add(member) //NOTE: 将 member 加入 group,若是 group 的第一个成员则设置为 leader
    maybePrepareRebalance(group) //NOTE: 进行 rebalance
    member
  }

  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback) {
    member.supportedProtocols = protocols
    member.awaitingJoinCallback = callback
    maybePrepareRebalance(group)
  }

  //NOTE: 如果可以进行 rebalance,那就进行 rebalance
  private def maybePrepareRebalance(group: GroupMetadata) {
    group synchronized {
      if (group.canRebalance)//NOTE: 根据当前 group 的状态（状态转换要求）,判断是否可以进行 rebalance, 已经在 rebalance 状态就不能再次进入 rebalance 状态
        prepareRebalance(group)
    }
  }

  //NOTE: 进行 rebalance 操作
  private def prepareRebalance(group: GroupMetadata) {
    // if any members are awaiting sync, cancel their request and have them rejoin
    //NOTE: 如果 group 的状态是 AwaitingSync,那么返回 group 正在 rebalance 的异常
    if (group.is(AwaitingSync))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    group.transitionTo(PreparingRebalance)//NOTE: 将 group 状态变为 rebalance
    info("Preparing to restabilize group %s with old generation %s".format(group.groupId, group.generationId))

    val rebalanceTimeout = group.rebalanceTimeoutMs
    val delayedRebalance = new DelayedJoin(this, group, rebalanceTimeout)//NOTE: 创建一个延迟操作,在一次 rebalance 中,只会创建一次
    val groupKey = GroupKey(group.groupId)
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  private def onMemberFailure(group: GroupMetadata, member: MemberMetadata) {
    trace("Member %s in group %s has failed".format(member.memberId, group.groupId))
    group.remove(member.memberId)//NOTE: 从 Group 移除当前 member 信息
    group.currentState match {
      case Dead | Empty =>
      case Stable | AwaitingSync => maybePrepareRebalance(group)//NOTE: 进行 rebalance
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))//NOTE: 检查操作是否可以完成
    }
  }

  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group synchronized {
      if (group.notYetRejoinedMembers.isEmpty)
        forceComplete()
      else false
    }
  }

  def onExpireJoin() {
    // TODO: add metrics for restabilize timeouts
  }

  //NOTE: 完成 rebalance 操作,向 leader 发送 group 的信息以进行 assign,follower 此项为空
  def onCompleteJoin(group: GroupMetadata) {
    var delayedStore: Option[DelayedStore] = None
    group synchronized {
      // remove any members who haven't joined the group yet
      group.notYetRejoinedMembers.foreach { failedMember => //NOTE: 从 group 中移除未 rejoin 的 member
        group.remove(failedMember.memberId)
        // TODO: cut the socket connection to the client
      }

      if (!group.is(Dead)) {
        group.initNextGeneration()//NOTE: 更新 group 的状态,变为 AwaitSync 或者 Empty 状态
        if (group.is(Empty)) {//NOTE: 测试 group 内没有 member
          info(s"Group ${group.groupId} with generation ${group.generationId} is now empty")

          delayedStore = groupManager.prepareStoreGroup(group, Map.empty, error => {
            if (error != Errors.NONE) {
              // we failed to write the empty group metadata. If the broker fails before another rebalance,
              // the previous generation written to the log will become active again (and most likely timeout).
              // This should be safe since there are no active members in an empty generation, so we just warn.
              warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
            }
          })
        } else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId}")

          // trigger the awaiting join group response callback for all the members after rebalancing
          //NOTE: 对 Group 的每个 member 都会触发这一操作
          for (member <- group.allMemberMetadata) {
            assert(member.awaitingJoinCallback != null)//NOTE: 发送 sendResponseCallback
            val joinResult = JoinGroupResult(//NOTE: 返回 join-group 的结果
              members=if (member.memberId == group.leaderId) { group.currentMemberMetadata } else { Map.empty },
              memberId=member.memberId,
              generationId=group.generationId,
              subProtocol=group.protocol,
              leaderId=group.leaderId,
              errorCode=Errors.NONE.code)

            member.awaitingJoinCallback(joinResult)//NOTE: 调用 sendResponseCallback 方法,发送 join-group 结果
            member.awaitingJoinCallback = null
            completeAndScheduleNextHeartbeatExpiration(group, member)
          }
        }
      }
    }

    // call without holding the group lock
    delayedStore.foreach(groupManager.store)
  }

  def tryCompleteHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group synchronized {
      if (shouldKeepMemberAlive(member, heartbeatDeadline) || member.isLeaving)
        forceComplete()
      else false
    }
  }

  def onExpireHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long) {
    group synchronized {
      if (!shouldKeepMemberAlive(member, heartbeatDeadline))
        onMemberFailure(group, member)
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  //note: 判断是否应该保持 member alive,如果 member 有 join 或 awaitingSync 的 callback 或者
  private def shouldKeepMemberAlive(member: MemberMetadata, heartbeatDeadline: Long) =
    member.awaitingJoinCallback != null || //note: coordinator 正在处理 consumer 的 join-group 请求
      member.awaitingSyncCallback != null || //note: coordinator 正在处理 consumer 的 sync-group 请求
      member.latestHeartbeat + member.sessionTimeoutMs > heartbeatDeadline //note: 心跳未超时

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadingInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            time: Time): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkUtils, replicaManager, heartbeatPurgatory, joinPurgatory, time)
  }

  private[coordinator] def offsetConfig(config: KafkaConfig) = OffsetConfig(
    maxMetadataSize = config.offsetMetadataMaxSize,
    loadBufferSize = config.offsetsLoadBufferSize,
    offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
    offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
    offsetsTopicNumPartitions = config.offsetsTopicPartitions,
    offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
    offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
    offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
    offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
    offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
  )

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time): GroupCoordinator = {
    val offsetConfig = this.offsetConfig(config)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
      offsetConfig, replicaManager, zkUtils, time)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time)
  }

}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int)

case class JoinGroupResult(members: Map[String, Array[Byte]],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           errorCode: Short)
