/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupCoordinatorNotAvailableException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.errors.InterruptException;

/**
 * AbstractCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See {@link ConsumerCoordinator} for example usage.
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of actions:
 *
 * <ol>
 *     <li>Group Registration: Group members register with the coordinator providing their own metadata
 *         (such as the set of topics they are interested in).</li>
 *     <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 *         as the leader.</li>
 *     <li>State Assignment: The leader collects the metadata from all the members of the group and
 *         assigns state.</li>
 *     <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 *         processing.</li>
 * </ol>
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in {@link #metadata()} and the format of the state assignment provided
 * by the leader in {@link #performAssignment(String, String, Map)} and becomes available to members in
 * {@link #onJoinComplete(int, String, String, ByteBuffer)}.
 *
 * Note on locking: this class shares state between the caller and a background thread which is
 * used for sending heartbeats after the client has joined the group. All mutable state as well as
 * state transitions are protected with the class's monitor. Generally this means acquiring the lock
 * before reading or writing the state of the group (e.g. generation, memberId) and holding the lock
 * when sending a request that affects the state of the group (e.g. JoinGroup, LeaveGroup).
 */
public abstract class AbstractCoordinator implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractCoordinator.class);

    private enum MemberState {
        UNJOINED,    // the client is not part of a group
        REBALANCING, // the client has begun rebalancing
        STABLE,      // the client has joined and is sending heartbeats
    }

    private final int rebalanceTimeoutMs;
    private final int sessionTimeoutMs;
    private final GroupCoordinatorMetrics sensors;
    private final Heartbeat heartbeat;
    protected final String groupId;
    protected final ConsumerNetworkClient client;
    protected final Time time;
    protected final long retryBackoffMs;

    private HeartbeatThread heartbeatThread = null;
    private boolean rejoinNeeded = true;
    private boolean needsJoinPrepare = true;
    private MemberState state = MemberState.UNJOINED;
    private RequestFuture<ByteBuffer> joinFuture = null;
    private Node coordinator = null;
    private Generation generation = Generation.NO_GENERATION;//NOTE: 初始化时,首次建立时,generation 的 memberId 为空

    private RequestFuture<Void> findCoordinatorFuture = null;

    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int rebalanceTimeoutMs,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs) {
        this.client = client;
        this.time = time;
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.heartbeat = new Heartbeat(sessionTimeoutMs, heartbeatIntervalMs, rebalanceTimeoutMs, retryBackoffMs);
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Unique identifier for the class of supported protocols (e.g. "consumer" or "connect").
     * @return Non-null protocol type name
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the list indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract List<ProtocolMetadata> metadata();

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any
     * cleanup from the previous generation (such as committing offsets for the consumer)
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    protected abstract void onJoinPrepare(int generation, String memberId);

    /**
     * Perform assignment for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     * @param leaderId The id of the leader (which is this member)
     * @param allMemberMetadata Metadata from all members of the group
     * @return A map from each member to their state assignment
     */
    protected abstract Map<String, ByteBuffer> performAssignment(String leaderId,
                                                                 String protocol,
                                                                 Map<String, ByteBuffer> allMemberMetadata);

    /**
     * Invoked when a group member has successfully joined a group.
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoinComplete(int generation,
                                           String memberId,
                                           String protocol,
                                           ByteBuffer memberAssignment);

    /**
     * Block until the coordinator for this group is known and is ready to receive requests.
     */
    //NOTE: block 直到获取 Group 的 GroupCoordinator
    public synchronized void ensureCoordinatorReady() {
        while (coordinatorUnknown()) {
            RequestFuture<Void> future = lookupCoordinator();//NOTE:  获取 GroupCoordinator,并建立连接
            client.poll(future);

            if (future.failed()) {//NOTE: 如果获取的过程中失败了
                if (future.isRetriable())
                    client.awaitMetadataUpdate();
                else
                    throw future.exception();
            } else if (coordinator != null && client.connectionFailed(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                coordinatorDead();
                time.sleep(retryBackoffMs);
            }
        }
    }

    protected synchronized RequestFuture<Void> lookupCoordinator() {
        if (findCoordinatorFuture == null) {
            // find a node to ask about the coordinator
            Node node = this.client.leastLoadedNode();//NOTE: 找一个节点,发送 groupCoordinator 的请求
            if (node == null) {
                // TODO: If there are no brokers left, perhaps we should use the bootstrap set
                // from configuration?
                return RequestFuture.noBrokersAvailable();
            } else
                findCoordinatorFuture = sendGroupCoordinatorRequest(node);//NOTE: 发送请求，并对 response 进行处理
        }
        return findCoordinatorFuture;
    }

    private synchronized void clearFindCoordinatorFuture() {
        findCoordinatorFuture = null;
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes)
     * @return true if it should, false otherwise
     */
    protected synchronized boolean needRejoin() {
        return rejoinNeeded;
    }

    private synchronized boolean rejoinIncomplete() {
        return joinFuture != null;
    }

    /**
     * Check the status of the heartbeat thread (if it is active) and indicate the liveness
     * of the client. This must be called periodically after joining with {@link #ensureActiveGroup()}
     * to ensure that the member stays in the group. If an interval of time longer than the
     * provided rebalance timeout expires without calling this method, then the client will proactively
     * leave the group.
     * @param now current time in milliseconds
     * @throws RuntimeException for unexpected errors raised from the heartbeat thread
     */
    //NOTE: 检查心跳线程的状态以来判断 client 是否存活,如果心跳线程失败,就抛出异常
    //NOTE: 必须周期性地向 GroupCoordinator 发送心跳,如果超时,那么 client 就有可能失败了,client 有可能离开了 group
    protected synchronized void pollHeartbeat(long now) {
        if (heartbeatThread != null) {
            if (heartbeatThread.hasFailed()) {//NOTE: 将心跳线程设置为空,并抛出异常,如果用户 catch 到异常,下次就可以重新启动该线程
                // set the heartbeat thread to null and raise an exception. If the user catches it,
                // the next call to ensureActiveGroup() will spawn a new heartbeat thread.
                RuntimeException cause = heartbeatThread.failureCause();
                heartbeatThread = null;
                throw cause;
            }

            heartbeat.poll(now);
        }
    }

    protected synchronized long timeToNextHeartbeat(long now) {
        // if we have not joined the group, we don't need to send heartbeats
        if (state == MemberState.UNJOINED)
            return Long.MAX_VALUE;
        return heartbeat.timeToNextHeartbeat(now);
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     */
    //NOTE: 确保 Group 是 active,并且加入该 group
    public void ensureActiveGroup() {
        // always ensure that the coordinator is ready because we may have been disconnected
        // when sending heartbeats and does not necessarily require us to rejoin the group.
        ensureCoordinatorReady();//NOTE: 确保 GroupCoordinator 已经连接
        startHeartbeatThreadIfNeeded();//NOTE: 启动心跳发送线程（并不一定发送心跳,满足条件后才会发送心跳）
        joinGroupIfNeeded();//NOTE: 发送 JoinGroup 请求,并对返回的信息进行处理
    }

    private synchronized void startHeartbeatThreadIfNeeded() {
        if (heartbeatThread == null) {
            heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();//NOTE:  启动发送心跳的线程
        }
    }

    private synchronized void disableHeartbeatThread() {
        if (heartbeatThread != null)
            heartbeatThread.disable();
    }

    // visible for testing. Joins the group without starting the heartbeat thread.
    void joinGroupIfNeeded() {
        while (needRejoin() || rejoinIncomplete()) {
            ensureCoordinatorReady();

            // call onJoinPrepare if needed. We set a flag to make sure that we do not call it a second
            // time if the client is woken up before a pending rebalance completes. This must be called
            // on each iteration of the loop because an event requiring a rebalance (such as a metadata
            // refresh which changes the matched subscription set) can occur while another rebalance is
            // still in progress.
            if (needsJoinPrepare) {
                onJoinPrepare(generation.generationId, generation.memberId);
                needsJoinPrepare = false;
            }

            RequestFuture<ByteBuffer> future = initiateJoinGroup();//NOTE: 初始化 JoinGroup 请求,并发送该请求
            client.poll(future);
            resetJoinGroupFuture();//NOTE: 重置 joinFuture 为空

            if (future.succeeded()) {
                needsJoinPrepare = true;
                onJoinComplete(generation.generationId, generation.memberId, generation.protocol, future.value());
            } else {
                RuntimeException exception = future.exception();
                if (exception instanceof UnknownMemberIdException ||
                        exception instanceof RebalanceInProgressException ||
                        exception instanceof IllegalGenerationException)
                    continue;
                else if (!future.isRetriable())
                    throw exception;
                time.sleep(retryBackoffMs);
            }
        }
    }

    //NOTE: 重置 JoinGroup 的 Future,下次再重新 join group 时,会重新初始化
    private synchronized void resetJoinGroupFuture() {
        this.joinFuture = null;
    }

    //NOTE: 发送 JoinGroup 的请求, 并添加 listener
    private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
        // we store the join future in case we are woken up by the user after beginning the
        // rebalance in the call to poll below. This ensures that we do not mistakenly attempt
        // to rejoin before the pending rebalance has completed.
        if (joinFuture == null) {
            // fence off the heartbeat thread explicitly so that it cannot interfere with the join group.
            // Note that this must come after the call to onJoinPrepare since we must be able to continue
            // sending heartbeats if that callback takes some time.
            disableHeartbeatThread();

            state = MemberState.REBALANCING;//NOTE: 标记为 rebalance
            joinFuture = sendJoinGroupRequest();//NOTE: 发送 JoinGroup 请求
            joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
                @Override
                public void onSuccess(ByteBuffer value) {
                    // handle join completion in the callback so that the callback will be invoked
                    // even if the consumer is woken up before finishing the rebalance
                    synchronized (AbstractCoordinator.this) {
                        log.info("Successfully joined group {} with generation {}", groupId, generation.generationId);
                        state = MemberState.STABLE;//NOTE: 标记 Consumer 为 stable

                        if (heartbeatThread != null)
                            heartbeatThread.enable();
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // we handle failures below after the request finishes. if the join completes
                    // after having been woken up, the exception is ignored and we will rejoin
                    synchronized (AbstractCoordinator.this) {
                        state = MemberState.UNJOINED;//NOTE: 标记 Consumer 为 Unjoined
                    }
                }
            });
        }
        return joinFuture;
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to {@link #performAssignment(String, String, Map)} if
     * elected leader by the coordinator.
     * @return A request future which wraps the assignment returned from the group leader
     */
    //NOTE: 发送 JoinGroup 请求,
    private RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        log.info("(Re-)joining group {}", groupId);
        JoinGroupRequest request = new JoinGroupRequest(
                groupId,
                this.sessionTimeoutMs,
                this.rebalanceTimeoutMs,
                this.generation.memberId,
                protocolType(),
                metadata());

        log.debug("Sending JoinGroup ({}) to coordinator {}", request, this.coordinator);
        return client.send(coordinator, ApiKeys.JOIN_GROUP, request)
                .compose(new JoinGroupResponseHandler());
    }

    //NOTE: 处理 JoinGroup response 的 handler（同步 group 信息）
    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            Errors error = Errors.forCode(joinResponse.errorCode());
            if (error == Errors.NONE) {
                log.debug("Received successful join group response for group {}: {}", groupId, joinResponse);
                sensors.joinLatency.record(response.requestLatencyMs());

                synchronized (AbstractCoordinator.this) {
                    if (state != MemberState.REBALANCING) {//NOTE: 如果此时 Consumer 的状态不是 rebalacing,就引起异常
                        // if the consumer was woken up before a rebalance completes, we may have already left
                        // the group. In this case, we do not want to continue with the sync group.
                        future.raise(new UnjoinedGroupException());
                    } else {
                        AbstractCoordinator.this.generation = new Generation(joinResponse.generationId(),
                                joinResponse.memberId(), joinResponse.groupProtocol());
                        AbstractCoordinator.this.rejoinNeeded = false;
                        //NOTE: 进行 aync-group 操作
                        if (joinResponse.isLeader()) {
                            onJoinLeader(joinResponse).chain(future);
                        } else {
                            onJoinFollower().chain(future);
                        }
                    }
                }
            } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                log.debug("Attempt to join group {} rejected since coordinator {} is loading the group.", groupId,
                        coordinator());
                // backoff and retry
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                // reset the member id and retry immediately
                resetGeneration();
                log.debug("Attempt to join group {} failed due to unknown member id.", groupId);
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                // re-discover the coordinator and retry with backoff
                coordinatorDead();
                log.debug("Attempt to join group {} failed due to obsolete coordinator information: {}", groupId, error.message());
                future.raise(error);
            } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                    || error == Errors.INVALID_SESSION_TIMEOUT
                    || error == Errors.INVALID_GROUP_ID) {
                // log the error and re-throw the exception
                log.error("Attempt to join group {} failed due to fatal error: {}", groupId, error.message());
                future.raise(error);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                // unexpected error, throw the exception
                future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
            }
        }
    }

    //NOTE: 当 consumer 为 follower 时,从 GroupCoordinator 拉取分配结果
    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        SyncGroupRequest request = new SyncGroupRequest(groupId, generation.generationId,
                generation.memberId, Collections.<String, ByteBuffer>emptyMap());
        log.debug("Sending follower SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, request);
        return sendSyncGroupRequest(request);
    }

    //NOTE: 当 Consumer 客户端为 leader 时,对 group 下的所有实例进行分配,将 assign 的结果发送到 GroupCoordinator
    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group
            Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.leaderId(), joinResponse.groupProtocol(),
                    joinResponse.members());//NOTE: 进行 assign 操作

            SyncGroupRequest request = new SyncGroupRequest(groupId, generation.generationId, generation.memberId, groupAssignment);
            log.debug("Sending leader SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, request);
            return sendSyncGroupRequest(request);//NOTE: 发送 sync-group 请求
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    //NOTE: 发送 SyncGroup 请求,获取对 partition 分配的安排
    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest request) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        return client.send(coordinator, ApiKeys.SYNC_GROUP, request)
                .compose(new SyncGroupResponseHandler());
    }

    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {
        @Override
        public void handle(SyncGroupResponse syncResponse,
                           RequestFuture<ByteBuffer> future) {
            Errors error = Errors.forCode(syncResponse.errorCode());
            if (error == Errors.NONE) {
                sensors.syncLatency.record(response.requestLatencyMs());
                future.complete(syncResponse.memberAssignment());
            } else {
                requestRejoin();

                if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {//NOTE: group 正在进攻 rebalance,任务失败
                    log.debug("SyncGroup for group {} failed due to coordinator rebalance", groupId);
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    resetGeneration();
                    future.raise(error);
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    coordinatorDead();
                    future.raise(error);
                } else {
                    future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    //NOTE: 发送 GroupCoordinator 的请求
    private RequestFuture<Void> sendGroupCoordinatorRequest(Node node) {
        // initiate the group metadata request
        log.debug("Sending coordinator request for group {} to broker {}", groupId, node);
        GroupCoordinatorRequest metadataRequest = new GroupCoordinatorRequest(this.groupId);
        return client.send(node, ApiKeys.GROUP_COORDINATOR, metadataRequest)
                     .compose(new GroupCoordinatorResponseHandler());
        //NOTE: compose 的作用是将 GroupCoordinatorResponseHandler 类转换为 RequestFuture
        //NOTE: 实际上就是为返回的 Future 类重置 onSuccess() 和 onFailure() 方法
    }

    //NOTE: 对 GroupCoordinator 的 response 进行处理,回调
    private class GroupCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received group coordinator response {}", resp);

            GroupCoordinatorResponse groupCoordinatorResponse = (GroupCoordinatorResponse) resp.responseBody();
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935
            Errors error = Errors.forCode(groupCoordinatorResponse.errorCode());
            clearFindCoordinatorFuture();
            if (error == Errors.NONE) {
                synchronized (AbstractCoordinator.this) {//NOTE: 如果获取 GroupCoordinator 正常时
                    AbstractCoordinator.this.coordinator = new Node(
                            Integer.MAX_VALUE - groupCoordinatorResponse.node().id(),
                            groupCoordinatorResponse.node().host(),
                            groupCoordinatorResponse.node().port());
                    log.info("Discovered coordinator {} for group {}.", coordinator, groupId);
                    client.tryConnect(coordinator);
                    heartbeat.resetTimeouts(time.milliseconds());
                }
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                log.debug("Group coordinator lookup for group {} failed: {}", groupId, error.message());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            clearFindCoordinatorFuture();
            super.onFailure(e, future);
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        return coordinator() == null;
    }

    /**
     * Get the current coordinator
     * @return the current coordinator or null if it is unknown
     */
    protected synchronized Node coordinator() {
        if (coordinator != null && client.connectionFailed(coordinator)) {
            coordinatorDead();
            return null;
        }
        return this.coordinator;
    }

    /**
     * Mark the current coordinator as dead.
     */
    //NOTE: 标记 consumerCoordinator 为 dead
    protected synchronized void coordinatorDead() {
        if (this.coordinator != null) {
            log.info("Marking the coordinator {} dead for group {}", this.coordinator, groupId);
            client.failUnsentRequests(this.coordinator, GroupCoordinatorNotAvailableException.INSTANCE);
            this.coordinator = null;
        }
    }

    /**
     * Get the current generation state if the group is stable.
     * @return the current generation or null if the group is unjoined/rebalancing
     */
    protected synchronized Generation generation() {
        if (this.state != MemberState.STABLE)
            return null;
        return generation;
    }

    /**
     * Reset the generation and memberId because we have fallen out of the group.
     */
    //NOTE: 重置 Consumer Coordinator 的 generation 和 member id
    protected synchronized void resetGeneration() {
        this.generation = Generation.NO_GENERATION;
        this.rejoinNeeded = true;
        this.state = MemberState.UNJOINED;
    }

    protected synchronized void requestRejoin() {
        this.rejoinNeeded = true;
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public synchronized void close() {
        if (heartbeatThread != null)
            heartbeatThread.close();
        maybeLeaveGroup();
    }

    /**
     * Leave the current group and reset local generation/memberId.
     */
    //NOTE: 发送 LeaveGroup 请求,并重置 generation/memeberId
    public synchronized void maybeLeaveGroup() {
        if (!coordinatorUnknown() && state != MemberState.UNJOINED && generation != Generation.NO_GENERATION) {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            LeaveGroupRequest request = new LeaveGroupRequest(groupId, generation.memberId);
            client.send(coordinator, ApiKeys.LEAVE_GROUP, request)
                    .compose(new LeaveGroupResponseHandler());
            client.pollNoWakeup();
        }

        resetGeneration();
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            Errors error = Errors.forCode(leaveResponse.errorCode());
            if (error == Errors.NONE) {
                log.debug("LeaveGroup request for group {} returned successfully", groupId);
                future.complete(null);
            } else {
                log.debug("LeaveGroup request for group {} failed with error: {}", groupId, error.message());
                future.raise(error);
            }
        }
    }

    // visible for testing
    //NOTE: 发送 HeartBeat 请求
    synchronized RequestFuture<Void> sendHeartbeatRequest() {
        HeartbeatRequest req = new HeartbeatRequest(this.groupId, this.generation.generationId, this.generation.memberId);
        return client.send(coordinator, ApiKeys.HEARTBEAT, req)
                .compose(new HeartbeatResponseHandler());
    }

    //NOTE: 处理 HeartBeat 请求的 response
    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatLatency.record(response.requestLatencyMs());
            Errors error = Errors.forCode(heartbeatResponse.errorCode());
            if (error == Errors.NONE) {
                log.debug("Received successful heartbeat response for group {}", groupId);
                future.complete(null);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP) {//NOTE: GroupCoordinator 是无效的
                log.debug("Attempt to heart beat failed for group {} since coordinator {} is either not started or not valid.",
                        groupId, coordinator());
                coordinatorDead();//NOTE: 把 Consumer 标记为 dead
                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {//NOTE: group 正在进行 rebalance
                log.debug("Attempt to heart beat failed for group {} since it is rebalancing.", groupId);
                requestRejoin();//NOTE: 需要重新进行 rejoin
                future.raise(Errors.REBALANCE_IN_PROGRESS);
            } else if (error == Errors.ILLEGAL_GENERATION) {//NOTE: generate id 是不合法的
                log.debug("Attempt to heart beat failed for group {} since generation id is not legal.", groupId);
                resetGeneration();//NOTE: 将 generate 设置为不存在,并标记 Consumer 为 Unjoined
                future.raise(Errors.ILLEGAL_GENERATION);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {//NOTE: member id 不合法
                log.debug("Attempt to heart beat failed for group {} since member id is not valid.", groupId);
                resetGeneration();//NOTE: 同上
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {//NOTE: 无权限
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {
        protected ClientResponse response;

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException)
                coordinatorDead();
            future.raise(e);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = (R) clientResponse.responseBody();
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

    }

    private class GroupCoordinatorMetrics {
        public final String metricGrpName;

        public final Sensor heartbeatLatency;
        public final Sensor joinLatency;
        public final Sensor syncLatency;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(metrics.metricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatLatency.add(metrics.metricName("heartbeat-rate",
                this.metricGrpName,
                "The average number of heartbeats per second"), new Rate(new Count()));

            this.joinLatency = metrics.sensor("join-latency");
            this.joinLatency.add(metrics.metricName("join-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-time-max",
                    this.metricGrpName,
                    "The max time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-rate",
                    this.metricGrpName,
                    "The number of group joins per second"), new Rate(new Count()));

            this.syncLatency = metrics.sensor("sync-latency");
            this.syncLatency.add(metrics.metricName("sync-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-time-max",
                    this.metricGrpName,
                    "The max time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-rate",
                    this.metricGrpName,
                    "The number of group syncs per second"), new Rate(new Count()));

            Measurable lastHeartbeat =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last controller heartbeat was sent"),
                lastHeartbeat);
        }
    }

    //NOTE: Consumer 客户端的心跳线程
    private class HeartbeatThread extends Thread {
        private boolean enabled = false;
        private boolean closed = false;
        private AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        HeartbeatThread() {
            super("kafka-coordinator-heartbeat-thread" + (groupId.isEmpty() ? "" : " | " + groupId));
            setDaemon(true);
        }

        public void enable() {
            synchronized (AbstractCoordinator.this) {
                this.enabled = true;
                heartbeat.resetTimeouts(time.milliseconds());
                AbstractCoordinator.this.notify();
            }
        }

        public void disable() {
            synchronized (AbstractCoordinator.this) {
                this.enabled = false;
            }
        }

        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                AbstractCoordinator.this.notify();
            }
        }

        private boolean hasFailed() {
            return failed.get() != null;
        }

        private RuntimeException failureCause() {
            return failed.get();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    synchronized (AbstractCoordinator.this) {
                        if (closed)
                            return;

                        if (!enabled) {//NOTE: 如果 enabled=false,那就进入线程等待
                            AbstractCoordinator.this.wait();
                            continue;
                        }

                        if (state != MemberState.STABLE) {//NOTE: Consumer 状态不是 stable,不允许发送心跳
                            // the group is not stable (perhaps because we left the group or because the coordinator
                            // kicked us out), so disable heartbeats and wait for the main thread to rejoin.
                            disable();
                            continue;
                        }

                        client.pollNoWakeup();
                        long now = time.milliseconds();

                        if (coordinatorUnknown()) {//NOTE: 如果 GroupCoordinator 未知
                            if (findCoordinatorFuture == null)
                                lookupCoordinator();//NOTE: find The GroupCoordinator
                            else
                                AbstractCoordinator.this.wait(retryBackoffMs);//NOTE: 等待 findCoordinatorFuture 的结果
                        } else if (heartbeat.sessionTimeoutExpired(now)) {//NOTE: 连接超时（session.time）,标记 Consumer 为 dead
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            coordinatorDead();//NOTE: 将对应的 groupCoordinator 赋值为 null,下次会重新建立连接
                        } else if (heartbeat.pollTimeoutExpired(now)) {//NOTE: rebalance 超时,标记为 Unjoined
                            // the poll timeout has expired, which means that the foreground thread has stalled
                            // in between calls to poll(), so we explicitly leave the group.
                            maybeLeaveGroup();//NOTE: 发送 leave-group 请求
                        } else if (!heartbeat.shouldHeartbeat(now)) {//NOTE: 定时未达到,wait 直到时间到达
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected
                            AbstractCoordinator.this.wait(retryBackoffMs);
                        } else {
                            heartbeat.sentHeartbeat(now);//NOTE: 更新上次心跳发送时间

                            //NOTE: 发送心跳信息
                            sendHeartbeatRequest().addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {//NOTE: 更新 heartbeat 的信息
                                    synchronized (AbstractCoordinator.this) {
                                        heartbeat.receiveHeartbeat(time.milliseconds());
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (AbstractCoordinator.this) {
                                        if (e instanceof RebalanceInProgressException) {//NOTE: group 正在进行 rebalance
                                            // it is valid to continue heartbeating while the group is rebalancing. This
                                            // ensures that the coordinator keeps the member in the group for as long
                                            // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                            // however, then the session timeout may expire before we can rejoin.
                                            //NOTE: 为了防止 Consumer 客户端依然在 group 内,可以认为这次心跳处理成功
                                            //NOTE: 在 rejoin 前,有可能导致时间超过 session.timeout
                                            heartbeat.receiveHeartbeat(time.milliseconds());
                                        } else {
                                            heartbeat.failHeartbeat();//NOTE: 标记为失败,经过 retryBackoffMs 后再次发送心跳

                                            // wake up the thread if it's sleeping to reschedule the heartbeat
                                            AbstractCoordinator.this.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (InterruptedException | InterruptException e) {
                Thread.interrupted();
                log.error("Unexpected interrupt received in heartbeat thread for group {}", groupId, e);
                this.failed.set(new RuntimeException(e));
            } catch (RuntimeException e) {
                log.error("Heartbeat thread for group {} failed due to unexpected error" , groupId, e);
                this.failed.set(e);
            }
        }

    }

    protected static class Generation {
        public static final Generation NO_GENERATION = new Generation(
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                null);

        public final int generationId;
        public final String memberId;
        public final String protocol;

        public Generation(int generationId, String memberId, String protocol) {
            this.generationId = generationId;
            this.memberId = memberId;
            this.protocol = protocol;
        }
    }

    private static class UnjoinedGroupException extends RetriableException {

    }

}
