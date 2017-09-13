/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * <p>
 * This class is not thread-safe!
 */
//NOTE: 对于异步请求/响应的网络的客户端
public class NetworkClient implements KafkaClient {

    private static final Logger log = LoggerFactory.getLogger(NetworkClient.class);

    /* the selector used to perform network i/o */
    private final Selectable selector;

    private final MetadataUpdater metadataUpdater;

    private final Random randOffset;

    /* the state of each node's connection */
    private final ClusterConnectionStates connectionStates;

    /* the set of requests currently being sent or awaiting a response */
    private final InFlightRequests inFlightRequests;

    /* the socket send buffer size in bytes */
    private final int socketSendBuffer;

    /* the socket receive size buffer in bytes */
    private final int socketReceiveBuffer;

    /* the client id used to identify this client in requests to the server */
    private final String clientId;

    /* the current correlation id to use when sending requests to servers */
    private int correlation;

    /* max time in ms for the producer to wait for acknowledgement from server*/
    private final int requestTimeoutMs; //note: 等待 server ack 的最大时间,默认是30s

    /* time in ms to wait before retrying to create connection to a server */
    private final long reconnectBackoffMs;

    private final Time time;

    /**
     * True if we should send an ApiVersionRequest when first connecting to a broker.
     */
    //note: 默认为 true
    private final boolean discoverBrokerVersions;

    private final Map<String, NodeApiVersions> nodeApiVersions = new HashMap<>();

    private final Set<String> nodesNeedingApiVersionsFetch = new HashSet<>();

    private final List<ClientResponse> abortedSends = new LinkedList<>();

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time,
                         boolean discoverBrokerVersions) {
        this(null, metadata, selector, clientId, maxInFlightRequestsPerConnection,
                reconnectBackoffMs, socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time, discoverBrokerVersions);
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time,
                         boolean discoverBrokerVersions) {
        this(metadataUpdater, null, selector, clientId, maxInFlightRequestsPerConnection, reconnectBackoffMs,
                socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time, discoverBrokerVersions);
    }

    private NetworkClient(MetadataUpdater metadataUpdater,
                          Metadata metadata,
                          Selectable selector,
                          String clientId,
                          int maxInFlightRequestsPerConnection,
                          long reconnectBackoffMs,
                          int socketSendBuffer,
                          int socketReceiveBuffer,
                          int requestTimeoutMs,
                          Time time,
                          boolean discoverBrokerVersions) {
        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         */
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);//note: 默认为 5
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.requestTimeoutMs = requestTimeoutMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.time = time;
        this.discoverBrokerVersions = discoverBrokerVersions;
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return True if we are ready to send to the given node
     */
    @Override
    //note: 如果 node 是已经连接的状态,就返回 true。
    public boolean ready(Node node, long now) {
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);

        if (isReady(node, now))
            return true;

        if (connectionStates.canConnect(node.idString(), now))
            // if we are interested in sending to a node and we don't have a connection to it, initiate one
            initiateConnect(node, now);

        return false;
    }

    /**
     * Closes the connection to a particular node (if there is one).
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        selector.close(nodeId);
        for (InFlightRequest request : inFlightRequests.clearAll(nodeId))
            if (request.isInternalRequest && request.header.apiKey() == ApiKeys.METADATA.id)
                metadataUpdater.handleDisconnection(request.destination);
        connectionStates.remove(nodeId);
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.connectionState(node.idString()).equals(ConnectionState.DISCONNECTED);
    }

    /**
     * Check if the node with the given id is ready to send more requests.
     *
     * @param node The node
     * @param now The current time in ms
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(Node node, long now) {
        // if we need to update our metadata now declare all requests unready to make metadata requests first
        // priority
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString());
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     *
     * @param node The node
     */
    /**
     * 是否可以向给定的 node 发送请求（状态是对否就绪）,从以下三个部分判断:
     * 1. connectionStates.isReady(node),连接状态是 READY;
     * 2. selector.isChannelReady(node),channel 是 ready 状态（channel 已经握手并且 channel 权限认证通过）;
     * 3. inFlightRequests.canSendMore(node), 这个 node 是否可以发送更多的请求
     */
    private boolean canSendRequest(String node) {
        return connectionStates.isReady(node) && selector.isChannelReady(node) && inFlightRequests.canSendMore(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     * @param request The request
     * @param now The current timestamp
     */
    //note: 不需要处理 Server 端的请求
    @Override
    public void send(ClientRequest request, long now) {
        doSend(request, false, now);
    }

    //note: 需要处理 Server 端 response 的请求
    private void sendInternalMetadataRequest(MetadataRequest.Builder builder,
                                             String nodeConnectionId, long now) {
        ClientRequest clientRequest = newClientRequest(nodeConnectionId, builder, now, true);//note: 缓存将要发送的 metadata 请求
        doSend(clientRequest, true, now);
    }

    //note: 发送请求
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
        String nodeId = clientRequest.destination();
        if (!isInternalRequest) {
            // If this request came from outside the NetworkClient, validate
            // that we can send data.  If the request is internal, we trust
            // that that internal code has done this validation.  Validation
            // will be slightly different for some internal requests (for
            // example, ApiVersionsRequests can be sent prior to being in
            // READY state.)
            if (!canSendRequest(nodeId))
                throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }
        AbstractRequest request = null;
        AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
        //note: 构建 AbstractRequest, 检查其版本信息
        try {
            NodeApiVersions versionInfo = nodeApiVersions.get(nodeId);
            // Note: if versionInfo is null, we have no server version information. This would be
            // the case when sending the initial ApiVersionRequest which fetches the version
            // information itself.  It is also the case when discoverBrokerVersions is set to false.
            if (versionInfo == null) {
                if (discoverBrokerVersions && log.isTraceEnabled())
                    log.trace("No version information found when sending message of type {} to node {}. " +
                            "Assuming version {}.", clientRequest.apiKey(), nodeId, builder.version());
            } else {
                short version = versionInfo.usableVersion(clientRequest.apiKey());
                builder.setVersion(version);
            }
            // The call to build may also throw UnsupportedVersionException, if there are essential
            // fields that cannot be represented in the chosen version.
            request = builder.build();//note: 当为 Produce 请求时,转化为 ProduceRequest,Metadata 请求时,转化为 Metadata 请求
        } catch (UnsupportedVersionException e) {
            // If the version is not supported, skip sending the request over the wire.
            // Instead, simply add it to the local queue of aborted requests.
            log.debug("Version mismatch when attempting to send {} to {}",
                    clientRequest.toString(), clientRequest.destination(), e);
            ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(),
                    clientRequest.callback(), clientRequest.destination(), now, now,
                    false, e, null);
            abortedSends.add(clientResponse);
            return;
        }
        RequestHeader header = clientRequest.makeHeader();
        if (log.isDebugEnabled()) {
            int latestClientVersion = ProtoUtils.latestVersion(clientRequest.apiKey().id);
            if (header.apiVersion() == latestClientVersion) {
                log.trace("Sending {} to node {}.", request, nodeId);
            } else {
                log.debug("Using older server API v{} to send {} to node {}.",
                    header.apiVersion(), request, nodeId);
            }
        }
        //note: Send是一个接口，这里返回的是 NetworkSend，而 NetworkSend 继承 ByteBufferSend
        Send send = request.toSend(nodeId, header);
        InFlightRequest inFlightRequest = new InFlightRequest(
                header,
                clientRequest.createdTimeMs(),
                clientRequest.destination(),
                clientRequest.callback(),
                clientRequest.expectResponse(),
                isInternalRequest,
                send,
                now);
        this.inFlightRequests.add(inFlightRequest);
        //note: 将 send 和对应 kafkaChannel 绑定起来，并开启该 kafkaChannel 底层 socket 的写事件
        selector.send(inFlightRequest.send);
    }

    /**
     * Do actual reads and writes to sockets.
     *
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        //note: Step1 判断是否需要更新 meta,如果需要就更新（请求更新 metadata 的地方）
        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        //note: Step2 调用 Selector.poll() 进行 socket 相关的 IO 操作
        try {
            this.selector.poll(Utils.min(timeout, metadataTimeout, requestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        // process completed actions
        //note: Step3 处理完成后的操作
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        handleAbortedSends(responses);
        //note: 处理已经完成的 send（不需要 response 的 request,如 send）
        handleCompletedSends(responses, updatedNow);//note: 通过 selector 中获取 Server 端的 response
        //note: 处理从 server 端接收到 Receive（如 Metadata 请求）
        handleCompletedReceives(responses, updatedNow);//note: 在返回的 handler 中，会处理 metadata 的更新
        //note: 处理连接失败那些连接,重新请求 meta
        handleDisconnections(responses, updatedNow);
        //note: 处理新建立的那些连接（还不能发送请求,比如:还未认证）
        handleConnections();
        handleInitiateApiVersionRequests(updatedNow);
        handleTimedOutRequests(responses, updatedNow);

        // invoke callbacks
        for (ClientResponse response : responses) {
            try {
                response.onComplete();
            } catch (Exception e) {
                log.error("Uncaught error in request completion:", e);
            }
        }

        return responses;
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.inFlightRequestCount();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.inFlightRequestCount(node);
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        this.selector.close();
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period.
     *
     * @return The node with the fewest in-flight requests.
     */
    @Override
    //note: 选择一个有最少连接的节点
    public Node leastLoadedNode(long now) {
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        int inflight = Integer.MAX_VALUE;
        Node found = null;

        int offset = this.randOffset.nextInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            int idx = (offset + i) % nodes.size();//note: 从中随机选择一个节点
            Node node = nodes.get(idx);
            int currInflight = this.inFlightRequests.inFlightRequestCount(node.idString());
            if (currInflight == 0 && this.connectionStates.isReady(node.idString())) {
                // NOTE：如果这个节点 is ready,并且没有 in-flight requests。
                // if we find an established connection with no in-flight requests we can stop right away
                log.trace("Found least loaded node {} connected with no in-flight requests", node);
                return node;
            } else if (!this.connectionStates.isBlackedOut(node.idString(), now) && currInflight < inflight) {
                // otherwise if this is the best we have found so far, record that
                //note:遍历整个集合找一个连接最小的节点
                inflight = currInflight;
                found = node;
            } else if (log.isTraceEnabled()) {
                log.trace("Removing node {} from least loaded node selection: is-blacked-out: {}, in-flight-requests: {}",
                        node, this.connectionStates.isBlackedOut(node.idString(), now), currInflight);
            }
        }

        if (found != null)
            log.trace("Found least loaded node {}", found);
        else
            log.trace("Least loaded node selection failed to find an available node");

        return found;
    }

    public static AbstractResponse parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer);//note: 解析 buffer 内容
        // Always expect the response version id to be the same as the request version id
        short apiKey = requestHeader.apiKey();
        short apiVer = requestHeader.apiVersion();
        Struct responseBody = ProtoUtils.responseSchema(apiKey, apiVer).read(responseBuffer);
        correlate(requestHeader, responseHeader);
        return AbstractResponse.getResponse(apiKey, responseBody);
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     */
    private void processDisconnection(List<ClientResponse> responses, String nodeId, long now) {
        connectionStates.disconnected(nodeId, now);
        nodeApiVersions.remove(nodeId);
        nodesNeedingApiVersionsFetch.remove(nodeId);
        for (InFlightRequest request : this.inFlightRequests.clearAll(nodeId)) {
            log.trace("Cancelled request {} due to node {} being disconnected", request, nodeId);
            if (request.isInternalRequest && request.header.apiKey() == ApiKeys.METADATA.id)
                metadataUpdater.handleDisconnection(request.destination);
            else
                responses.add(request.disconnected(now));
        }
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        List<String> nodeIds = this.inFlightRequests.getNodesWithTimedOutRequests(now, this.requestTimeoutMs);
        for (String nodeId : nodeIds) {
            // close connection to the node
            this.selector.close(nodeId);
            log.debug("Disconnecting from node {} due to request timeout.", nodeId);
            processDisconnection(responses, nodeId, now);
        }

        // we disconnected, so we should probably refresh our metadata
        if (!nodeIds.isEmpty())
            metadataUpdater.requestUpdate();
    }

    private void handleAbortedSends(List<ClientResponse> responses) {
        responses.addAll(abortedSends);
        abortedSends.clear();
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    //note: 处理已经完成的发送请求,特别是如果没有响应,那么证明请求已经完成
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        for (Send send : this.selector.completedSends()) {
            InFlightRequest request = this.inFlightRequests.lastSent(send.destination());
            if (!request.expectResponse) {//note: 不期望接收 server 端 response 的请求
                this.inFlightRequests.completeLastSent(send.destination());
                responses.add(request.completed(null, now));//note: 将 server 端的 response 添加到响应列表中
            }
        }
    }

    /**
     * Handle any completed receives and update the response list with the responses received.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    //note: 处理任何已经完成的接收响应
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        for (NetworkReceive receive : this.selector.completedReceives()) {
            String source = receive.source();
            InFlightRequest req = inFlightRequests.completeNext(source);
            AbstractResponse body = parseResponse(receive.payload(), req.header);
            log.trace("Completed receive from node {}, for key {}, received {}", req.destination, req.header.apiKey(), body);
            if (req.isInternalRequest && body instanceof MetadataResponse)//note: 如果是 meta 响应
                metadataUpdater.handleCompletedMetadataResponse(req.header, now, (MetadataResponse) body);
            else if (req.isInternalRequest && body instanceof ApiVersionsResponse)
                handleApiVersionsResponse(responses, req, now, (ApiVersionsResponse) body); //note: 如果是其他响应
            else
                responses.add(req.completed(body, now));
        }
    }

    private void handleApiVersionsResponse(List<ClientResponse> responses,
                                           InFlightRequest req, long now, ApiVersionsResponse apiVersionsResponse) {
        final String node = req.destination;
        if (apiVersionsResponse.errorCode() != Errors.NONE.code()) {
            log.warn("Node {} got error {} when making an ApiVersionsRequest.  Disconnecting.",
                    node, Errors.forCode(apiVersionsResponse.errorCode()));
            this.selector.close(node);
            processDisconnection(responses, node, now);
            return;
        }
        NodeApiVersions nodeVersionInfo = new NodeApiVersions(apiVersionsResponse.apiVersions());
        nodeApiVersions.put(node, nodeVersionInfo);
        this.connectionStates.ready(node);
        if (log.isDebugEnabled()) {
            log.debug("Recorded API versions for node {}: {}", node, nodeVersionInfo);
        }
    }

    /**
     * Handle any disconnected connections
     *
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    //note: 处理连接失败那些连接
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        for (String node : this.selector.disconnected()) {
            log.debug("Node {} disconnected.", node);
            processDisconnection(responses, node, now);
        }
        // we got a disconnect so we should probably refresh our metadata and see if that broker is dead
        if (this.selector.disconnected().size() > 0)
            metadataUpdater.requestUpdate();
    }

    /**
     * Record any newly completed connections
     */
    //note: 处理新建立的连接
    private void handleConnections() {
        for (String node : this.selector.connected()) {
            // We are now connected.  Node that we might not still be able to send requests. For instance,
            // if SSL is enabled, the SSL handshake happens after the connection is established.
            // Therefore, it is still necessary to check isChannelReady before attempting to send on this
            // connection.
            if (discoverBrokerVersions) {
                this.connectionStates.checkingApiVersions(node);
                nodesNeedingApiVersionsFetch.add(node);
                log.debug("Completed connection to node {}.  Fetching API versions.", node);
            } else {
                this.connectionStates.ready(node);
                log.debug("Completed connection to node {}.  Ready.", node);
            }
        }
    }

    private void handleInitiateApiVersionRequests(long now) {
        Iterator<String> iter = nodesNeedingApiVersionsFetch.iterator();
        while (iter.hasNext()) {
            String node = iter.next();
            if (selector.isChannelReady(node) && inFlightRequests.canSendMore(node)) {
                log.debug("Initiating API versions fetch from node {}.", node);
                ApiVersionsRequest.Builder apiVersionRequest = new ApiVersionsRequest.Builder();
                ClientRequest clientRequest = newClientRequest(node, apiVersionRequest, now, true);
                doSend(clientRequest, true, now);
                iter.remove();
            }
        }
    }

    /**
     * Validate that the response corresponds to the request we expect or else explode
     */
    private static void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId())
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                    + ") does not match request (" + requestHeader.correlationId() + "), request header: " + requestHeader);
    }

    /**
     * Initiate a connection to the given node
     */
    //note: 调用 selector.connect() 初始化一个连接
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            log.debug("Initiating connection to node {} at {}:{}.", node.id(), node.host(), node.port());
            this.connectionStates.connecting(nodeConnectionId, now);//note:状态变为正在连接
            selector.connect(nodeConnectionId,
                             new InetSocketAddress(node.host(), node.port()),
                             this.socketSendBuffer,
                             this.socketReceiveBuffer);//note:建立一个连接
        } catch (IOException e) {
            /* attempt failed, we'll try again after the backoff */
            connectionStates.disconnected(nodeConnectionId, now);
            /* maybe the problem is our metadata, update it */
            metadataUpdater.requestUpdate();//note: 请求更新 meta 信息
            log.debug("Error connecting to node {} at {}:{}:", node.id(), node.host(), node.port(), e);
        }
    }

    //note: 默认 MetadataUpdater 的实现
    class DefaultMetadataUpdater implements MetadataUpdater {

        /* the current cluster metadata */
        private final Metadata metadata;

        /* true iff there is a metadata request that has been sent and for which we have not yet received a response */
        private boolean metadataFetchInProgress;//note: metadata fetch 请求已经发送,但还未收到响应

        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.metadataFetchInProgress = false;
        }

        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        @Override
        public boolean isUpdateDue(long now) {
            return !this.metadataFetchInProgress && this.metadata.timeToNextUpdate(now) == 0;
        }

        @Override
        public long maybeUpdate(long now) {
            // should we update our metadata?
            //note: metadata 是否应该更新
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);//note: metadata 下次更新的时间
            //note: 如果一条 metadata 的 fetch 请求还未从 server 收到回复,那么时间设置为 waitForMetadataFetch（默认30s）
            long waitForMetadataFetch = this.metadataFetchInProgress ? requestTimeoutMs : 0;

            long metadataTimeout = Math.max(timeToNextMetadataUpdate, waitForMetadataFetch);
            if (metadataTimeout > 0) {//note: 时间未到时,直接返回下次应该更新的时间
                return metadataTimeout;
            }

            // Beware that the behavior of this method and the computation of timeouts for poll() are
            // highly dependent on the behavior of leastLoadedNode.
            Node node = leastLoadedNode(now);//note:选择一个连接最小的节点
            if (node == null) {
                log.debug("Give up sending metadata request since no node is available");
                return reconnectBackoffMs;
            }

            return maybeUpdate(now, node); //note: 可以发送 metadata 请求的话
        }

        @Override
        public void handleDisconnection(String destination) {
            Cluster cluster = metadata.fetch();
            if (cluster.isBootstrapConfigured()) {
                int nodeId = Integer.parseInt(destination);
                Node node = cluster.nodeById(nodeId);
                if (node != null)
                    log.warn("Bootstrap broker {}:{} disconnected", node.host(), node.port());
            }

            metadataFetchInProgress = false;
        }

        @Override
        //note: 处理 Server 端对 Metadata 请求处理后的 response
        public void handleCompletedMetadataResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
            this.metadataFetchInProgress = false;
            Cluster cluster = response.cluster();
            // check if any topics metadata failed to get updated
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty())
                log.warn("Error while fetching metadata with correlation id {} : {}", requestHeader.correlationId(), errors);

            // don't update the cluster if there are no valid nodes...the topic we want may still be in the process of being
            // created which means we will get errors and no nodes until it exists
            if (cluster.nodes().size() > 0) {
                this.metadata.update(cluster, now);//note: 更新 meta 信息
            } else {//note: 如果 metadata 中 node 信息无效,则不更新 metadata 信息
                log.trace("Ignoring empty metadata response with correlation id {}.", requestHeader.correlationId());
                this.metadata.failedUpdate(now);
            }
        }

        @Override
        //note: 请求更新 metadata
        public void requestUpdate() {
            this.metadata.requestUpdate();
        }

        /**
         * Return true if there's at least one connection establishment is currently underway
         */
        //note:
        private boolean isAnyNodeConnecting() {
            for (Node node : fetchNodes()) {
                if (connectionStates.isConnecting(node.idString())) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Add a metadata request to the list of sends if we can make one
         */
        //note: 判断是否可以发送请求,可以的话将 metadata 请求加入到发送列表中
        private long maybeUpdate(long now, Node node) {
            String nodeConnectionId = node.idString();

            if (canSendRequest(nodeConnectionId)) {//note: 通道已经 ready 并且支持发送更多的请求
                this.metadataFetchInProgress = true; //note: 准备开始发送数据,将 metadataFetchInProgress 置为 true
                MetadataRequest.Builder metadataRequest; //note: 创建 metadata 请求
                if (metadata.needMetadataForAllTopics())//note: 强制更新所有 topic 的 metadata
                    metadataRequest = MetadataRequest.Builder.allTopics();
                else //note: 只更新 metadata 中的 topics 列表（列表中的 topics 由 metadata.add() 得到）
                    metadataRequest = new MetadataRequest.Builder(new ArrayList<>(metadata.topics()));


                log.debug("Sending metadata request {} to node {}", metadataRequest, node.id());
                sendInternalMetadataRequest(metadataRequest, nodeConnectionId, now);//note: 发送 metadata 请求
                return requestTimeoutMs;
            }

            // If there's any connection establishment underway, wait until it completes. This prevents
            // the client from unnecessarily connecting to additional nodes while a previous connection
            // attempt has not been completed.
            if (isAnyNodeConnecting()) {//note: 如果正在与任何一个 node 的连接状态是 connecting,那么就进行等待
                // Strictly the timeout we should return here is "connect timeout", but as we don't
                // have such application level configuration, using reconnect backoff instead.
                return reconnectBackoffMs;
            }

            if (connectionStates.canConnect(nodeConnectionId, now)) {//note: 如果没有连接这个 node,那就初始化连接
                // we don't have a connection to this node right now, make one
                log.debug("Initialize connection to node {} for sending metadata request", node.id());
                initiateConnect(node, now);//note: 初始化连接
                return reconnectBackoffMs;
            }

            // connected, but can't send more OR connecting
            // In either case, we just need to wait for a network event to let us know the selected
            // connection might be usable again.
            return Long.MAX_VALUE;
        }

    }

    @Override
    public ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs,
                                          boolean expectResponse) {
        return newClientRequest(nodeId, requestBuilder, createdTimeMs, expectResponse, null);
    }

    @Override
    public ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs,
                                          boolean expectResponse, RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, correlation++, clientId, createdTimeMs, expectResponse,
                callback);
    }

    static class InFlightRequest {
        final RequestHeader header;
        final String destination;
        final RequestCompletionHandler callback;
        final boolean expectResponse;
        final boolean isInternalRequest; // used to flag requests which are initiated internally by NetworkClient
        final Send send;
        final long sendTimeMs;
        final long createdTimeMs;

        public InFlightRequest(RequestHeader header,
                               long createdTimeMs,
                               String destination,
                               RequestCompletionHandler callback,
                               boolean expectResponse,
                               boolean isInternalRequest,
                               Send send,
                               long sendTimeMs) {
            this.header = header;
            this.destination = destination;
            this.callback = callback;
            this.expectResponse = expectResponse;
            this.isInternalRequest = isInternalRequest;
            this.send = send;
            this.sendTimeMs = sendTimeMs;
            this.createdTimeMs = createdTimeMs;
        }

        public ClientResponse completed(AbstractResponse response, long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs, false, null, response);
        }

        public ClientResponse disconnected(long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs, true, null, null);
        }
    }

    public boolean discoverBrokerVersions() {
        return discoverBrokerVersions;
    }
}
