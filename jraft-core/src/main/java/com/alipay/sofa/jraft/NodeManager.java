/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.ConcurrentHashSet;

/**
 * 单纯一个 raft node 是没有什么用，测试可以是单个节点，
 * 但是正常情况下一个 raft grup 至少应该是三个节点，如果考虑到异地多机房容灾，应该扩展到5个节点。（7 ～ 11）
 *
 * 节点之间的通讯使用 bolt 框架的 RPC 服务。
 *
 * 首先，创建节点后，需要将节点地址加入到 NodeManager:
 *      NodeManager.getInstance().addAddress(serverId.getEndpoint());
 *
 * NodeManager 的 address 集合表示本进程提供的 RPC 服务地址列表。
 * 其次，创建 Raft 专用的 RPCServer，内部内置了一套处理内部节点之间交互协议的 processor：
 *      RPCServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndPoint());
 *      // 启动 RPC 服务
 *      rpcServer.init(null);
 *
 * 上述创建和 start 两个步骤可以合并为一个调用：
 *      RPCServer rpcServer = RaftRpcServerFactory.createAndStartRaftRpcServer(serverId.getEndPoint());
 * 这样就是为了本节点提供 RPC Server 服务，其他节点可以连接本节点进行通讯，比如发起选举、心跳和复制等。
 *
 * 但是大部分应用的服务端也会同时提供 RPC 服务给用户使用，
 * jraft 允许 raft 节点使用业务提供的 RPCServer 对象，
 * 也就是和业务共用同一个服务端口，这就需要为业务的 RPCServer 注册 raft 特有的通讯协议处理器：
 *      RpcServer rpcServer = ... // 业务的 RPCServer 对象
 *      ...注册业务的处理器...
 *      // 注册 Raft 内部协议处理器
 *      RaftRpcServerFactory.addRaftRequestProcessors(rpcServer);
 *      // 启动，共用了端口
 *      rpcServer.init(null);
 *
 * 同样，应用服务器节点之间可能需要一些业务通讯，会使用到 bolt 的 RpcClient，你也可以直接使用 jraft 内部的 rpcClient:
 *      RpcClient rpcClient = ((AbstractBoltClientService) (((NodeImpl) node).getRpcService())).getRpcClient();
 *
 * 这样可以做到一些资源复用，减少消耗，代价就是依赖了 jraft 的内部实现和缺少一些可自定义配置。
 *
 * 节点配置变更可以通过 CliService，也可以通过 Leader 节点 Node 的系列方法来变更，实质上 CliService 都是转发到 leader 节点执行。
 *
 * Raft nodes manager.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-22 5:58:23 PM
 */
public class NodeManager {

    private static final NodeManager                INSTANCE = new NodeManager();

    private final ConcurrentMap<NodeId, Node>       nodeMap  = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<Node>> groupMap = new ConcurrentHashMap<>();
    private final ConcurrentHashSet<Endpoint>       addrSet  = new ConcurrentHashSet<>();

    public static NodeManager getInstance() {
        return INSTANCE;
    }

    /**
     * Return true when RPC service is registered.
     */
    public boolean serverExists(final Endpoint addr) {
        if (addr.getIp().equals(Utils.IP_ANY)) {
            return this.addrSet.contains(new Endpoint(Utils.IP_ANY, addr.getPort()));
        }
        return this.addrSet.contains(addr);
    }

    /**
     * Remove a RPC service address.
     */
    public boolean removeAddress(final Endpoint addr) {
        return this.addrSet.remove(addr);
    }

    /**
     * Adds a RPC service address.
     */
    public void addAddress(final Endpoint addr) {
        this.addrSet.add(addr);
    }

    /**
     * Adds a node.
     */
    public boolean add(final Node node) {
        // check address ok?
        if (!serverExists(node.getNodeId().getPeerId().getEndpoint())) {
            return false;
        }
        final NodeId nodeId = node.getNodeId();
        if (this.nodeMap.putIfAbsent(nodeId, node) == null) {
            final String groupId = node.getGroupId();
            List<Node> nodes = this.groupMap.get(groupId);
            if (nodes == null) {
                nodes = Collections.synchronizedList(new ArrayList<>());
                List<Node> existsNode = this.groupMap.putIfAbsent(groupId, nodes);
                if (existsNode != null) {
                    nodes = existsNode;
                }
            }
            nodes.add(node);
            return true;
        }
        return false;
    }

    /**
     * Clear the states, for test
     */
    @OnlyForTest
    public void clear() {
        this.groupMap.clear();
        this.nodeMap.clear();
        this.addrSet.clear();
    }

    /**
     * Remove a node.
     */
    public boolean remove(final Node node) {
        if (this.nodeMap.remove(node.getNodeId(), node)) {
            final List<Node> nodes = this.groupMap.get(node.getGroupId());
            if (nodes != null) {
                return nodes.remove(node);
            }
        }
        return false;
    }

    /**
     * Get node by groupId and peer.
     */
    public Node get(final String groupId, final PeerId peerId) {
        return this.nodeMap.get(new NodeId(groupId, peerId));
    }

    /**
     * Get all nodes in a raft group.
     */
    public List<Node> getNodesByGroupId(final String groupId) {
        return this.groupMap.get(groupId);
    }

    /**
     * Get all nodes
     */
    public List<Node> getAllNodes() {
        return this.groupMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    private NodeManager() {
    }
}
