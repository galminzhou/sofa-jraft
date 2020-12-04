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
package com.alipay.sofa.jraft.rpc;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.util.Endpoint;

/**
 * RPC 模块用于节点之间的网络通讯:
 * 1. RPC Server: 内置于 Node 内的 RPC 服务器，接收其他节点或者客户端发过来的请求，转交给对应服务处理。
 * 2. RPC Client: 用于向其他节点发起请求，例如投票、复制日志、心跳等。
 *
 * 客户端的通讯层都依赖 Bolt 的 RpcClient，封装在 CliClientService 接口中，实现类就是 BoltCliClientService 。
 * 可以通过 BoltCliClientService 的 getRpcClient 方法获取底层的 bolt RpcClient 实例，用于其他通讯用途，做到资源复用。
 *
 * RouteTable 更新 leader 信息同样需要传入 CliClientService 实例，用户应该尽量复用这些底层通讯组件，而非重复创建用。
 *
 * @author jiachun.fjc
 */
public interface RpcClient extends Lifecycle<RpcOptions> {

    /**
     * Check connection for given address.
     *
     * @param endpoint target address
     * @return true if there is a connection and the connection is active and writable.
     */
    boolean checkConnection(final Endpoint endpoint);

    /**
     * Check connection for given address and async to create a new one if there is no connection.
     * @param endpoint       target address
     * @param createIfAbsent create a new one if there is no connection
     * @return true if there is a connection and the connection is active and writable.
     */
    boolean checkConnection(final Endpoint endpoint, final boolean createIfAbsent);

    /**
     * Close all connections of a address.
     *
     * @param endpoint target address
     */
    void closeConnection(final Endpoint endpoint);

    /**
     * Register a connect event listener for the replicator group.
     *
     * @param replicatorGroup replicator group
     */
    void registerConnectEventListener(final ReplicatorGroup replicatorGroup);

    /**
     * Synchronous invocation.
     *
     * @param endpoint  target address
     * @param request   request object
     * @param timeoutMs timeout millisecond
     * @return invoke result
     */
    default Object invokeSync(final Endpoint endpoint, final Object request, final long timeoutMs)
            throws InterruptedException, RemotingException {
        return invokeSync(endpoint, request, null, timeoutMs);
    }

    /**
     * Synchronous invocation using a invoke context.
     *
     * @param endpoint  target address
     * @param request   request object
     * @param ctx       invoke context
     * @param timeoutMs timeout millisecond
     * @return invoke result
     */
    Object invokeSync(final Endpoint endpoint, final Object request, final InvokeContext ctx,
                      final long timeoutMs) throws InterruptedException, RemotingException;

    /**
     * Asynchronous invocation with a callback.
     *
     * @param endpoint  target address
     * @param request   request object
     * @param callback  invoke callback
     * @param timeoutMs timeout millisecond
     */
    default void invokeAsync(final Endpoint endpoint, final Object request, final InvokeCallback callback,
                             final long timeoutMs) throws InterruptedException, RemotingException {
        invokeAsync(endpoint, request, null, callback, timeoutMs);
    }

    /**
     * Asynchronous invocation with a callback.
     *
     * @param endpoint  target address
     * @param request   request object
     * @param ctx       invoke context
     * @param callback  invoke callback
     * @param timeoutMs timeout millisecond
     */
    void invokeAsync(final Endpoint endpoint, final Object request, final InvokeContext ctx, final InvokeCallback callback,
                     final long timeoutMs) throws InterruptedException, RemotingException;
}
