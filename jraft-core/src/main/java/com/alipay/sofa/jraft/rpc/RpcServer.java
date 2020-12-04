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
import com.alipay.sofa.jraft.rpc.impl.ConnectionClosedEventListener;

/**
 * RPC 模块用于节点之间的网络通讯:
 * 1. RPC Server: 内置于 Node 内的 RPC 服务器，接收其他节点或者客户端发过来的请求，转交给对应服务处理。
 * 2. RPC Client: 用于向其他节点发起请求，例如投票、复制日志、心跳等。
 * @author jiachun.fjc
 */
public interface RpcServer extends Lifecycle<Void> {

    /**
     * Register a conn closed event listener.
     *
     * @param listener the event listener.
     */
    void registerConnectionClosedEventListener(final ConnectionClosedEventListener listener);

    /**
     * Register user processor.
     *
     * @param processor the user processor which has a interest
     */
    void registerProcessor(final RpcProcessor<?> processor);

    /**
     *
     * @return bound port
     */
    int boundPort();
}
