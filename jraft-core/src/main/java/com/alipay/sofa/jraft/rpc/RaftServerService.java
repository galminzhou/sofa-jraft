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

import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.google.protobuf.Message;

/**
 * Raft RPC service in server.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 4:25:09 PM
 */
public interface RaftServerService {

    /**
     * 处理预选举投票（pre-vote）请求
     * Handle pre-vote request.
     *
     * @param request   data of the pre vote
     * @return the response message
     */
    Message handlePreVoteRequest(RequestVoteRequest request);

    /**
     * 处理选举投票（rquest-vote）请求
     * Handle request-vote request.
     *
     * @param request   data of the vote
     * @return the response message
     */
    Message handleRequestVoteRequest(RequestVoteRequest request);

    /**
     * 处理复制日志（append-entries）请求，通过响应返回响应消息或调用done.run()
     * Handle append-entries request, return response message or
     * called done.run() with response.
     *
     * @param request   data of the entries to append
     * @param done      callback
     * @return the response message
     */
    Message handleAppendEntriesRequest(AppendEntriesRequest request, RpcRequestClosure done);

    /**
     * 处理安装快照（install-snapshot），通过响应返回响应消息或调用done.run()
     * Handle install-snapshot request, return response message or
     * called done.run() with response.
     *
     * @param request   data of the install snapshot request
     * @param done      callback
     * @return the response message
     */
    Message handleInstallSnapshot(InstallSnapshotRequest request, RpcRequestClosure done);

    /**
     * TODO 处理time-out-now 请求，通过响应返回 响应消息或调用done.run()
     * Handle time-out-now request, return response message or
     * called done.run() with response.
     *
     * @param request   data of the timeout now request
     * @param done      callback
     * @return the response message
     */
    Message handleTimeoutNowRequest(TimeoutNowRequest request, RpcRequestClosure done);

    /**
     * 处理线性一致性读（read-index）请求
     * Handle read-index request, call the RPC closure with response.
     *
     * @param request   data of the readIndex read
     * @param done      callback
     */
    void handleReadIndexRequest(ReadIndexRequest request, RpcResponseClosure<ReadIndexResponse> done);
}
