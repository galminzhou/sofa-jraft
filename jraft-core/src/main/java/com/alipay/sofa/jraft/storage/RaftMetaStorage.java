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
package com.alipay.sofa.jraft.storage;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;

/**
 * Meta Storage 是用来存储记录 Raft 实现的内部状态，比如当前 Term 、投票给哪个节点等信息；
 *
 * JRaft 提供了 LocalRaftMetaStorage 实现类，基于本地文件系统采用 protobuf 协议对元数据执行序列化之后进行存储。
 *
 * RaftMetaStorage 元信息存储实现，定义 Raft 元数据的 Metadata 存储模块核心 API 接口包括：
 *      1) 设置/获取 Raft 元数据的当前任期 Term；
 *      2) 分配/查询 Raft 元信息的 PeerId 节点投票。
 *
 * Raft 内部状态任期 Term 是在整个 Raft Group 里单调递增的 long 数字，用来表示一轮投票的编号，
 * 其中成功选举出来的 Leader 对应的 Term 称为 Leader Term，Leader 没有发生变更期间提交的日志都有相同的 Term 编号。
 * PeerId 表示 Raft 协议的参与者(Leader/Follower/Candidate etc.)，
 * 由三元素组成： ip:port:index，其中 ip 是节点的 IP， port 是端口， index 表示同一个端口的序列号(暂不使用)。
 *
 * Raft metadata storage service.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:54:21 PM
 */
public interface RaftMetaStorage extends Lifecycle<RaftMetaStorageOptions>, Storage {

    /**
     * Set current term.
     */
    boolean setTerm(final long term);

    /**
     * Get current term.
     */
    long getTerm();

    /**
     * Set voted for information.
     */
    boolean setVotedFor(final PeerId peerId);

    /**
     * Get voted for information.
     */
    PeerId getVotedFor();

    /**
     * Set term and voted for information.
     */
    boolean setTermAndVotedFor(final long term, final PeerId peerId);
}
