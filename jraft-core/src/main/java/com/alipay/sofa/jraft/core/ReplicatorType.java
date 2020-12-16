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
package com.alipay.sofa.jraft.core;

/**
 * Follower：参与投票，可参与Leader选举（分配权重不为-1）；
 * Learner：不参与投票，不参与选举，其它与Follower 一致；
 *
 * 从 1.3.0 版本开始，
 * SOFAJRaft 引入了只读成员（学习者：Learner）支持，
 * 只读的节点类似 Follower，将从 Leader 复制日志并应用到本地状态机，
 * 但是不参与选举，复制成功也不被认为是多数派的一员。
 * 简而言之，除了复制日志以外，只读成员不参与其他任何 raft 算法过程。
 * 一般应用在为某个服务创建一个只读服务的时候，实现类似读写分离的效果，或者数据冷备等场景。
 *
 * 为一个 raft group 设置一个只读节点非常容易，任何以 /learner 为后缀的节点都将被认为是只读节点：
 *      // 3 节点 raft group 带一个只读节点
 *      Configuration conf = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083,localhost:8084/learner");
 *
 * 上面就创建了一个 raft 分组，
 * 其中普通成员是 localhost:8081,localhost:8082,localhost:8083，
 * 而 localhost:8084 就是一个 learner 只读节点，它带有 /learner 后缀。
 * 可以指定任意多个只读节点，但是由于日志复制都是从 leader 到 follower/learner，
 * 如果有大量学习者的话，可能 leader 的带宽会是一个问题，需要适当留意。
 *
 * Learner 节点的启动和其他 raft node 没有区别，同样可以有 StateMachine 和 Snapshot 机制。
 * 同时，只读节点也同样支持线性一致读 readIndex 调用。
 *
 * 除了静态配置之外，你还可以通过 CliService 动态地增加或者移除只读节点：
 *      // 增加只读节点
 *      Status addLearners(final String groupId, final Configuration conf, final List<PeerId> learners);
 *
 *      // 移除只读节点
 *      Status removeLearners(final String groupId, final Configuration conf, final List<PeerId> learners);
 *
 *      // 重新设置所有只读节点
 *      Status resetLearners(final String groupId, final Configuration conf, final List<PeerId> learners);
 *
 * Replicator role
 * @author boyan(boyan@antfin.com)
 *
 */
public enum ReplicatorType {
    Follower, Learner;

    public final boolean isFollower() {
        return this == Follower;
    }

    public final boolean isLearner() {
        return this == Learner;
    }
}