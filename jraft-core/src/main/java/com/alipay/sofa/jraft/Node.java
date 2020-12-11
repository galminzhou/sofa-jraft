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

import java.util.List;

import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.core.Replicator;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.entity.UserLog;
import com.alipay.sofa.jraft.error.LogIndexOutOfBoundsException;
import com.alipay.sofa.jraft.error.LogNotFoundException;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.util.Describer;

/**
 -----------------JRaft Node Design ------------------------------------
 * Node
 *      Raft 分组中的一个节点，连接封装底层的所有服务，用户看到的主要服务接口，
 *      特别是 apply(task) 用于向 raft group 组成的复制状态机集群提交新任务应用到业务状态机
 * 存储:
 *      Log 存储，记录 raft 用户提交任务的日志，将日志从 leader 复制到其他节点上。
 *          LogStorage 是存储实现，默认实现基于 RocksDB 存储，你也可以很容易扩展自己的日志存储实现
 *          LogManager 负责对底层存储的调用，对调用做缓存、批量提交、必要的检查和优化
 *      Metadata 存储，元信息存储，记录 raft 实现的内部状态，比如当前 term、投票给哪个节点等信息
 *      Snapshot 存储，用于存放用户的状态机 snapshot 及元信息，可选。
 *          SnapshotStorage 用于 snapshot 存储实现。
 *          SnapshotExecutor 用于 snapshot 实际存储、远程安装、复制的管理
 * 状态机
 *      StateMachine：用户核心逻辑的实现，核心是 onApply(Iterator) 方法, 应用通过 Node#apply(task) 提交的日志到业务状态机
 *      FSMCaller:封装对业务 StateMachine 的状态转换的调用以及日志的写入等,一个有限状态机的实现,做必要的检查、请求合并提交和并发处理等
 * 复制
 *      Replicator：用于 leader 向 followers 复制日志，也就是 raft 中的 AppendEntries 调用，包括心跳存活检查等
 *      ReplicatorGroup：用于单个 raft group 管理所有的 replicator，必要的权限检查和派发
 * RPC：RPC 模块用于节点之间的网络通讯
 *      RPC Server：内置于 Node 内的 RPC 服务器，接收其他节点或者客户端发过来的请求，转交给对应服务处理
 *      RPC Client：用于向其他节点发起请求，例如投票、复制日志、心跳等
 * KV Store：KV Store 是各种 Raft 实现的一个典型应用场景，JRaft 中包含了一个嵌入式的分布式 KV 存储实现（JRaft-RheaKV）。
 -----------------JRaft Node Design ------------------------------------
 *
 *
 * Raft 节点 Node ： Node 接口表示一个 raft 的参与节点，
 * 可以提交 task，以及查询 raft group 信息，比如当前状态、当前 leader/term 等；
 * 他的角色可能是 leader、follower 或者 candidate，随着选举过程而转变，（1.3之后加入Learner节点，不参与任何投票）。
 *
 * 创建一个 raft 节点可以通过 RaftServiceFactory.createRaftNode(String groupId, PeerId serverId) 静态方法，其中
 * - groupId 该 raft 节点的 raft group Id。
 * - serverId 该 raft 节点的 PeerId 。
 *
 * 创建后还需要初始化才可以使用，初始化调用 boolean init(NodeOptions opts) 方法，需要传入 NodeOptions 配置。
 *
 * NodeOptions 最重要的就是设置三个存储的路径，以及应用状态机实例，如果是第一次启动，还需要设置 initialConf 初始配置节点列表。
 * 初始化创建的 Node:
 *      NodeOptions opts = ...
 *      Node node = RaftServiceFactory.createRaftNode(groupId, serverId);
 *      if(!node.init(opts))
 *          throw new IllegalStateException("启动 raft 节点失败，具体错误信息请参考日志。");
 *
 * 创建和初始化结合起来也可以直接用 createAndInitRaftNode 方法：
 *      Node node = RaftServiceFactory.createAndInitRaftNode(groupId, serverId, nodeOpts);
 *
 *
 *
 *
 * A raft replica node.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 4:06:55 PM
 */
public interface Node extends Lifecycle<NodeOptions>, Describer {

    /**
     * 获取当前 raft group 的 leader peerId，如果未知，返回 null。
     * Get the leader peer id for redirect, null if absent.
     */
    PeerId getLeaderId();

    /**
     * Get current node id.
     */
    NodeId getNodeId();

    /**
     * Get the node metrics, only valid when node option {@link NodeOptions#isEnableMetrics()} is true.
     */
    NodeMetrics getNodeMetrics();

    /**
     * Get the raft group id.
     */
    String getGroupId();

    /**
     * Get the node options.
     */
    NodeOptions getOptions();

    /**
     * Get the raft options
     */
    RaftOptions getRaftOptions();

    /**
     * Returns true when the node is leader.
     */
    boolean isLeader();

    /**
     * Returns true when the node is leader.
     * @param blocking if true, will be blocked until the node finish it's state change
     */
    boolean isLeader(final boolean blocking);

    /**
     * 停止一个 raft 节点
     * Shutdown local replica node.
     *
     * @param done callback
     */
    void shutdown(final Closure done);

    /**
     * 在 shutdown 调用后等待停止过程结束
     * Block the thread until the node is successfully stopped.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    void join() throws InterruptedException;

    /**
     * 提交一个新任务到 raft group，此方法是线程安全并且非阻塞，
     * 无论任务是否成功提交到 raft group，都会通过 task 关联的 closure done 通知到。
     * 如果当前节点不是 leader，会直接失败通知 done closure。
     *
     * [Thread-safe and wait-free]
     *
     * Apply task to the replicated-state-machine
     *
     * About the ownership:
     * |task.data|: for the performance consideration, we will take away the
     *               content. If you want keep the content, copy it before call
     *               this function
     * |task.done|: If the data is successfully committed to the raft group. We
     *              will pass the ownership to #{@link StateMachine#onApply(Iterator)}.
     *              Otherwise we will specify the error and call it.
     *
     * @param task task to apply
     */
    void apply(final Task task);

    /**
     * [Thread-safe and wait-free]
     * [SSS-发起线性一致读请求]
     *
     * 当可安全读取的时候（appleIndex > ReadIndex）， 设置的的 closure（callback）将被调用，
     * 正常情况下可以从状态机中读取数据返回给客户端， jraft 将保证读取的线性一致性。
     * 其中 requestContext 提供给用户作为请求的附加上下文，可以在 closure 里再次拿到继续处理。
     *
     * 注意：线性一致读可以在集群内的任何节点发起，并不需要强制要求放到 Leader 节点上，
     * 也可以在 Follower 执行，因此可以大大降低 Leader 的读取压力。（JRaft 中可配置是否从 follower 读取，默认不打开）
     *
     * 默认情况下，jraft 提供的线性一致读是基于 RAFT 协议的 ReadIndex 实现的；
     * 在一些更高性能（两个实现的性能差距大概在 15% 左右）的场景下，并且可以保证集群内机器的 CPU 时钟同步，
     * 那么可以采用 Clock + Heartbeat 的 Lease Read 优化，
     * 可以通过服务端设置 RaftOptions 的 ReadOnlyOption 为 ReadOnlyLeaseBased 来实现。
     *
     * Starts a linearizable read-only query request with request context(optional,
     * such as request id etc.) and closure.  The closure will be called when the
     * request is completed, and user can read data from state machine if the result
     * status is OK.
     *
     * @param requestContext the context of request
     * @param done           callback
     *
     * @since 0.0.3
     */
    void readIndex(final byte[] requestContext, final ReadIndexClosure done);

    /**
     * List peers of this raft group, only leader returns.
     *
     * [NOTE] <strong>when list_peers concurrency with {@link #addPeer(PeerId, Closure)}/{@link #removePeer(PeerId, Closure)},
     * maybe return peers is staled.  Because {@link #addPeer(PeerId, Closure)}/{@link #removePeer(PeerId, Closure)}
     * immediately modify configuration in memory</strong>
     *
     * @return the peer list
     */
    List<PeerId> listPeers();

    /**
     * List all alive peers of this raft group, only leader returns.</p>
     *
     * [NOTE] <strong>list_alive_peers is just a transient data (snapshot)
     * and a short-term loss of response by the follower will cause it to
     * temporarily not exist in this list.</strong>
     *
     * @return the alive peer list
     * @since 1.2.6
     */
    List<PeerId> listAlivePeers();

    /**
     * List all learners of this raft group, only leader returns.</p>
     *
     * [NOTE] <strong>when listLearners concurrency with {@link #addLearners(List, Closure)}/{@link #removeLearners(List, Closure)}/{@link #resetLearners(List, Closure)},
     * maybe return peers is staled.  Because {@link #addLearners(List, Closure)}/{@link #removeLearners(List, Closure)}/{@link #resetLearners(List, Closure)}
     * immediately modify configuration in memory</strong>
     *
     * @return the learners set
     * @since 1.3.0
     */
    List<PeerId> listLearners();

    /**
     * List all alive learners of this raft group, only leader returns.</p>
     *
     * [NOTE] <strong>when listAliveLearners concurrency with {@link #addLearners(List, Closure)}/{@link #removeLearners(List, Closure)}/{@link #resetLearners(List, Closure)},
     * maybe return peers is staled.  Because {@link #addLearners(List, Closure)}/{@link #removeLearners(List, Closure)}/{@link #resetLearners(List, Closure)}
     * immediately modify configuration in memory</strong>
     *
     * @return the  alive learners set
     * @since 1.3.0
     */
    List<PeerId> listAliveLearners();

    /**
     * Add a new peer to the raft group. done.run() would be invoked after this
     * operation finishes, describing the detailed result.
     *
     * @param peer peer to add
     * @param done callback
     */
    void addPeer(final PeerId peer, final Closure done);

    /**
     * Remove the peer from the raft group. done.run() would be invoked after
     * operation finishes, describing the detailed result.
     *
     * @param peer peer to remove
     * @param done callback
     */
    void removePeer(final PeerId peer, final Closure done);

    /**
     * Change the configuration of the raft group to |newPeers| , done.un()
     * would be invoked after this operation finishes, describing the detailed result.
     *
     * @param newPeers new peers to change
     * @param done     callback
     */
    void changePeers(final Configuration newPeers, final Closure done);

    /**
     * Reset the configuration of this node individually, without any replication
     * to other peers before this node becomes the leader. This function is
     * supposed to be invoked when the majority of the replication group are
     * dead and you'd like to revive the service in the consideration of
     * availability.
     * Notice that neither consistency nor consensus are guaranteed in this
     * case, BE CAREFULE when dealing with this method.
     *
     * @param newPeers new peers
     */
    Status resetPeers(final Configuration newPeers);

    /**
     * Add some new learners to the raft group. done.run() will be invoked after this
     * operation finishes, describing the detailed result.
     *
     * @param learners learners to add
     * @param done     callback
     * @since 1.3.0
     */
    void addLearners(final List<PeerId> learners, final Closure done);

    /**
     * Remove some learners from the raft group. done.run() will be invoked after this
     * operation finishes, describing the detailed result.
     *
     * @param learners learners to remove
     * @param done     callback
     * @since 1.3.0
     */
    void removeLearners(final List<PeerId> learners, final Closure done);

    /**
     * Reset learners in the raft group. done.run() will be invoked after this
     * operation finishes, describing the detailed result.
     *
     * @param learners learners to set
     * @param done     callback
     * @since 1.3.0
     */
    void resetLearners(final List<PeerId> learners, final Closure done);

    /**
     * 触发当前节点执行一次 snapshot 保存操作，结果通过 done 通知
     *
     * Start a snapshot immediately if possible. done.run() would be invoked when
     * the snapshot finishes, describing the detailed result.
     *
     * @param done callback
     */
    void snapshot(final Closure done);

    /**
     * Reset the election_timeout for the every node.
     *
     * @param electionTimeoutMs the timeout millis of election
     */
    void resetElectionTimeoutMs(final int electionTimeoutMs);

    /**
     * Try transferring leadership to |peer|. If peer is ANY_PEER, a proper follower
     * will be chosen as the leader for the next term.
     * Returns 0 on success, -1 otherwise.
     *
     * @param peer the target peer of new leader
     * @return operation status
     */
    Status transferLeadershipTo(final PeerId peer);

    /**
     * Read the first committed user log from the given index.
     *   Return OK on success and user_log is assigned with the very data. Be awared
     *   that the user_log may be not the exact log at the given index, but the
     *   first available user log from the given index to lastCommittedIndex.
     *   Otherwise, appropriate errors are returned:
     *        - return ELOGDELETED when the log has been deleted;
     *        - return ENOMOREUSERLOG when we can't get a user log even reaching lastCommittedIndex.
     * [NOTE] in consideration of safety, we use lastAppliedIndex instead of lastCommittedIndex
     * in code implementation.
     *
     * @param index log index
     * @return user log entry
     * @throws LogNotFoundException  the user log is deleted at index.
     * @throws LogIndexOutOfBoundsException  the special index is out of bounds.
     */
    UserLog readCommittedUserLog(final long index);

    /**
     * SOFAJRaft users can implement the ReplicatorStateListener interface by themselves.
     * So users can do their own logical operator in this listener when replicator created, destroyed or had some errors.
     *
     * @param replicatorStateListener added ReplicatorStateListener which is implemented by users.
     */
    void addReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener);

    /**
     * End User can remove their implement the ReplicatorStateListener interface by themselves.
     *
     * @param replicatorStateListener need to remove the ReplicatorStateListener which has been added by users.
     */
    void removeReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener);

    /**
     * Remove all the ReplicatorStateListeners which have been added by users.
     *
     */
    void clearReplicatorStateListeners();

    /**
     * Get the ReplicatorStateListeners which have been added by users.
     *
     * @return node's replicatorStatueListeners which have been added by users.
     */
    List<Replicator.ReplicatorStateListener> getReplicatorStatueListeners();

    /**
     * Get the node's target election priority value.
     *
     * @return node's target election priority value.
     * @since 1.3.0
     */
    int getNodeTargetPriority();
}
