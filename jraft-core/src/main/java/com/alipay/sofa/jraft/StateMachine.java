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

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

/**
 * 状态机 StateMachine
 *
 * 用户核心逻辑的实现,核心是 onApply(Iterator) 方法，应用通过 Node#apply(task) 提交的日志到业务状态机。
 *
 * 业务逻辑实现的主要接口，状态机运行在每个 raft 节点上，提交的 task 如果成功，最终都会复制应用到每个节点的状态机上。
 *
 * 什么是分布式一致性：
 *      分布式一致性 (distributed consensus) 是分布式系统中最基本的问题，用来保证一个分布式系统的可靠性以及容灾能力。
 *      简单的来讲，就是如何在多个机器间对某一个值达成一致, 并且当达成一致之后，无论之后这些机器间发生怎样的故障，这个值能保持不变。
 *      抽象定义上， 一个分布式系统里的所有进程要确定一个值 v，如果这个系统满足如下几个性质， 就可以认为它解决了分布式一致性问题, 分别是:
 *          Termination: 所有正常的进程都会决定 v 具体的值，不会出现一直在循环的进程。
 *          Validity: 任何正常的进程确定的值 v’（例如：随机数生成器就不满足这个性质）, 那么 v’ 肯定是某个进程提交的。
 *          Agreement: 所有正常的进程选择的值都是一样的。
 *
 * 什么是一致性状态机：
 *      对于一个无限增长的序列 a[1, 2, 3…], 如果对于任意整数 i, a[i] 的值满足分布式一致性，这个系统就满足一致性状态机的要求。
 *      基本上所有的系统都会有源源不断的操作, 这时候单独对某个特定的值达成一致是不够的。
 *      为了真实系统保证所有的副本的一致性，通常会把操作转化为 write-ahead-log(简称WAL)。
 *      然后让系统的所有副本对WAL保持一致，这样每个进程按照顺序执行WAL里的操作，就能保证最终的状态是一致的。
 *
 *  Write-Ahead-Log：
 *      简称WAL，在分布式存储系统中的元数据更新中应用得十分广泛。
 *      WAL的主要意思是说在将元数据的变更操作写入到持久稳定的db之前，先预先写入到一个log中，然后再由另外的操作将log apply到外部的持久db里去。
 *      这种模式会减少掉每次的db写入操作，尤其当系统要处理大量的transaction操作的时候，WAL的方式相比较于实时同步db的方式有着更高的效率。
 *
 * WAL还有一点很重要的帮助是可以在disaster recovery过程中起到状态恢复的作用，系统在load完元数据db后，再把未来得及提交的WAL apply进来，就能恢复成和之前最终一致的状态。
 *
 * |StateMachine| is the sink of all the events of a very raft node.
 * Implement a specific StateMachine for your own business logic.
 * NOTE: All the interfaces are not guaranteed to be thread safe and they are
 * called sequentially, saying that every single operation will block all the
 * following ones.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 5:43:21 PM
 */
public interface StateMachine {

    /**
     * 最核心的方法，应用任务列表到状态机，任务将按照提交顺序应用。
     * 请注意，当这个方法返回的时候，我们就认为这一批任务都已经成功应用到状态机上，
     * 如果没有完全应用（比如错误、异常），将会被当做一个 critical 级别的错误，报告给状态机的 onError 方法，
     * 错误类型为 ERROR_TYPE_STATE_MACHINE 。
     *
     *
     * Update the StateMachine with a batch a tasks that can be accessed
     * through |iterator|.
     *
     * Invoked when one or more tasks that were passed to Node#apply(Task) have been
     * committed to the raft group (quorum of the group peers have received
     * those tasks and stored them on the backing storage).
     *
     * Once this function returns to the caller, we will regard all the iterated
     * tasks through |iter| have been successfully applied. And if you didn't
     * apply all the the given tasks, we would regard this as a critical error
     * and report a error whose type is ERROR_TYPE_STATE_MACHINE.
     *
     * @param iter iterator of states
     */
    void onApply(final Iterator iter);

    /**
     * 当状态机所在 raft 节点被关闭的时候调用，可以用于一些状态机的资源清理工作，比如关闭文件等。
     *
     * Invoked once when the raft node was shut down.
     * Default do nothing
     */
    void onShutdown();

    /**
     * 保存状态的最新状态，保存的文件信息可以写到 SnapshotWriter 中，保存完成切记调用 done.run(status) 方法。
     * 通常情况下，每次 `onSnapshotSave` 被调用都应该阻塞状态机（同步调用）以保证用户可以捕获当前状态机的状态，
     * 如果想通过异步 snapshot 来提升性能，那么需要用户状态机支持快照读，并先同步读快照，再异步保存快照数据。
     *
     * 如果生成快照成功，需要调用 SnapshotWriter#addFile
     * 方法将快照文件名和对应的元数据信息记录到快照元数据信息表中。
     * 这么做的目的除了能够让 JRaft 识别该快照文件，业务也可以在后续安装快照文件时读取到快照的元数据信息。
     *
     * User defined snapshot generate function, this method will block StateMachine#onApply(Iterator).
     * user can make snapshot async when fsm can be cow(copy-on-write).
     * call done.run(status) when snapshot finished.
     * Default: Save nothing and returns error.
     *
     * @param writer snapshot writer
     * @param done   callback
     */
    void onSnapshotSave(final SnapshotWriter writer, final Closure done);

    /**
     * 加载或者安装 snapshot，从 SnapshotReader 读取 snapshot 文件列表并使用。
     * 注意:
     *  程序启动会调用 `onSnapshotLoad` 方法，也就是说业务状态机的数据一致性保障全权由 jraft 接管，业务状态机的启动时应保持状态为空，
     *  如果状态机持久化了数据那么应该在启动时先清除数据，并依赖 raft snapshot + replay raft log 来恢复状态机数据。
     *
     * User defined snapshot load function
     * get and load snapshot
     * Default: Load nothing and returns error.
     *
     * @param reader snapshot reader
     * @return true on success
     */
    boolean onSnapshotLoad(final SnapshotReader reader);

    /**
     * 当状态机所属的 raft 节点成为 leader 的时候被调用，成为 leader 当前的 term 通过参数传入。
     *
     * Invoked when the belonging node becomes the leader of the group at |term|
     * Default: Do nothing
     *
     * @param term new term num
     */
    void onLeaderStart(final long term);

    /**
     * 当前状态机所属的 raft 节点失去 leader 资格时调用，status 字段描述了详细的原因，比如主动转移 leadership、重新发生选举等。
     *
     * Invoked when this node steps down from the leader of the replication
     * group and |status| describes detailed information
     *
     * @param status status info
     */
    void onLeaderStop(final Status status);

    /**
     * 当 critical 错误发生的时候，会调用此方法，
     * RaftException 包含了 status 等详细的错误信息；
     * 当这个方法被调用后，将不允许新的任务应用到状态机，直到错误被修复并且节点被重启。
     * 因此对于任何在开发阶段发现的错误，都应当及时做修正，如果是 jraft 的问题，请及时报告。
     *
     * This method is called when a critical error was encountered, after this
     * point, no any further modification is allowed to applied to this node
     * until the error is fixed and this node restarts.
     *
     * @param e raft error message
     */
    void onError(final RaftException e);

    /**
     * 当一个 raft group 的节点配置提交到 raft group 日志的时候调用，通常不需要实现此方法，或者打印个日志即可。
     *
     * Invoked when a configuration has been committed to the group.
     *
     * @param conf committed configuration
     */
    void onConfigurationCommitted(final Configuration conf);

    /**
     * 当一个 raft follower 停止 follower 一个 leader 节点的时候调用，
     * 这种情况一般是发生了 leadership 转移，比如重新选举产生了新的 leader，或者进入选举阶段等。
     * 同样 LeaderChangeContext 描述了停止 follow 的 leader 的信息，其中 status 描述了停止 follow 的原因。
     *
     * This method is called when a follower stops following a leader and its leaderId becomes null,
     * situations including:
     * 1. handle election timeout and start preVote
     * 2. receive requests with higher term such as VoteRequest from a candidate
     *    or appendEntries request from a new leader
     * 3. receive timeoutNow request from current leader and start request vote.
     * 
     * the parameter ctx gives the information(leaderId, term and status) about the
     * very leader whom the follower followed before.
     * User can reset the node's information as it stops following some leader.
     *
     * @param ctx context of leader change
     */
    void onStopFollowing(final LeaderChangeContext ctx);

    /**
     * 当一个 raft follower 或者 candidate 节点开始 follow 一个 leader 的时候调用，
     * LeaderChangeContext 包含了 leader 的 PeerId/term/status 等上下文信息。
     * 并且当前 raft node 的 leaderId 属性会被设置为新的 leader 节点 PeerId。
     *
     *
     * This method is called when a follower or candidate starts following a leader and its leaderId
     * (should be NULL before the method is called) is set to the leader's id,
     * situations including:
     * 1. a candidate receives appendEntries request from a leader
     * 2. a follower(without leader) receives appendEntries from a leader
     * 
     * the parameter ctx gives the information(leaderId, term and status) about
     * the very leader whom the follower starts to follow.
     * User can reset the node's information as it starts to follow some leader.
     *
     * @param ctx context of leader change
     */
    void onStartFollowing(final LeaderChangeContext ctx);
}
