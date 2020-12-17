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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.JRaftServiceFactory;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.ReadOnlyService;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.CatchUpClosure;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.closure.SynchronizedClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.Ballot;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.entity.UserLog;
import com.alipay.sofa.jraft.error.LogIndexOutOfBoundsException;
import com.alipay.sofa.jraft.error.LogNotFoundException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.BallotBoxOptions;
import com.alipay.sofa.jraft.option.BootstrapOptions;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.option.LogManagerOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyOption;
import com.alipay.sofa.jraft.option.ReadOnlyServiceOptions;
import com.alipay.sofa.jraft.option.ReplicatorGroupOptions;
import com.alipay.sofa.jraft.option.SnapshotExecutorOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.rpc.impl.core.DefaultRaftClientService;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.SnapshotExecutor;
import com.alipay.sofa.jraft.storage.impl.LogManagerImpl;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl;
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.DisruptorBuilder;
import com.alipay.sofa.jraft.util.DisruptorMetricSet;
import com.alipay.sofa.jraft.util.JRaftServiceLoader;
import com.alipay.sofa.jraft.util.JRaftSignalHandler;
import com.alipay.sofa.jraft.util.LogExceptionHandler;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Platform;
import com.alipay.sofa.jraft.util.RepeatedTimer;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.alipay.sofa.jraft.util.SignalHelper;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import com.alipay.sofa.jraft.util.ThreadHelper;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.concurrent.LongHeldDetectingReadWriteLock;
import com.alipay.sofa.jraft.util.timer.RaftTimerFactory;
import com.google.protobuf.Message;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Node：SOFAJRaft Server 节点；
 * raft分布式算法中需要的所有行为，不限于选举计时、投票计时、快照计时、日志管理、Follower复制器、接收rpc请求等。
 *
 ----------------------------------------------------------------------------------
 JRaft Group：
 在实际应用中，一个Raft Node是没有任何意义的，因此在线上环境中必须是集群（2n+1，1个Leader + 2*（1+）个Follower）；

 JRaft Multi Group：
 一个集群组（JRaft Group）是无法解决大流量的读写瓶颈的，JRaft 自然而然的支持 JRaft Multi Group;


 ----------------------------------------------------------------------------------
 *
 * The raft replica node implementation.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 4:26:51 PM
 */
public class NodeImpl implements Node, RaftServerService {

    private static final Logger LOG = LoggerFactory.getLogger(NodeImpl.class);

    static {
        try {
            if (SignalHelper.supportSignal()) {
                // TODO support windows signal
                if (!Platform.isWindows()) {
                    final List<JRaftSignalHandler> handlers = JRaftServiceLoader.load(JRaftSignalHandler.class) //
                        .sort();
                    SignalHelper.addSignal(SignalHelper.SIG_USR2, handlers);
                }
            }
        } catch (final Throwable t) {
            LOG.error("Fail to add signal.", t);
        }
    }

    public final static RaftTimerFactory                                   TIMER_FACTORY            = JRaftUtils.raftTimerFactory();

    /** 应用任务最大重试次数 */
    // Max retry times when applying tasks.
    private static final int                                               MAX_APPLY_RETRY_TIMES    = 3;

    public static final AtomicInteger                                      GLOBAL_NUM_NODES         = new AtomicInteger(0);

    /** Internal states */
    private final ReadWriteLock                                            readWriteLock            = new NodeReadWriteLock(this);
    protected final Lock                                                   writeLock                = this.readWriteLock.writeLock();
    protected final Lock                                                   readLock                 = this.readWriteLock.readLock();
    private volatile State                                                 state;
    private volatile CountDownLatch                                        shutdownLatch;
    private long                                                           currTerm;
    /**
     * 在一个非对称网络中，S2 和 Leader S1 无法通信，但是它可以和另一个 Follower S3 依然能够通信，
     * 在此情况下，S2 发起预投票得到了 S3 的响应，S2 可以发起投票请求。
     * 接下来 S2 的投票请求会使得 S3 的 Term 也增加以至于超过 Leader S1（S3 收到 S2 的投票请求后，会相应把自己的 Term 提升到跟 S2 一致），
     * 因此 S3 接下来会拒绝 Leader S1 的日志复制。
     * 为解决这种情况，SOFAJRaft 在 Follower 本地维护了一个时间戳来记录收到 Leader 上一次数据更新的时间，
     * Follower S3 只有超过 election timeout 之后才允许接受预投票请求，这样也就避免了 S2 发起投票请求。
     * */
    private volatile long                                                  lastLeaderTimestamp;
    /** Raft Group - Leader */
    private PeerId                                                         leaderId                 = new PeerId();
    private PeerId                                                         votedId;
    /** 选举投票箱 */
    private final Ballot                                                   voteCtx                  = new Ballot();
    /** 预选举投票箱 */
    private final Ballot                                                   prevVoteCtx              = new Ballot();
    private ConfigurationEntry                                             conf;
    private StopTransferArg                                                stopTransferArg;
    /** Raft group and node options and identifier */
    private final String                                                   groupId;
    private NodeOptions                                                    options;
    private RaftOptions                                                    raftOptions;
    private final PeerId                                                   serverId;
    /** Other services */
    private final ConfigurationCtx                                         confCtx;
    private LogStorage                                                     logStorage;
    private RaftMetaStorage                                                metaStorage;
    private ClosureQueue                                                   closureQueue;
    private ConfigurationManager                                           configManager;
    private LogManager                                                     logManager;
    private FSMCaller                                                      fsmCaller;
    private BallotBox                                                      ballotBox;
    private SnapshotExecutor                                               snapshotExecutor;
    /**
     * 用于单个Raft Group 管理所有的 replicator，必要的权限检查和派发；
     * Leader节点通过 Replicator 和 Follower建立连接之后，要发送一个 Probe 类型的探针请求，
     * 目的是知道 Follower 已经拥有的的日志位置，以便于向 Follower 发送后续的日志。
     */
    private ReplicatorGroup                                                replicatorGroup;
    private final List<Closure>                                            shutdownContinuations    = new ArrayList<>();
    private RaftClientService                                              rpcService;
    private ReadOnlyService                                                readOnlyService;
    /** Timers */
    private Scheduler                                                      timerManager;
    /** 预投票计时器
     * 当Leader在一定时间内（election timeout）没有与 Follower 通信时（定期发送心跳包：appendEntries()）
     * Follower就认为Leader已经不能够正常的担任旗舰的职责了，则Follower就会发起一次选举尝试让自己担任Leader */
    private RepeatedTimer                                                  electionTimer;
    /** 定期检查是否需要选举Leader，若存在Candidate节点，则发起预投票，通过之后选举Leader */
    private RepeatedTimer                                                  voteTimer;
    /** Leader下台计时器，定时检查是否需要重新选举Leader */
    private RepeatedTimer                                                  stepDownTimer;
    /** 如果需开启 SOFAJRaft 的 Snapshot 机制，
     * 则需要在其客户端中设置配置参数类 NodeOptions 的“snapshotUri”属性（即为：Snapshot 文件的存储路径），
     * 配置该属性后，默认会启动一个定时器任务（“JRaft-SnapshotTimer”）自动去完成 Snapshot 操作，
     * 间隔时间通过配置类 NodeOptions 的“snapshotIntervalSecs”属性指定，默认 3600 秒。 */
    private RepeatedTimer                                                  snapshotTimer;
    private ScheduledFuture<?>                                             transferTimer;
    private ThreadId                                                       wakingCandidate;
    /** Disruptor to run node service */
    private Disruptor<LogEntryAndClosure>                                  applyDisruptor;
    private RingBuffer<LogEntryAndClosure>                                 applyQueue;

    /** Metrics */
    private NodeMetrics                                                    metrics;

    private NodeId                                                         nodeId;
    private JRaftServiceFactory                                            serviceFactory;

    /** ReplicatorStateListeners */
    private final CopyOnWriteArrayList<Replicator.ReplicatorStateListener> replicatorStateListeners = new CopyOnWriteArrayList<>();
    /** Node's target leader election priority value */
    private volatile int                                                   targetPriority;
    /** The number of elections time out for current node
     * 当前节点参与Leader选举的超时次数
     */
    private volatile int                                                   electionTimeoutCounter;

    private static class NodeReadWriteLock extends LongHeldDetectingReadWriteLock {

        static final long  MAX_BLOCKING_MS_TO_REPORT = SystemPropertyUtil.getLong(
                                                         "jraft.node.detecting.lock.max_blocking_ms_to_report", -1);

        private final Node node;

        public NodeReadWriteLock(final Node node) {
            super(MAX_BLOCKING_MS_TO_REPORT, TimeUnit.MILLISECONDS);
            this.node = node;
        }

        @Override
        public void report(final AcquireMode acquireMode, final Thread heldThread,
                           final Collection<Thread> queuedThreads, final long blockedNanos) {
            final long blockedMs = TimeUnit.NANOSECONDS.toMillis(blockedNanos);
            LOG.warn(
                "Raft-Node-Lock report: currentThread={}, acquireMode={}, heldThread={}, queuedThreads={}, blockedMs={}.",
                Thread.currentThread(), acquireMode, heldThread, queuedThreads, blockedMs);

            final NodeMetrics metrics = this.node.getNodeMetrics();
            if (metrics != null) {
                metrics.recordLatency("node-lock-blocked", blockedMs);
            }
        }
    }

    /**
     * Disruptor队列 - 数据和结束通知的实体类
     *
     * Node service event.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:29:55 PM
     */
    private static class LogEntryAndClosure {
        LogEntry       entry;
        Closure        done;
        long           expectedTerm;
        CountDownLatch shutdownLatch;

        public void reset() {
            this.entry = null;
            this.done = null;
            this.expectedTerm = 0;
            this.shutdownLatch = null;
        }
    }

    /**
     * Disruptor队列-RingBuffer生产工厂，在初始化RingBuffer时使用
     */
    private static class LogEntryAndClosureFactory implements EventFactory<LogEntryAndClosure> {

        @Override
        public LogEntryAndClosure newInstance() {
            return new LogEntryAndClosure();
        }
    }

    /**
     * Disruptor队列-Event句柄处理器
     *
     * Event handler.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:30:07 PM
     */
    private class LogEntryAndClosureHandler implements EventHandler<LogEntryAndClosure> {
        // task list for batch
        private final List<LogEntryAndClosure> tasks = new ArrayList<>(NodeImpl.this.raftOptions.getApplyBatch());

        @Override
        public void onEvent(final LogEntryAndClosure event, final long sequence, final boolean endOfBatch) throws Exception {
            if (event.shutdownLatch != null) {
                if (!this.tasks.isEmpty()) {
                    executeApplyingTasks(this.tasks);
                    this.tasks.clear();
                }
                final int num = GLOBAL_NUM_NODES.decrementAndGet();
                LOG.info("The number of active nodes decrement to {}.", num);
                event.shutdownLatch.countDown();
                return;
            }

            this.tasks.add(event);
            // 是否批处理结束或者已满足批处理中最大的应用数（32）；处理完成则清空List数据；
            if (this.tasks.size() >= NodeImpl.this.raftOptions.getApplyBatch() || endOfBatch) {
                executeApplyingTasks(this.tasks);
                this.tasks.clear();
            }
        }
    }

    /**
     * Configuration commit context.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:29:38 PM
     */
    private static class ConfigurationCtx {
        enum Stage {
            STAGE_NONE, // none stage
            STAGE_CATCHING_UP, // the node is catching-up
            STAGE_JOINT, // joint stage
            STAGE_STABLE // stable stage
        }

        final NodeImpl node;
        Stage          stage;
        // Peers change times
        int            nchanges;
        long           version;
        // peers
        List<PeerId>   newPeers    = new ArrayList<>();
        List<PeerId>   oldPeers    = new ArrayList<>();
        List<PeerId>   addingPeers = new ArrayList<>();
        // learners
        List<PeerId>   newLearners = new ArrayList<>();
        List<PeerId>   oldLearners = new ArrayList<>();
        Closure        done;

        public ConfigurationCtx(final NodeImpl node) {
            super();
            this.node = node;
            this.stage = Stage.STAGE_NONE;
            this.version = 0;
            this.done = null;
        }

        /**
         * Start change configuration.
         */
        void start(final Configuration oldConf, final Configuration newConf, final Closure done) {
            if (isBusy()) {
                if (done != null) {
                    Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Already in busy stage."));
                }
                throw new IllegalStateException("Busy stage");
            }
            if (this.done != null) {
                if (done != null) {
                    Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "Already have done closure."));
                }
                throw new IllegalArgumentException("Already have done closure");
            }
            this.done = done;
            this.stage = Stage.STAGE_CATCHING_UP;
            this.oldPeers = oldConf.listPeers();
            this.newPeers = newConf.listPeers();
            this.oldLearners = oldConf.listLearners();
            this.newLearners = newConf.listLearners();
            final Configuration adding = new Configuration();
            final Configuration removing = new Configuration();
            newConf.diff(oldConf, adding, removing);
            this.nchanges = adding.size() + removing.size();

            addNewLearners();
            if (adding.isEmpty()) {
                nextStage();
                return;
            }
            addNewPeers(adding);
        }

        private void addNewPeers(final Configuration adding) {
            this.addingPeers = adding.listPeers();
            LOG.info("Adding peers: {}.", this.addingPeers);
            for (final PeerId newPeer : this.addingPeers) {
                if (!this.node.replicatorGroup.addReplicator(newPeer)) {
                    LOG.error("Node {} start the replicator failed, peer={}.", this.node.getNodeId(), newPeer);
                    onCaughtUp(this.version, newPeer, false);
                    return;
                }
                final OnCaughtUp caughtUp = new OnCaughtUp(this.node, this.node.currTerm, newPeer, this.version);
                final long dueTime = Utils.nowMs() + this.node.options.getElectionTimeoutMs();
                if (!this.node.replicatorGroup.waitCaughtUp(newPeer, this.node.options.getCatchupMargin(), dueTime,
                    caughtUp)) {
                    LOG.error("Node {} waitCaughtUp, peer={}.", this.node.getNodeId(), newPeer);
                    onCaughtUp(this.version, newPeer, false);
                    return;
                }
            }
        }

        private void addNewLearners() {
            final Set<PeerId> addingLearners = new HashSet<>(this.newLearners);
            addingLearners.removeAll(this.oldLearners);
            LOG.info("Adding learners: {}.", this.addingPeers);
            for (final PeerId newLearner : addingLearners) {
                if (!this.node.replicatorGroup.addReplicator(newLearner, ReplicatorType.Learner)) {
                    LOG.error("Node {} start the learner replicator failed, peer={}.", this.node.getNodeId(),
                        newLearner);
                }
            }
        }

        void onCaughtUp(final long version, final PeerId peer, final boolean success) {
            if (version != this.version) {
                LOG.warn("Ignore onCaughtUp message, mismatch configuration context version, expect {}, but is {}.",
                    this.version, version);
                return;
            }
            Requires.requireTrue(this.stage == Stage.STAGE_CATCHING_UP, "Stage is not in STAGE_CATCHING_UP");
            if (success) {
                this.addingPeers.remove(peer);
                if (this.addingPeers.isEmpty()) {
                    nextStage();
                    return;
                }
                return;
            }
            LOG.warn("Node {} fail to catch up peer {} when trying to change peers from {} to {}.",
                this.node.getNodeId(), peer, this.oldPeers, this.newPeers);
            reset(new Status(RaftError.ECATCHUP, "Peer %s failed to catch up.", peer));
        }

        void reset() {
            reset(null);
        }

        void reset(final Status st) {
            if (st != null && st.isOk()) {
                this.node.stopReplicator(this.newPeers, this.oldPeers);
                this.node.stopReplicator(this.newLearners, this.oldLearners);
            } else {
                this.node.stopReplicator(this.oldPeers, this.newPeers);
                this.node.stopReplicator(this.oldLearners, this.newLearners);
            }
            clearPeers();
            clearLearners();

            this.version++;
            this.stage = Stage.STAGE_NONE;
            this.nchanges = 0;
            if (this.done != null) {
                Utils.runClosureInThread(this.done, st != null ? st : new Status(RaftError.EPERM,
                    "Leader stepped down."));
                this.done = null;
            }
        }

        private void clearLearners() {
            this.newLearners.clear();
            this.oldLearners.clear();
        }

        private void clearPeers() {
            this.newPeers.clear();
            this.oldPeers.clear();
            this.addingPeers.clear();
        }

        /**
         * Invoked when this node becomes the leader, write a configuration change log as the first log.
         */
        void flush(final Configuration conf, final Configuration oldConf) {
            Requires.requireTrue(!isBusy(), "Flush when busy");
            this.newPeers = conf.listPeers();
            this.newLearners = conf.listLearners();
            if (oldConf == null || oldConf.isEmpty()) {
                this.stage = Stage.STAGE_STABLE;
                this.oldPeers = this.newPeers;
                this.oldLearners = this.newLearners;
            } else {
                this.stage = Stage.STAGE_JOINT;
                this.oldPeers = oldConf.listPeers();
                this.oldLearners = oldConf.listLearners();
            }
            this.node.unsafeApplyConfiguration(conf, oldConf == null || oldConf.isEmpty() ? null : oldConf, true);
        }

        void nextStage() {
            Requires.requireTrue(isBusy(), "Not in busy stage");
            switch (this.stage) {
                case STAGE_CATCHING_UP:
                    if (this.nchanges > 0) {
                        this.stage = Stage.STAGE_JOINT;
                        this.node.unsafeApplyConfiguration(new Configuration(this.newPeers, this.newLearners),
                            new Configuration(this.oldPeers), false);
                        return;
                    }
                case STAGE_JOINT:
                    this.stage = Stage.STAGE_STABLE;
                    this.node.unsafeApplyConfiguration(new Configuration(this.newPeers, this.newLearners), null, false);
                    break;
                case STAGE_STABLE:
                    final boolean shouldStepDown = !this.newPeers.contains(this.node.serverId);
                    reset(new Status());
                    if (shouldStepDown) {
                        this.node.stepDown(this.node.currTerm, true, new Status(RaftError.ELEADERREMOVED,
                            "This node was removed."));
                    }
                    break;
                case STAGE_NONE:
                    // noinspection ConstantConditions
                    Requires.requireTrue(false, "Can't reach here");
                    break;
            }
        }

        boolean isBusy() {
            return this.stage != Stage.STAGE_NONE;
        }
    }

    public NodeImpl() {
        this(null, null);
    }

    public NodeImpl(final String groupId, final PeerId serverId) {
        super();
        if (groupId != null) {
            Utils.verifyGroupId(groupId);
        }
        this.groupId = groupId;
        this.serverId = serverId != null ? serverId.copy() : null;
        this.state = State.STATE_UNINITIALIZED;
        this.currTerm = 0;
        updateLastLeaderTimestamp(Utils.monotonicMs());
        this.confCtx = new ConfigurationCtx(this);
        this.wakingCandidate = null;
        final int num = GLOBAL_NUM_NODES.incrementAndGet();
        LOG.info("The number of active nodes increment to {}.", num);
    }

    private boolean initSnapshotStorage() {
        if (StringUtils.isEmpty(this.options.getSnapshotUri())) {
            LOG.warn("Do not set snapshot uri, ignore initSnapshotStorage.");
            return true;
        }
        this.snapshotExecutor = new SnapshotExecutorImpl();
        final SnapshotExecutorOptions opts = new SnapshotExecutorOptions();
        opts.setUri(this.options.getSnapshotUri());
        opts.setFsmCaller(this.fsmCaller);
        opts.setNode(this);
        opts.setLogManager(this.logManager);
        opts.setAddr(this.serverId != null ? this.serverId.getEndpoint() : null);
        opts.setInitTerm(this.currTerm);
        opts.setFilterBeforeCopyRemote(this.options.isFilterBeforeCopyRemote());
        // get snapshot throttle
        opts.setSnapshotThrottle(this.options.getSnapshotThrottle());
        return this.snapshotExecutor.init(opts);
    }

    private boolean initLogStorage() {
        Requires.requireNonNull(this.fsmCaller, "Null fsm caller");
        this.logStorage = this.serviceFactory.createLogStorage(this.options.getLogUri(), this.raftOptions);
        this.logManager = new LogManagerImpl();
        final LogManagerOptions opts = new LogManagerOptions();
        opts.setLogEntryCodecFactory(this.serviceFactory.createLogEntryCodecFactory());
        opts.setLogStorage(this.logStorage);
        opts.setConfigurationManager(this.configManager);
        opts.setFsmCaller(this.fsmCaller);
        opts.setNodeMetrics(this.metrics);
        opts.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());
        opts.setRaftOptions(this.raftOptions);
        return this.logManager.init(opts);
    }

    private boolean initMetaStorage() {
        this.metaStorage = this.serviceFactory.createRaftMetaStorage(this.options.getRaftMetaUri(), this.raftOptions);
        RaftMetaStorageOptions opts = new RaftMetaStorageOptions();
        opts.setNode(this);
        if (!this.metaStorage.init(opts)) {
            LOG.error("Node {} init meta storage failed, uri={}.", this.serverId, this.options.getRaftMetaUri());
            return false;
        }
        this.currTerm = this.metaStorage.getTerm();
        this.votedId = this.metaStorage.getVotedFor().copy();
        return true;
    }

    private void handleSnapshotTimeout() {
        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                return;
            }
        } finally {
            this.writeLock.unlock();
        }
        // do_snapshot in another thread to avoid blocking the timer thread.
        Utils.runInThread(() -> doSnapshot(null));
    }

    private void handleElectionTimeout() {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (this.state != State.STATE_FOLLOWER) {
                return;
            }
            // 心跳包会不断的更新lastLeaderTimestamp，
            // 如果超过了electionTimeoutMs，Follower会发起第一轮选举（预投票，因为也有可能是Follower失联了）
            if (isCurrentLeaderValid()) {
                return;
            }
            resetLeaderId(PeerId.emptyPeer(), new Status(RaftError.ERAFTTIMEDOUT, "Lost connection from leader %s.",
                this.leaderId));

            // Judge whether to launch a election.
            // 检查优先级是否允许参与此term election timeout的选举（分配权重越大的节点，越有机会获取Leader身份））
            if (!allowLaunchElection()) {
                return;
            }

            doUnlock = false;

            // 预投票选举确认是否可以发起Leader选举
            // 原因：若因为网络分区原因，Follower与Leader及大多数的Follower失去链接，
            //      当前Follower开始竞选Leader，虽然竟然会失败，但是Term是递增的，会导致在与Leader连接之后Term大于Leader，
            //      从而导致Leader step down，为了预防"瞎捣乱"，加入 Pre-Vote机制，只有获取大多数节点承认之后，才可开始Leader选举。
            preVote();

        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * 定义了当前 Node 节点判断 priority 值与本地全局变量 targetPriority 值的逻辑。
     * 同时，如果在上一轮选举周期内没有选举出 Leader 角色的节点，那么执行目标优先级衰减降级方法，并设置相关的变量值。
     * Whether to allow for launching election or not by comparing node's priority with target
     * priority. And at the same time, if next leader is not elected until next election
     * timeout, it decays its local target priority exponentially.
     *
     * @return Whether current node will launch election or not.
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private boolean allowLaunchElection() {

        // Priority 0 is a special value so that a node will never participate in election.
        // ElectionPriority.NotElected = 0，表示不参与Leader选举
        if (this.serverId.isPriorityNotElected()) {
            LOG.warn("Node {} will never participate in election, because it's priority={}.", getNodeId(),
                this.serverId.getPriority());
            return false;
        }

        // If this nodes disable priority election, then it can make a election.
        // ElectionPriority.NotElected <= -1，表示该节点禁用了优先级选举功能
        if (this.serverId.isPriorityDisabled()) {
            return true;
        }

        // If current node's priority < target_priority, it does not initiate leader,
        // election and waits for the next election timeout.
        // 当前节点的优先级少于目标节点的优先级，参与election timeout选举次数 + 1
        if (this.serverId.getPriority() < this.targetPriority) {
            this.electionTimeoutCounter++;

            // If next leader is not elected until next election timeout, it
            // decays its local target priority exponentially.
            // 第一轮参与当前term的election timeout，不衰减目标优先级；
            // 等待第二轮参与term的election timeout的目标优先级衰减；
            if (this.electionTimeoutCounter > 1) {
                decayTargetPriority();
                // 已经衰减目标优先级，重置当前term election timeout参与的选举的超时次数
                this.electionTimeoutCounter = 0;
            }

            // 第一轮参与当前term的election timeout，由于优先级权重分配，当前不参与
            if (this.electionTimeoutCounter == 1) {
                LOG.debug("Node {} does not initiate leader election and waits for the next election timeout.",
                    getNodeId());
                return false;
            }
        }
        // 比较当前节点优先级和目标的优先级，若符合要求则参与Leader选举
        return this.serverId.getPriority() >= this.targetPriority;
    }

    /**
     * Decay targetPriority value based on gap value.
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void decayTargetPriority() {
        // Default Gap value should be bigger than 10.
        final int decayPriorityGap = Math.max(this.options.getDecayPriorityGap(), 10);
        final int gap = Math.max(decayPriorityGap, (this.targetPriority / 5));

        final int prevTargetPriority = this.targetPriority;
        this.targetPriority = Math.max(ElectionPriority.MinValue, (this.targetPriority - gap));
        LOG.info("Node {} priority decay, from: {}, to: {}.", getNodeId(), prevTargetPriority, this.targetPriority);
    }

    /**
     * Check and set configuration for node.At the same time, if configuration is changed,
     * then compute and update the target priority value.
     *
     * @param inLock whether the writeLock has already been locked in other place.
     *
     */
    private void checkAndSetConfiguration(final boolean inLock) {
        if (!inLock) {
            this.writeLock.lock();
        }
        try {
            final ConfigurationEntry prevConf = this.conf;
            this.conf = this.logManager.checkAndSetConfiguration(prevConf);

            if (this.conf != prevConf) {
                // Update target priority value
                final int prevTargetPriority = this.targetPriority;
                this.targetPriority = getMaxPriorityOfNodes(this.conf.getConf().getPeers());
                if (prevTargetPriority != this.targetPriority) {
                    LOG.info("Node {} target priority value has changed from: {}, to: {}.", getNodeId(),
                        prevTargetPriority, this.targetPriority);
                }
                this.electionTimeoutCounter = 0;
            }
        } finally {
            if (!inLock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * Get max priority value for all nodes in the same Raft group, and update current node's target priority value.
     *
     * @param peerIds peer nodes in the same Raft group
     *
     */
    private int getMaxPriorityOfNodes(final List<PeerId> peerIds) {
        Requires.requireNonNull(peerIds, "Null peer list");

        int maxPriority = Integer.MIN_VALUE;
        for (final PeerId peerId : peerIds) {
            final int priorityVal = peerId.getPriority();
            maxPriority = Math.max(priorityVal, maxPriority);
        }

        return maxPriority;
    }

    private boolean initFSMCaller(final LogId bootstrapId) {
        if (this.fsmCaller == null) {
            LOG.error("Fail to init fsm caller, null instance, bootstrapId={}.", bootstrapId);
            return false;
        }
        this.closureQueue = new ClosureQueueImpl();
        final FSMCallerOptions opts = new FSMCallerOptions();
        opts.setAfterShutdown(status -> afterShutdown());
        opts.setLogManager(this.logManager);
        opts.setFsm(this.options.getFsm());
        opts.setClosureQueue(this.closureQueue);
        opts.setNode(this);
        opts.setBootstrapId(bootstrapId);
        opts.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());
        return this.fsmCaller.init(opts);
    }

    private static class BootstrapStableClosure extends LogManager.StableClosure {

        private final SynchronizedClosure done = new SynchronizedClosure();

        public BootstrapStableClosure() {
            super(null);
        }

        public Status await() throws InterruptedException {
            return this.done.await();
        }

        @Override
        public void run(final Status status) {
            this.done.run(status);
        }
    }

    public boolean bootstrap(final BootstrapOptions opts) throws InterruptedException {
        if (opts.getLastLogIndex() > 0 && (opts.getGroupConf().isEmpty() || opts.getFsm() == null)) {
            LOG.error("Invalid arguments for bootstrap, groupConf={}, fsm={}, lastLogIndex={}.", opts.getGroupConf(),
                opts.getFsm(), opts.getLastLogIndex());
            return false;
        }
        if (opts.getGroupConf().isEmpty()) {
            LOG.error("Bootstrapping an empty node makes no sense.");
            return false;
        }
        Requires.requireNonNull(opts.getServiceFactory(), "Null jraft service factory");
        this.serviceFactory = opts.getServiceFactory();
        // Term is not an option since changing it is very dangerous
        final long bootstrapLogTerm = opts.getLastLogIndex() > 0 ? 1 : 0;
        final LogId bootstrapId = new LogId(opts.getLastLogIndex(), bootstrapLogTerm);
        this.options = new NodeOptions();
        this.raftOptions = this.options.getRaftOptions();
        this.metrics = new NodeMetrics(opts.isEnableMetrics());
        this.options.setFsm(opts.getFsm());
        this.options.setLogUri(opts.getLogUri());
        this.options.setRaftMetaUri(opts.getRaftMetaUri());
        this.options.setSnapshotUri(opts.getSnapshotUri());

        this.configManager = new ConfigurationManager();
        // Create fsmCaller at first as logManager needs it to report error
        this.fsmCaller = new FSMCallerImpl();

        if (!initLogStorage()) {
            LOG.error("Fail to init log storage.");
            return false;
        }
        if (!initMetaStorage()) {
            LOG.error("Fail to init meta storage.");
            return false;
        }
        if (this.currTerm == 0) {
            this.currTerm = 1;
            if (!this.metaStorage.setTermAndVotedFor(1, new PeerId())) {
                LOG.error("Fail to set term.");
                return false;
            }
        }

        if (opts.getFsm() != null && !initFSMCaller(bootstrapId)) {
            LOG.error("Fail to init fsm caller.");
            return false;
        }

        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.getId().setTerm(this.currTerm);
        entry.setPeers(opts.getGroupConf().listPeers());
        entry.setLearners(opts.getGroupConf().listLearners());

        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);

        final BootstrapStableClosure bootstrapDone = new BootstrapStableClosure();
        this.logManager.appendEntries(entries, bootstrapDone);
        if (!bootstrapDone.await().isOk()) {
            LOG.error("Fail to append configuration.");
            return false;
        }

        if (opts.getLastLogIndex() > 0) {
            if (!initSnapshotStorage()) {
                LOG.error("Fail to init snapshot storage.");
                return false;
            }
            final SynchronizedClosure snapshotDone = new SynchronizedClosure();
            this.snapshotExecutor.doSnapshot(snapshotDone);
            if (!snapshotDone.await().isOk()) {
                LOG.error("Fail to save snapshot, status={}.", snapshotDone.getStatus());
                return false;
            }
        }

        if (this.logManager.getFirstLogIndex() != opts.getLastLogIndex() + 1) {
            throw new IllegalStateException("First and last log index mismatch");
        }
        if (opts.getLastLogIndex() > 0) {
            if (this.logManager.getLastLogIndex() != opts.getLastLogIndex()) {
                throw new IllegalStateException("Last log index mismatch");
            }
        } else {
            if (this.logManager.getLastLogIndex() != opts.getLastLogIndex() + 1) {
                throw new IllegalStateException("Last log index mismatch");
            }
        }

        return true;
    }

    private int heartbeatTimeout(final int electionTimeout) {
        return Math.max(electionTimeout / this.raftOptions.getElectionHeartbeatFactor(), 10);
    }

    private int randomTimeout(final int timeoutMs) {
        return ThreadLocalRandom.current().nextInt(timeoutMs, timeoutMs + this.raftOptions.getMaxElectionDelayMs());
    }

    @Override
    public boolean init(final NodeOptions opts) {
        Requires.requireNonNull(opts, "Null node options");
        Requires.requireNonNull(opts.getRaftOptions(), "Null raft options");
        Requires.requireNonNull(opts.getServiceFactory(), "Null jraft service factory");
        this.serviceFactory = opts.getServiceFactory();
        this.options = opts;
        this.raftOptions = opts.getRaftOptions();
        this.metrics = new NodeMetrics(opts.isEnableMetrics());
        this.serverId.setPriority(opts.getElectionPriority());
        this.electionTimeoutCounter = 0;

        if (this.serverId.getIp().equals(Utils.IP_ANY)) {
            LOG.error("Node can't started from IP_ANY.");
            return false;
        }

        if (!NodeManager.getInstance().serverExists(this.serverId.getEndpoint())) {
            LOG.error("No RPC server attached to, did you forget to call addService?");
            return false;
        }

        // 定时器，在 heartbeat、block 时使用
        this.timerManager = TIMER_FACTORY.getRaftScheduler(this.options.isSharedTimerPool(),
            this.options.getTimerPoolSize(), "JRaft-Node-ScheduleThreadPool");

        // Init timers
        final String suffix = getNodeId().toString();
        /* 创建：投票计时器、选举计时器、Down机计时器、快照计时器 start */
        String name = "JRaft-VoteTimer-" + suffix;
        // 定期检查是否需要选举Leader，若存在Candidate节点，则发起预投票，通过之后选举Leader
        this.voteTimer = new RepeatedTimer(name, this.options.getElectionTimeoutMs(), TIMER_FACTORY.getVoteTimer(
            this.options.isSharedVoteTimer(), name)) {

            @Override
            protected void onTrigger() {
                // 处理投票超时（节点启动之后立即执行）
                handleVoteTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                // 在一定范围内产生一个随机数
                return randomTimeout(timeoutMs);
            }
        };
        name = "JRaft-ElectionTimer-" + suffix;
        // 设计预投票计时器
        // 当Leader在一定时间内（election timeout）没有与 Follower 通信时（定期发送心跳包：appendEntries()）
        // Follower就认为Leader已经不能够正常的担任旗舰的职责了，则Follower就会发起一次选举尝试让自己担任Leader
        // 在发起Leader选举之前，也有可能是Follower与集群失联，因此需要先发起预投票，
        // 当满足大多数（Peers.size / 2 + 1）之后，才可以发起Leader选举投票
        this.electionTimer = new RepeatedTimer(name, this.options.getElectionTimeoutMs(),
            TIMER_FACTORY.getElectionTimer(this.options.isSharedElectionTimer(), name)) {

            @Override
            protected void onTrigger() {
                handleElectionTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                // 在一定服务内随机一个时间范围，
                // 尽量避免同时发起Leader投票，而造成分票（VoteSplit）
                return randomTimeout(timeoutMs);
            }
        };
        name = "JRaft-StepDownTimer-" + suffix;
        // Leader下台计时器，定时检查是否需要重新选举Leader
        this.stepDownTimer = new RepeatedTimer(name, this.options.getElectionTimeoutMs() >> 1,
            TIMER_FACTORY.getStepDownTimer(this.options.isSharedStepDownTimer(), name)) {

            @Override
            protected void onTrigger() {
                handleStepDownTimeout();
            }
        };
        name = "JRaft-SnapshotTimer-" + suffix;
        // 快照计时器
        this.snapshotTimer = new RepeatedTimer(name, this.options.getSnapshotIntervalSecs() * 1000,
            TIMER_FACTORY.getSnapshotTimer(this.options.isSharedSnapshotTimer(), name)) {

            private volatile boolean firstSchedule = true;

            @Override
            protected void onTrigger() {
                handleSnapshotTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                if (!this.firstSchedule) {
                    return timeoutMs;
                }

                // Randomize the first snapshot trigger timeout
                this.firstSchedule = false;
                if (timeoutMs > 0) {
                    int half = timeoutMs / 2;
                    return half + ThreadLocalRandom.current().nextInt(half);
                } else {
                    return timeoutMs;
                }
            }
        };
        /* 创建：投票计时器、选举计时器、Down机计时器、快照计时器 start */

        this.configManager = new ConfigurationManager();

        /* 构建一个Disruptor队列，队列RingBuffer Size:16384，MULTI, 使用 BlockingWaitStrategy（
        BlockingWaitStrategy 是最低效的策略，但其对CPU的消耗最小并且在各种不同的部署环境中能提供更加一致性的表现；
        ）  */
        this.applyDisruptor = DisruptorBuilder.<LogEntryAndClosure> newInstance() //
            .setRingBufferSize(this.raftOptions.getDisruptorBufferSize()) //
            .setEventFactory(new LogEntryAndClosureFactory()) //
            .setThreadFactory(new NamedThreadFactory("JRaft-NodeImpl-Disruptor-", true)) //
            .setProducerType(ProducerType.MULTI) //
            .setWaitStrategy(new BlockingWaitStrategy()) //
            .build();
        this.applyDisruptor.handleEventsWith(new LogEntryAndClosureHandler());
        this.applyDisruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.applyQueue = this.applyDisruptor.start();
        if (this.metrics.getMetricRegistry() != null) {
            this.metrics.getMetricRegistry().register("jraft-node-impl-disruptor",
                new DisruptorMetricSet(this.applyQueue));
        }

        // 有限状态机
        this.fsmCaller = new FSMCallerImpl();
        // 创建并实例化：LogManagerImpl（Disruptor队列），采用rocksdb作为日志存储
        if (!initLogStorage()) {
            LOG.error("Node {} initLogStorage failed.", getNodeId());
            return false;
        }
        // Raft元数据存储（非线程安全），存储 curr term，vote PeerId。
        if (!initMetaStorage()) {
            LOG.error("Node {} initMetaStorage failed.", getNodeId());
            return false;
        }
        //
        if (!initFSMCaller(new LogId(0, 0))) {
            LOG.error("Node {} initFSMCaller failed.", getNodeId());
            return false;
        }
        // 日志复制选票箱，用于执行状态机
        this.ballotBox = new BallotBox();
        final BallotBoxOptions ballotBoxOpts = new BallotBoxOptions();
        ballotBoxOpts.setWaiter(this.fsmCaller);
        ballotBoxOpts.setClosureQueue(this.closureQueue);
        if (!this.ballotBox.init(ballotBoxOpts)) {
            LOG.error("Node {} init ballotBox failed.", getNodeId());
            return false;
        }

        if (!initSnapshotStorage()) {
            LOG.error("Node {} initSnapshotStorage failed.", getNodeId());
            return false;
        }

        final Status st = this.logManager.checkConsistency();
        if (!st.isOk()) {
            LOG.error("Node {} is initialized with inconsistent log, status={}.", getNodeId(), st);
            return false;
        }
        this.conf = new ConfigurationEntry();
        this.conf.setId(new LogId());
        // if have log using conf in log, else using conf in options
        if (this.logManager.getLastLogIndex() > 0) {
            checkAndSetConfiguration(false);
        } else {
            this.conf.setConf(this.options.getInitialConf());
            // initially set to max(priority of all nodes)
            this.targetPriority = getMaxPriorityOfNodes(this.conf.getConf().getPeers());
        }

        if (!this.conf.isEmpty()) {
            Requires.requireTrue(this.conf.isValid(), "Invalid conf: %s", this.conf);
        } else {
            LOG.info("Init node {} with empty conf.", this.serverId);
        }

        // TODO RPC service and ReplicatorGroup is in cycle dependent, refactor it
        this.replicatorGroup = new ReplicatorGroupImpl();
        this.rpcService = new DefaultRaftClientService(this.replicatorGroup);
        final ReplicatorGroupOptions rgOpts = new ReplicatorGroupOptions();
        rgOpts.setHeartbeatTimeoutMs(heartbeatTimeout(this.options.getElectionTimeoutMs()));
        rgOpts.setElectionTimeoutMs(this.options.getElectionTimeoutMs());
        rgOpts.setLogManager(this.logManager);
        rgOpts.setBallotBox(this.ballotBox);
        rgOpts.setNode(this);
        rgOpts.setRaftRpcClientService(this.rpcService);
        rgOpts.setSnapshotStorage(this.snapshotExecutor != null ? this.snapshotExecutor.getSnapshotStorage() : null);
        rgOpts.setRaftOptions(this.raftOptions);
        rgOpts.setTimerManager(this.timerManager);

        // Adds metric registry to RPC service.
        this.options.setMetricRegistry(this.metrics.getMetricRegistry());

        if (!this.rpcService.init(this.options)) {
            LOG.error("Fail to init rpc service.");
            return false;
        }
        this.replicatorGroup.init(new NodeId(this.groupId, this.serverId), rgOpts);

        this.readOnlyService = new ReadOnlyServiceImpl();
        final ReadOnlyServiceOptions rosOpts = new ReadOnlyServiceOptions();
        rosOpts.setFsmCaller(this.fsmCaller);
        rosOpts.setNode(this);
        rosOpts.setRaftOptions(this.raftOptions);

        if (!this.readOnlyService.init(rosOpts)) {
            LOG.error("Fail to init readOnlyService.");
            return false;
        }

        // set state to follower
        this.state = State.STATE_FOLLOWER;

        if (LOG.isInfoEnabled()) {
            LOG.info("Node {} init, term={}, lastLogId={}, conf={}, oldConf={}.", getNodeId(), this.currTerm,
                this.logManager.getLastLogId(false), this.conf.getConf(), this.conf.getOldConf());
        }

        if (this.snapshotExecutor != null && this.options.getSnapshotIntervalSecs() > 0) {
            LOG.debug("Node {} start snapshot timer, term={}.", getNodeId(), this.currTerm);
            this.snapshotTimer.start();
        }

        if (!this.conf.isEmpty()) {
            stepDown(this.currTerm, false, new Status());
        }

        if (!NodeManager.getInstance().add(this)) {
            LOG.error("NodeManager add {} failed.", getNodeId());
            return false;
        }

        // Now the raft node is started , have to acquire the writeLock to avoid race
        // conditions
        this.writeLock.lock();
        // 若是单节点则选举自己当Leader
        if (this.conf.isStable() && this.conf.getConf().size() == 1 && this.conf.getConf().contains(this.serverId)) {
            // The group contains only this server which must be the LEADER, trigger
            // the timer immediately.
            electSelf();
        } else {
            this.writeLock.unlock();
        }

        return true;
    }

    @OnlyForTest
    void tryElectSelf() {
        this.writeLock.lock();
        // unlock in electSelf
        electSelf();
    }

    /**
     * 选举Leader流程，投选自己一票，然后发起Leader选举投票请求
     */
    // should be in writeLock
    private void electSelf() {
        long oldTerm;
        try {
            LOG.info("Node {} start vote and grant vote self, term={}.", getNodeId(), this.currTerm);
            // 若当前节点不允许参与选举则结束
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do electSelf as it is not in {}.", getNodeId(), this.conf);
                return;
            }
            // 停止预投票计时器 election timer Task，为选举Leader做准备；
            // 将当前的计时器添加cancelledTimeouts队列，由tick++ 移除出timeouts队列
            if (this.state == State.STATE_FOLLOWER) {
                LOG.debug("Node {} stop election timer, term={}.", getNodeId(), this.currTerm);
                this.electionTimer.stop();
            }
            // Follower重置自己持有的LeaderId，并且将节点从Follower变为Candidate，将任期抬高一次（term++）
            resetLeaderId(PeerId.emptyPeer(), new Status(RaftError.ERAFTTIMEDOUT,
                "A follower's leader_id is reset to NULL as it begins to request_vote."));
            this.state = State.STATE_CANDIDATE;
            this.currTerm++;
            // 候选人（Candidate）为自己投选一票
            this.votedId = this.serverId.copy();
            LOG.debug("Node {} start vote timer, term={} .", getNodeId(), this.currTerm);
            // 启动选举期限（在一定范围内产生一个随机时间）时间轮，
            // 若stepDownWhenVoteTimedout=true，进行Leader下线操作，然后发起预投票流程（第一轮流程）
            // 否则再次发起选举自己为Leader投票请求（第二轮流程）
            this.voteTimer.start();
            // 初始化投票箱有效的投票数（peers.size()/2 + 1）
            this.voteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            // 留存当前的最新任期
            oldTerm = this.currTerm;
        } finally {
            this.writeLock.unlock();
        }

        // 刷盘之后的最新lastLogIndexId（Disruptor队列）
        final LogId lastLogId = this.logManager.getLastLogId(true);

        this.writeLock.lock();
        try {
            // vote need defense ABA after unlock&writeLock
            if (oldTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when getLastLogId.", getNodeId(), this.currTerm);
                return;
            }
            for (final PeerId peer : this.conf.listPeers()) {
                if (peer.equals(this.serverId)) {
                    continue;
                }
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, address={}.", getNodeId(), peer.getEndpoint());
                    continue;
                }
                final OnRequestVoteRpcDone done = new OnRequestVoteRpcDone(peer, this.currTerm, this);
                done.request = RequestVoteRequest.newBuilder() //
                    .setPreVote(false) // It's not a pre-vote request.
                    .setGroupId(this.groupId) //
                    .setServerId(this.serverId.toString()) //
                    .setPeerId(peer.toString()) //
                    .setTerm(this.currTerm) //
                    .setLastLogIndex(lastLogId.getIndex()) //
                    .setLastLogTerm(lastLogId.getTerm()) //
                    .build();
                this.rpcService.requestVote(peer.getEndpoint(), done.request, done);
            }

            this.metaStorage.setTermAndVotedFor(this.currTerm, this.serverId);
            this.voteCtx.grant(this.serverId);
            if (this.voteCtx.isGranted()) {
                becomeLeader();
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private void resetLeaderId(final PeerId newLeaderId, final Status status) {
        if (newLeaderId.isEmpty()) {
            if (!this.leaderId.isEmpty() && this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                this.fsmCaller.onStopFollowing(new LeaderChangeContext(this.leaderId.copy(), this.currTerm, status));
            }
            this.leaderId = PeerId.emptyPeer();
        } else {
            if (this.leaderId == null || this.leaderId.isEmpty()) {
                this.fsmCaller.onStartFollowing(new LeaderChangeContext(newLeaderId, this.currTerm, status));
            }
            this.leaderId = newLeaderId.copy();
        }
    }

    /**
     * 基于请求中的term和本地节点状态，决策是否执行 stepDown；
     *      1.请求中的term大于当前最新的term，执行 stepDown;
     *      2. Candidate 收到任期相同的 Leader的 appendEntries;
     *      3. Leader.isEmpty() true，
     *  Leader == null || Leader.isEmpty()，将请求的PeerId的设置为Leader；
     */
    // in writeLock
    private void checkStepDown(final long requestTerm, final PeerId serverId) {
        final Status status = new Status();
        if (requestTerm > this.currTerm) {
            status.setError(RaftError.ENEWLEADER, "Raft node receives message from new leader with higher term.");
            stepDown(requestTerm, false, status);
        } else if (this.state != State.STATE_FOLLOWER) {
            status.setError(RaftError.ENEWLEADER, "Candidate receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        } else if (this.leaderId.isEmpty()) {
            status.setError(RaftError.ENEWLEADER, "Follower receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        }
        // save current leader
        // 若Group Raft中 PeerId的leaderId不存在，则将请求的PeerId设置为Leader
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            resetLeaderId(serverId, status);
        }
    }

    /**
     * 当选Leader，重置RaftGroup Term，添加协议参与者（PeerId）的Replicator进行复制日志
     * */
    private void becomeLeader() {
        Requires.requireTrue(this.state == State.STATE_CANDIDATE, "Illegal state: " + this.state);
        LOG.info("Node {} become leader of group, term={}, conf={}, oldConf={}.", getNodeId(), this.currTerm,
            this.conf.getConf(), this.conf.getOldConf());
        // cancel candidate vote timer
        // 取消candidate（候选人）投票定时器
        stopVoteTimer();
        // 设置为Leader，保存leaderId，重置term
        this.state = State.STATE_LEADER;
        this.leaderId = this.serverId.copy();
        this.replicatorGroup.resetTerm(this.currTerm);
        // Start follower's replicators
        // 为每一个Follower（追随者）设置一个Replicator；
        for (final PeerId peer : this.conf.listPeers()) {
            if (peer.equals(this.serverId)) {
                continue;
            }
            LOG.debug("Node {} add a replicator, term={}, peer={}.", getNodeId(), this.currTerm, peer);
            if (!this.replicatorGroup.addReplicator(peer)) {
                LOG.error("Fail to add a replicator, peer={}.", peer);
            }
        }

        // Start learner's replicators
        // 为每一个Learner（学习者）设置一个Replicator；
        for (final PeerId peer : this.conf.listLearners()) {
            LOG.debug("Node {} add a learner replicator, term={}, peer={}.", getNodeId(), this.currTerm, peer);
            if (!this.replicatorGroup.addReplicator(peer, ReplicatorType.Learner)) {
                LOG.error("Fail to add a learner replicator, peer={}.", peer);
            }
        }

        // init commit manager
        // 因为Leader在下台的时候会将最新日志的标致（pendingIndex=0），重置为0，表示在新Leader未产生之前不允许复制日志；
        // 所以在Leader上线之后，将pendingIndex的设置为当前最新的日志+1，同时Follower也可以开始复制日志了；
        this.ballotBox.resetPendingIndex(this.logManager.getLastLogIndex() + 1);
        // Register _conf_ctx to reject configuration changing before the first log
        // is committed.
        // 在提交第一个日志之前拒绝 _conf_ctx 配置更改；
        if (this.confCtx.isBusy()) {
            throw new IllegalStateException();
        }
        // 将配置更改日志写入为第一个日志（成为Leader时的第一个日志）
        this.confCtx.flush(this.conf.getConf(), this.conf.getOldConf());
        // 启动Leader下台的定时器
        this.stepDownTimer.start();
    }

    // should be in writeLock
    // Leader下台，
    private void stepDown(final long term, final boolean wakeupCandidate, final Status status) {
        LOG.debug("Node {} stepDown, term={}, newTerm={}, wakeupCandidate={}.", getNodeId(), this.currTerm, term,
            wakeupCandidate);
        // 当前节点是否异常（状态不是STATE_LEADER, STATE_TRANSFERRING, STATE_CANDIDATE, STATE_FOLLOWER）或正在关闭
        if (!this.state.isActive()) {
            return;
        }
        // 若是Candidate，则停止选举
        if (this.state == State.STATE_CANDIDATE) {
            stopVoteTimer();
        }
        // 若是Leader或者Transferring状态
        else if (this.state.compareTo(State.STATE_TRANSFERRING) <= 0) {
            // 停止stepDownTimer的运作
            stopStepDownTimer();
            // 清空投票箱
            this.ballotBox.clearPendingTasks();
            // signal fsm leader stop immediately
            if (this.state == State.STATE_LEADER) {
                // 发送Leader下台的时间给其他的Follower节点
                onLeaderStop(status);
            }
        }
        // reset leader_id
        resetLeaderId(PeerId.emptyPeer(), status);

        // soft state in memory
        this.state = State.STATE_FOLLOWER;
        this.confCtx.reset();
        updateLastLeaderTimestamp(Utils.monotonicMs());
        if (this.snapshotExecutor != null) {
            this.snapshotExecutor.interruptDownloadingSnapshots(term);
        }

        // meta state
        if (term > this.currTerm) {
            this.currTerm = term;
            this.votedId = PeerId.emptyPeer();
            this.metaStorage.setTermAndVotedFor(term, this.votedId);
        }

        // 暂停所有日志复制的 Replicator 线程，
        // 同时找到下一个具有最完备日志的节点作为最后可能接任下一任 Leader 角色的 Candidate 候选人。
        // 所以引入优先级选举的概念后，除了需要比较日志的 logindex 值大小以外，
        // 如果两个节点的 logindex 值是相等的，那么还需要再判断 priority 值。
        if (wakeupCandidate) {
            this.wakingCandidate = this.replicatorGroup.stopAllAndFindTheNextCandidate(this.conf);
            if (this.wakingCandidate != null) {
                Replicator.sendTimeoutNowAndStop(this.wakingCandidate, this.options.getElectionTimeoutMs());
            }
        } else {
            this.replicatorGroup.stopAll();
        }
        if (this.stopTransferArg != null) {
            if (this.transferTimer != null) {
                this.transferTimer.cancel(true);
            }
            // There is at most one StopTransferTimer at the same term, it's safe to
            // mark stopTransferArg to NULL
            this.stopTransferArg = null;
        }
        // Learner node will not trigger the election timer.
        if (!isLearner()) {
            this.electionTimer.restart();
        } else {
            LOG.info("Node {} is a learner, election timer is not started.", this.nodeId);
        }
    }

    // Should be in readLock
    private boolean isLearner() {
        return this.conf.listLearners().contains(this.serverId);
    }

    private void stopStepDownTimer() {
        if (this.stepDownTimer != null) {
            this.stepDownTimer.stop();
        }
    }

    private void stopVoteTimer() {
        if (this.voteTimer != null) {
            this.voteTimer.stop();
        }
    }

    class LeaderStableClosure extends LogManager.StableClosure {

        public LeaderStableClosure(final List<LogEntry> entries) {
            super(entries);
        }

        @Override
        public void run(final Status status) {
            if (status.isOk()) {
                NodeImpl.this.ballotBox.commitAt(this.firstLogIndex, this.firstLogIndex + this.nEntries - 1,
                    NodeImpl.this.serverId);
            } else {
                LOG.error("Node {} append [{}, {}] failed, status={}.", getNodeId(), this.firstLogIndex,
                    this.firstLogIndex + this.nEntries - 1, status);
            }
        }
    }

    /*
    两阶段提交（2PC）
        1、第一阶段：投票阶段（Prepare）
            第一阶段主要分为3步
            1）事务询问
                协调者 向所有的 参与者 发送事务预处理请求，称之为Prepare，并开始等待各 参与者 的响应。
            2）执行本地事务
                各个 参与者 节点执行本地事务操作,但在执行完成后并不会真正提交数据库本地事务，
                而是先向 协调者 报告说：“我这边可以处理了/我这边不能处理”。.
            3）各参与者向协调者反馈事务询问的响应
                如果 参与者 成功执行了事务操作,那么就反馈给协调者 Yes 响应,表示事务可以执行,
                如果没有 参与者 成功执行事务,那么就反馈给协调者 No 响应,表示事务不可以执行。

    ----第一阶段执行完后，存在两种可能。1、所有都返回Yes. 2、有一个或者多个返回No。----

        2、第二阶段：提交/执行阶段（成功流程）（Commit）
            成功条件：所有参与者都返回Yes。
            第二阶段主要分为两步
            1)所有的参与者反馈给协调者的信息都是Yes,那么就会执行事务提交
                协调者 向 所有参与者 节点发出Commit请求.
            2)事务提交
                参与者 收到Commit请求之后,就会正式执行本地事务Commit操作,并在完成提交之后释放整个事务执行期间占用的事务资源。
        3、第二阶段：提交/执行阶段（异常流程）（Rollback）
            异常条件：任何一个 参与者 向 协调者 反馈了 No 响应,或者等待超时之后,协调者尚未收到所有参与者的反馈响应。
            异常流程第二阶段也分为两步
            1)发送回滚请求
                协调者 向所有参与者节点发出 RollBack 请求.
            2)事务回滚
                参与者 接收到RollBack请求后,会回滚本地事务。
    * */

    /**
     * 由Leader执行应用任务，检查Task的term，为Task创建投票机制，决策LogEntry是否允许Commit；
     * ------
     * Raft 算法：
     *      Raft 是一种为了管理复制日志的一致性算法；
     *
     * 什么是一致性呢？
     *      Raft论文：一致性算法允许一组机器像一个整体一样工作，即使其中一些机器出现故障也能够继续工作下去；
     *
     * 可视化的Raft算法：
     *      https://raft.github.io/raftscope/index.html
     *
     *      1）准备阶段
     *          Leader 节点创建并初始化投票箱，用于确定有效投票数 quorum（PeerId.size() / 2 + 1）；
     *      2）确认阶段
     *          复制日志的 pipeline 机制
     *
     *          Pipeline 使得 Leader 和 Follower 双方不再需要严格遵从 “Request - Response - Request” 的交互模式，
     *          Leader 可以在没有收到 Response 的情况下，持续的将复制日志的 AppendEntriesRequest 发送给 Follower。
     *
     *          在具体实现时，Leader 只需要针对每个 Follower 维护一个队列，记录下已经复制的日志，
     *          如果有日志复制失败的情况，就将其后的日志重发给 Follower。
     *
     * ---------
     * 源码解读 {@link BallotBox#appendPendingTask(Configuration, Configuration, Closure)
     *      创建并初始化投票箱，确认有效投票数 quorum（peers.size()/2 + 1），
     *      根据 pendingIndex<=0 再次验证是否是 Leader节点为LogEntry决策Commit发起投票机制，
     *      设置callback回调，用于确认是否赢得过半的票数；
     * }
     * 源码解读 {@link LogManagerImpl#appendEntries(List, LogManager.StableClosure)
     *      Raft协议参与者接收日志（Leader，Follower，Learner），主要实现如下：
     *          1）如果term < currentTerm, 不接受日志并返回false
     *          2）如果索引prevLogIndex处的日志的任期号与prevLogTerm不匹配, 不接受日志并返回false
     *          3）如果一条已存在的日志与新的冲突(index相同但是term不同), 则删除已经存在的日志条目和它之后所有的日志条目
     *          4）添加任何在已有日志中不存在的条目
     *          5）如果leaderCommit > commitIndex, 则设置commitIndex = min(leaderCommit, index of last new entry)
     * }
     */
    private void executeApplyingTasks(final List<LogEntryAndClosure> tasks) {
        this.writeLock.lock();
        try {
            final int size = tasks.size();
            // 若当前节点不是Leader，则标记任务失败，设置Callback；
            if (this.state != State.STATE_LEADER) {
                final Status st = new Status();
                if (this.state != State.STATE_TRANSFERRING) {
                    st.setError(RaftError.EPERM, "Is not leader.");
                } else {
                    st.setError(RaftError.EBUSY, "Is transferring leadership.");
                }
                LOG.debug("Node {} can't apply, status={}.", getNodeId(), st);
                final List<LogEntryAndClosure> savedTasks = new ArrayList<>(tasks);
                Utils.runInThread(() -> {
                    for (int i = 0; i < size; i++) {
                        savedTasks.get(i).done.run(st);
                    }
                });
                return;
            }
            final List<LogEntry> entries = new ArrayList<>(size);
            // 循环处理Tasks，
            for (int i = 0; i < size; i++) {
                final LogEntryAndClosure task = tasks.get(i);
                // 若预期term不是默认值-1，或者预期term与当前term不一致，则忽略此Task的LogEntry；
                if (task.expectedTerm != -1 && task.expectedTerm != this.currTerm) {
                    LOG.debug("Node {} can't apply task whose expectedTerm={} doesn't match currTerm={}.", getNodeId(),
                        task.expectedTerm, this.currTerm);
                    if (task.done != null) {
                        final Status st = new Status(RaftError.EPERM, "expected_term=%d doesn't match current_term=%d",
                            task.expectedTerm, this.currTerm);
                        Utils.runClosureInThread(task.done, st);
                    }
                    continue;
                }
                // Leader 节点调用appendPendingTask，为Task创建并初始化投票机制，决策LogEntry 是否允许commit
                if (!this.ballotBox.appendPendingTask(this.conf.getConf(),
                    this.conf.isStable() ? null : this.conf.getOldConf(), task.done)) {
                    Utils.runClosureInThread(task.done, new Status(RaftError.EINTERNAL, "Fail to append task."));
                    continue;
                }
                // set task entry info before adding to list.
                // 设置日志的term为当前term，类型为ENTRY_TYPE_DATA
                task.entry.getId().setTerm(this.currTerm);
                task.entry.setType(EnumOutter.EntryType.ENTRY_TYPE_DATA);
                entries.add(task.entry);
            }
            this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
            // update conf.first
            checkAndSetConfiguration(true);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Returns the node metrics.
     *
     * @return returns metrics of current node.
     */
    @Override
    public NodeMetrics getNodeMetrics() {
        return this.metrics;
    }

    /**
     * Returns the JRaft service factory for current node.
     * @since 1.2.6
     * @return the service factory
     */
    public JRaftServiceFactory getServiceFactory() {
        return this.serviceFactory;
    }

    /**
     *
     *
     */
    @Override
    public void readIndex(final byte[] requestContext, final ReadIndexClosure done) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(done, new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(done, "Null closure");
        this.readOnlyService.addRequest(requestContext, done);
    }

    /**
     * ReadIndex response closure
     * @author dennis
     */
    private class ReadIndexHeartbeatResponseClosure extends RpcResponseClosureAdapter<AppendEntriesResponse> {
        final ReadIndexResponse.Builder             respBuilder;
        final RpcResponseClosure<ReadIndexResponse> closure;
        final int                                   quorum;
        final int                                   failPeersThreshold;
        int                                         ackSuccess;
        int                                         ackFailures;
        boolean                                     isDone;

        public ReadIndexHeartbeatResponseClosure(final RpcResponseClosure<ReadIndexResponse> closure,
                                                 final ReadIndexResponse.Builder rb, final int quorum,
                                                 final int peersCount) {
            super();
            this.closure = closure;
            this.respBuilder = rb;
            this.quorum = quorum;
            this.failPeersThreshold = peersCount % 2 == 0 ? (quorum - 1) : quorum;
            this.ackSuccess = 0;
            this.ackFailures = 0;
            this.isDone = false;
        }

        @Override
        public synchronized void run(final Status status) {
            if (this.isDone) {
                return;
            }
            if (status.isOk() && getResponse().getSuccess()) {
                this.ackSuccess++;
            } else {
                this.ackFailures++;
            }
            // Include leader self vote yes.
            if (this.ackSuccess + 1 >= this.quorum) {
                this.respBuilder.setSuccess(true);
                this.closure.setResponse(this.respBuilder.build());
                this.closure.run(Status.OK());
                this.isDone = true;
            } else if (this.ackFailures >= this.failPeersThreshold) {
                this.respBuilder.setSuccess(false);
                this.closure.setResponse(this.respBuilder.build());
                this.closure.run(Status.OK());
                this.isDone = true;
            }
        }
    }

    /**
     * Handle read index request.
     */
    @Override
    public void handleReadIndexRequest(final ReadIndexRequest request, final RpcResponseClosure<ReadIndexResponse> done) {
        final long startMs = Utils.monotonicMs();
        this.readLock.lock();
        try {
            switch (this.state) {
                case STATE_LEADER:
                    readLeader(request, ReadIndexResponse.newBuilder(), done);
                    break;
                case STATE_FOLLOWER:
                    readFollower(request, done);
                    break;
                case STATE_TRANSFERRING:
                    done.run(new Status(RaftError.EBUSY, "Is transferring leadership."));
                    break;
                default:
                    done.run(new Status(RaftError.EPERM, "Invalid state for readIndex: %s.", this.state));
                    break;
            }
        } finally {
            this.readLock.unlock();
            this.metrics.recordLatency("handle-read-index", Utils.monotonicMs() - startMs);
            this.metrics.recordSize("handle-read-index-entries", request.getEntriesCount());
        }
    }

    private int getQuorum() {
        final Configuration c = this.conf.getConf();
        if (c.isEmpty()) {
            return 0;
        }
        return c.getPeers().size() / 2 + 1;
    }

    private void readFollower(final ReadIndexRequest request, final RpcResponseClosure<ReadIndexResponse> closure) {
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            closure.run(new Status(RaftError.EPERM, "No leader at term %d.", this.currTerm));
            return;
        }
        // send request to leader.
        final ReadIndexRequest newRequest = ReadIndexRequest.newBuilder() //
            .mergeFrom(request) //
            .setPeerId(this.leaderId.toString()) //
            .build();
        this.rpcService.readIndex(this.leaderId.getEndpoint(), newRequest, -1, closure);
    }

    private void readLeader(final ReadIndexRequest request, final ReadIndexResponse.Builder respBuilder,
                            final RpcResponseClosure<ReadIndexResponse> closure) {
        final int quorum = getQuorum();
        if (quorum <= 1) {
            // Only one peer, fast path.
            respBuilder.setSuccess(true) //
                .setIndex(this.ballotBox.getLastCommittedIndex());
            closure.setResponse(respBuilder.build());
            closure.run(Status.OK());
            return;
        }

        final long lastCommittedIndex = this.ballotBox.getLastCommittedIndex();
        if (this.logManager.getTerm(lastCommittedIndex) != this.currTerm) {
            // Reject read only request when this leader has not committed any log entry at its term
            closure
                .run(new Status(
                    RaftError.EAGAIN,
                    "ReadIndex request rejected because leader has not committed any log entry at its term, logIndex=%d, currTerm=%d.",
                    lastCommittedIndex, this.currTerm));
            return;
        }
        respBuilder.setIndex(lastCommittedIndex);

        if (request.getPeerId() != null) {
            // request from follower or learner, check if the follower/learner is in current conf.
            final PeerId peer = new PeerId();
            peer.parse(request.getServerId());
            if (!this.conf.contains(peer) && !this.conf.containsLearner(peer)) {
                closure
                    .run(new Status(RaftError.EPERM, "Peer %s is not in current configuration: %s.", peer, this.conf));
                return;
            }
        }

        ReadOnlyOption readOnlyOpt = this.raftOptions.getReadOnlyOptions();
        if (readOnlyOpt == ReadOnlyOption.ReadOnlyLeaseBased && !isLeaderLeaseValid()) {
            // If leader lease timeout, we must change option to ReadOnlySafe
            readOnlyOpt = ReadOnlyOption.ReadOnlySafe;
        }

        switch (readOnlyOpt) {
            case ReadOnlySafe:
                final List<PeerId> peers = this.conf.getConf().getPeers();
                Requires.requireTrue(peers != null && !peers.isEmpty(), "Empty peers");
                final ReadIndexHeartbeatResponseClosure heartbeatDone = new ReadIndexHeartbeatResponseClosure(closure,
                    respBuilder, quorum, peers.size());
                // Send heartbeat requests to followers
                for (final PeerId peer : peers) {
                    if (peer.equals(this.serverId)) {
                        continue;
                    }
                    this.replicatorGroup.sendHeartbeat(peer, heartbeatDone);
                }
                break;
            case ReadOnlyLeaseBased:
                // Responses to followers and local node.
                respBuilder.setSuccess(true);
                closure.setResponse(respBuilder.build());
                closure.run(Status.OK());
                break;
        }
    }

    /**
     * Task: 用于向一个 Raft group 提交一个任务，Task包含：数据（二进制协议：protobuf），term（预期任期），done（Callback 接口）等；
     * Disruptor 队列 - 使用LogEntry封装Task，以事件的形式投递给RingBuffer；
     * 源码解读 {@link LogEntryAndClosureHandler#onEvent(LogEntryAndClosure, long, boolean)
     *      接收到RingBuffer的事件，调用executeApplyingTasks消费事件；
     * }
     * 源码解读 {@link NodeImpl#executeApplyingTasks(List)
     *      以Leader身份处理任务，包括：决策是否Commit、异步刷盘、写入Rock是DB存储、应用任务至状态机等；
     * }
     */
    @Override
    public void apply(final Task task) {
        // 当前节点（Leader）被关闭
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(task.getDone(), new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(task, "Null task");

        // 创建一个LogEntry,用于封装Task数据
        final LogEntry entry = new LogEntry();
        entry.setData(task.getData());
        int retryTimes = 0;
        try {
            //将SourceEventModel转换成TargetEventModel，然后disruptor中的handler链就消费TargetEventModel对象；
            final EventTranslator<LogEntryAndClosure> translator = (event, sequence) -> {
                event.reset();
                event.done = task.getDone();
                event.entry = entry;
                event.expectedTerm = task.getExpectedTerm();
            };
            /**
             * 尝试将事件发布至缓冲区（RingBuffer）Disruptor队列进行异步处理，若指定容量不可用，则返回false，最大重试次数为3次；
             * 当事件在RingBuffer可用时，Disruptor 队列设置了一个回调的接口函数（EventHandler<T>）用于消费Disruptor队列中的事件；
             * 源码解读- {@link LogEntryAndClosureHandler#onEvent(LogEntryAndClosure, long, boolean)} 消费Disruptor队列事件}
             *
             * */
            while (true) {
                // Task封装成LogEntry对象以事件的形式投递给
                if (this.applyQueue.tryPublishEvent(translator)) {
                    break;
                } else {
                    retryTimes++;
                    if (retryTimes > MAX_APPLY_RETRY_TIMES) {
                        Utils.runClosureInThread(task.getDone(),
                            new Status(RaftError.EBUSY, "Node is busy, has too many tasks."));
                        LOG.warn("Node {} applyQueue is overload.", getNodeId());
                        this.metrics.recordTimes("apply-task-overload-times", 1);
                        return;
                    }
                    ThreadHelper.onSpinWait();
                }
            }

        } catch (final Exception e) {
            LOG.error("Fail to apply task.", e);
            Utils.runClosureInThread(task.getDone(), new Status(RaftError.EPERM, "Node is down."));
        }
    }

    /**
     * [SSS-预选举投票入口]
     *
     * 接收日志的follower需要实现：
     *      如果term < currentTerm, 那么拒绝投票并返回false
     *      如果votedFor为空或者与candidateId相同, 并且候选人的日志和自己一样新或者更新, 那么就给候选人投票并返回true
     */
    @Override
    public Message handlePreVoteRequest(final RequestVoteRequest request) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 节点状态不是 LEADER, TRANSFERRING, CANDIDATE, FOLLOWER，则非正常节点
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(RequestVoteResponse.getDefaultInstance(), RaftError.EINVAL,
                        "Node %s is not in active state, state %s.", getNodeId(), this.state.name());
            }
            // 构建一个 candidate ，作为 raft 协议参与者
            final PeerId candidateId = new PeerId();
            // 解析 request 请求携带的 serverId 内容是否符合标准
            if (!candidateId.parse(request.getServerId())) {
                LOG.warn("Node {} received PreVoteRequest from {} serverId bad format.", getNodeId(),
                    request.getServerId());
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(RequestVoteResponse.getDefaultInstance(), RaftError.EINVAL,
                        "Parse candidateId failed: %s.", request.getServerId());
            }
            boolean granted = false;
            // noinspection ConstantConditions
            do {
                // 不在conf范围的节点忽略（不参与Leader选举）
                if (!this.conf.contains(candidateId)) {
                    LOG.warn("Node {} ignore PreVoteRequest from {} as it is not in conf <{}>.", getNodeId(),
                        request.getServerId(), this.conf);
                    break;
                }
                // 当前存在Leader，并且Leader还还在ElectionTime期间，不可预投票
                // isCurrentLeaderValid() 作用：
                //      在一个非对称网络中，S2 和 Leader S1 无法通信，但是它可以和另一个 Follower S3 依然能够通信，
                //      在此情况下，S2 发起预投票得到了 S3 的响应，S2 可以发起投票请求。
                //      接下来 S2 的投票请求会使得 S3 的 Term 也增加以至于超过 Leader S1（S3 收到 S2 的投票请求后，会相应把自己的 Term 提升到跟 S2 一致），
                //      因此 S3 接下来会拒绝 Leader S1 的日志复制。
                //      为解决这种情况，SOFAJRaft 在 Follower 本地维护了一个时间戳来记录收到 Leader 上一次数据更新的时间，
                //      Follower S3 只有超过 election timeout 之后才允许接受预投票请求，这样也就避免了 S2 发起投票请求。
                if (this.leaderId != null && !this.leaderId.isEmpty() && isCurrentLeaderValid()) {
                    LOG.info(
                        "Node {} ignore PreVoteRequest from {}, term={}, currTerm={}, because the leader {}'s lease is still valid.",
                        getNodeId(), request.getServerId(), request.getTerm(), this.currTerm, this.leaderId);
                    break;
                }
                // 请求节点的term少于当前term
                if (request.getTerm() < this.currTerm) {
                    LOG.info("Node {} ignore PreVoteRequest from {}, term={}, currTerm={}.", getNodeId(),
                        request.getServerId(), request.getTerm(), this.currTerm);
                    // A follower replicator may not be started when this node become leader, so we must check it.
                    // 此节点有可能是Leader节点，校验请求节点是否为复制节点，重新加入到replicatorGroup中；
                    checkReplicator(candidateId);
                    break;
                }
                // A follower replicator may not be started when this node become leader, so we must check it.
                // check replicator state
                // 请求节点的term大于等于当前term，那么请求节点也有可能是Leader，
                // 校验请求节点是否为复制节点，重新加入到replicatorGroup中；
                checkReplicator(candidateId);

                doUnlock = false;
                this.writeLock.unlock();

                // 获取最新的日志
                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();
                final LogId requestLastLogId = new LogId(request.getLastLogIndex(), request.getLastLogTerm());
                // 比较请求节点的日志完整度和最新节点的日志完整度
                granted = requestLastLogId.compareTo(lastLogId) >= 0;

                LOG.info(
                    "Node {} received PreVoteRequest from {}, term={}, currTerm={}, granted={}, requestLastLogId={}, lastLogId={}.",
                    getNodeId(), request.getServerId(), request.getTerm(), this.currTerm, granted, requestLastLogId,
                    lastLogId);
            } while (false); // do-while（false）单次循环只是为了使用break；

            // 在预选举投票中，Follower至少可投选一个预选票发起者的Follower成为Candidates；
            // 不限制Follower的有效票数为一张，只要符合要求即可获取选票
            return RequestVoteResponse.newBuilder() //
                .setTerm(this.currTerm) //
                .setGranted(granted) //
                .build();
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    // in read_lock
    private boolean isLeaderLeaseValid() {
        final long monotonicNowMs = Utils.monotonicMs();
        if (checkLeaderLease(monotonicNowMs)) {
            return true;
        }
        checkDeadNodes0(this.conf.getConf().getPeers(), monotonicNowMs, false, null);
        return checkLeaderLease(monotonicNowMs);
    }

    private boolean checkLeaderLease(final long monotonicNowMs) {
        return monotonicNowMs - this.lastLeaderTimestamp < this.options.getLeaderLeaseTimeoutMs();
    }

    private boolean isCurrentLeaderValid() {
        return Utils.monotonicMs() - this.lastLeaderTimestamp < this.options.getElectionTimeoutMs();
    }

    private void updateLastLeaderTimestamp(final long lastLeaderTimestamp) {
        this.lastLeaderTimestamp = lastLeaderTimestamp;
    }

    private void checkReplicator(final PeerId candidateId) {
        if (this.state == State.STATE_LEADER) {
            this.replicatorGroup.checkReplicator(candidateId, false);
        }
    }

    @Override
    public Message handleRequestVoteRequest(final RequestVoteRequest request) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(RequestVoteResponse.getDefaultInstance(), RaftError.EINVAL,
                        "Node %s is not in active state, state %s.", getNodeId(), this.state.name());
            }
            final PeerId candidateId = new PeerId();
            if (!candidateId.parse(request.getServerId())) {
                LOG.warn("Node {} received RequestVoteRequest from {} serverId bad format.", getNodeId(),
                    request.getServerId());
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(RequestVoteResponse.getDefaultInstance(), RaftError.EINVAL,
                        "Parse candidateId failed: %s.", request.getServerId());
            }

            // noinspection ConstantConditions
            do {
                // check term
                if (request.getTerm() >= this.currTerm) {
                    LOG.info("Node {} received RequestVoteRequest from {}, term={}, currTerm={}.", getNodeId(),
                        request.getServerId(), request.getTerm(), this.currTerm);
                    // increase current term, change state to follower
                    if (request.getTerm() > this.currTerm) {
                        stepDown(request.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                            "Raft node receives higher term RequestVoteRequest."));
                    }
                } else {
                    // ignore older term
                    LOG.info("Node {} ignore RequestVoteRequest from {}, term={}, currTerm={}.", getNodeId(),
                        request.getServerId(), request.getTerm(), this.currTerm);
                    break;
                }
                doUnlock = false;
                this.writeLock.unlock();

                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();
                // vote need ABA check after unlock&writeLock
                if (request.getTerm() != this.currTerm) {
                    LOG.warn("Node {} raise term {} when get lastLogId.", getNodeId(), this.currTerm);
                    break;
                }

                final boolean logIsOk = new LogId(request.getLastLogIndex(), request.getLastLogTerm())
                    .compareTo(lastLogId) >= 0;

                if (logIsOk && (this.votedId == null || this.votedId.isEmpty())) {
                    stepDown(request.getTerm(), false, new Status(RaftError.EVOTEFORCANDIDATE,
                        "Raft node votes for some candidate, step down to restart election_timer."));
                    this.votedId = candidateId.copy();
                    this.metaStorage.setVotedFor(candidateId);
                }
            } while (false);

            return RequestVoteResponse.newBuilder() //
                .setTerm(this.currTerm) //
                .setGranted(request.getTerm() == this.currTerm && candidateId.equals(this.votedId)) //
                .build();
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private static class FollowerStableClosure extends LogManager.StableClosure {

        final long                          committedIndex;
        final AppendEntriesResponse.Builder responseBuilder;
        final NodeImpl                      node;
        final RpcRequestClosure             done;
        final long                          term;

        public FollowerStableClosure(final AppendEntriesRequest request,
                                     final AppendEntriesResponse.Builder responseBuilder, final NodeImpl node,
                                     final RpcRequestClosure done, final long term) {
            super(null);
            this.committedIndex = Math.min(
            // committed index is likely less than the lastLogIndex
                request.getCommittedIndex(),
                // The logs after the appended entries can not be trust, so we can't commit them even if their indexes are less than request's committed index.
                request.getPrevLogIndex() + request.getEntriesCount());
            this.responseBuilder = responseBuilder;
            this.node = node;
            this.done = done;
            this.term = term;
        }

        @Override
        public void run(final Status status) {

            if (!status.isOk()) {
                this.done.run(status);
                return;
            }

            this.node.readLock.lock();
            try {
                if (this.term != this.node.currTerm) {
                    // The change of term indicates that leader has been changed during
                    // appending entries, so we can't respond ok to the old leader
                    // because we are not sure if the appended logs would be truncated
                    // by the new leader:
                    //  - If they won't be truncated and we respond failure to the old
                    //    leader, the new leader would know that they are stored in this
                    //    peer and they will be eventually committed when the new leader
                    //    found that quorum of the cluster have stored.
                    //  - If they will be truncated and we responded success to the old
                    //    leader, the old leader would possibly regard those entries as
                    //    committed (very likely in a 3-nodes cluster) and respond
                    //    success to the clients, which would break the rule that
                    //    committed entries would never be truncated.
                    // So we have to respond failure to the old leader and set the new
                    // term to make it stepped down if it didn't.
                    this.responseBuilder.setSuccess(false).setTerm(this.node.currTerm);
                    this.done.sendResponse(this.responseBuilder.build());
                    return;
                }
            } finally {
                // It's safe to release lock as we know everything is ok at this point.
                this.node.readLock.unlock();
            }

            // Don't touch node any more.
            this.responseBuilder.setSuccess(true).setTerm(this.term);

            // Ballot box is thread safe and tolerates disorder.
            this.node.ballotBox.setLastCommittedIndex(this.committedIndex);

            this.done.sendResponse(this.responseBuilder.build());
        }
    }

    /**
     * [SSS-Raft日志复制 关注方法]
     *
     * 探针（or心跳）请求流程概括：
     *      1) 如果当前节点处于非活跃状态，则响应错误；
     *      2) 否则，解析请求来源节点的serverId，如果解析失败则响应错误；
     *      3) 否则，校验请求中的 term 值是否小于当前节点，如果是则拒绝请求；
     *      4) 否则，基于请求和当前节点本地状态判断是否需要执行 stepDown 操作；
     *      5) 判断请求来源节点是否是当前节点所认可的 Leader 节点，如果不是则说明可能出现网络分区，
     *         尝试将响应中的 term 值加 1，以触发请求节点执行 stepDown 操作；
     *      6) 否则，更新本地记录的最近一次收到来自 Leader 节点的时间戳；
     *      7) 校验最近一次完成复制的 LogEntry 对应的 term 值是否与本地相匹配，如果不匹配则拒绝请求，并返回本地已知的最新 logIndex 值；
     *      8) 否则，依据请求中的 committedIndex 值更新本地的 committedIndex 值，同时响应请求，返回本地已知的最新 logIndex 和 term 值。
     *
     * 复制日志数据：
     * 源码解读 {@link NodeImpl#logEntryFromMeta(long, ByteBuffer, RaftOutter.EntryMeta)
     *      Follower 节点会基于请求中的 LogEntry 元数据和数据体信息逐一解析构造对应的 LogEntry 对象；
     * }
     * 源码解读 {@link LogManagerImpl#appendEntries(List, LogManager.StableClosure)
     *      Follower节点调用，向节点追加日志数据的操作；
     * }
     */
    @Override
    public Message handleAppendEntriesRequest(final AppendEntriesRequest request, final RpcRequestClosure done) {
        boolean doUnlock = true;
        final long startMs = Utils.monotonicMs();
        this.writeLock.lock();
        final int entriesCount = request.getEntriesCount();
        try {
            // 当前节点处于非活跃状态，响应错误
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(AppendEntriesResponse.getDefaultInstance(), RaftError.EINVAL,
                        "Node %s is not in active state, state %s.", getNodeId(), this.state.name());
            }

            // 解析来源（协议参与者）节点信息：IP+端口，若解析失败则响应错误；
            final PeerId serverId = new PeerId();
            if (!serverId.parse(request.getServerId())) {
                LOG.warn("Node {} received AppendEntriesRequest from {} serverId bad format.", getNodeId(),
                    request.getServerId());
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(AppendEntriesResponse.getDefaultInstance(), RaftError.EINVAL,
                        "Parse serverId failed: %s.", request.getServerId());
            }

            // Check stale term
            // 若请求中的term值少于当前term，则拒绝请求，并返回当前最新的term值；
            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale AppendEntriesRequest from {}, term={}, currTerm={}.", getNodeId(),
                    request.getServerId(), request.getTerm(), this.currTerm);
                return AppendEntriesResponse.newBuilder() //
                    .setSuccess(false) //
                    .setTerm(this.currTerm) //
                    .build();
            }

            // Check term and state to step down
            // 基于请求中的term和本地节点状态，决策是否执行 stepDown；
            checkStepDown(request.getTerm(), serverId);
            // 请求来源节点并不是当前节点已知的Leader节点，
            // 可能已经发生网络分区，尝试将请求的 term+1，以触发Leader的stepDown；
            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                    serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                stepDown(request.getTerm() + 1, false, new Status(RaftError.ELEADERCONFLICT,
                    "More than one leader in the same term."));
                return AppendEntriesResponse.newBuilder() //
                    .setSuccess(false) //
                    .setTerm(request.getTerm() + 1) //
                    .build();
            }

            // 更新本地记录的最近一次收到来自 Leader 节点请求的时间戳
            // 用于拒绝在一个非对称网络中，Follower与Leader节点失联，但与其它节点正常连接而导致Leader election timeout；
            updateLastLeaderTimestamp(Utils.monotonicMs());

            // 当前是复制日志的请求，但是节点正在执行安装快照；响应错误（防止安装的快照覆盖新的committed）
            if (entriesCount > 0 && this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn("Node {} received AppendEntriesRequest while installing snapshot.", getNodeId());
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(AppendEntriesResponse.getDefaultInstance(), RaftError.EBUSY,
                        "Node %s:%s is installing snapshot.", this.groupId, this.serverId);
            }

            final long prevLogIndex = request.getPrevLogIndex();
            final long prevLogTerm = request.getPrevLogTerm();
            final long localPrevLogTerm = this.logManager.getTerm(prevLogIndex);
            // follower 日志落后于 Leader，
            // 因为此时只有： request.getTerm() == this.currTerm，所以 localPrevLogTerm <= this.currTerm;
            // 若 prevLogIndex > lastLogIndex，
            //     说明localPrevLogTerm=0，RocksDB未把日志刷盘，机器挂了，丢失最近一部分数据；
            // 若 prevLogIndex < lastLogIndex，
            //     说明localPrevLogTerm!=0 && localPrevLogTerm < prevLogTerm，日志属于过期Leader，
            //     原因是长期网络分区，产生了一个Leader，一直在appendEntries，但是quorum不满足，在连接之后此部分日志不允许committed，
            //     需要保证强一致性，每行日志的term&logIndex必须一致；
            if (localPrevLogTerm != prevLogTerm) {
                final long lastLogIndex = this.logManager.getLastLogIndex();

                LOG.warn(
                    "Node {} reject term_unmatched AppendEntriesRequest from {}, term={}, prevLogIndex={}, prevLogTerm={}, localPrevLogTerm={}, lastLogIndex={}, entriesSize={}.",
                    getNodeId(), request.getServerId(), request.getTerm(), prevLogIndex, prevLogTerm, localPrevLogTerm,
                    lastLogIndex, entriesCount);

                return AppendEntriesResponse.newBuilder() //
                    .setSuccess(false) //
                    .setTerm(this.currTerm) //
                    .setLastLogIndex(lastLogIndex) //
                    .build();
            }

            // 心跳或探针请求，返回本地当前的term值以及对应的最新 lastLogIndex；
            if (entriesCount == 0) {
                // heartbeat or probe request
                final AppendEntriesResponse.Builder respBuilder = AppendEntriesResponse.newBuilder() //
                    .setSuccess(true) //
                    .setTerm(this.currTerm) //
                    .setLastLogIndex(this.logManager.getLastLogIndex());
                doUnlock = false;
                this.writeLock.unlock();
                // see the comments at FollowerStableClosure#run()
                // 基于 Leader 的 committedIndex 更新本地的 lastCommittedIndex 值
                // Follower 调用，设置从 Leader收到的已提交的索引 committedIndex
                this.ballotBox.setLastCommittedIndex(Math.min(request.getCommittedIndex(), prevLogIndex));
                return respBuilder.build();
            }

            // Parse request
            /* 复制数据日志请求 - start */
            long index = prevLogIndex;
            final List<LogEntry> entries = new ArrayList<>(entriesCount);
            ByteBuffer allData = null;
            if (request.hasData()) {
                allData = request.getData().asReadOnlyByteBuffer();
            }

            final List<RaftOutter.EntryMeta> entriesList = request.getEntriesList();
            // 遍历逐一解析请求中的 LogEntry 数据，记录到 entries 列表中
            for (int i = 0; i < entriesCount; i++) {
                index++;
                // 获取 LogEntry 元数据信息
                final RaftOutter.EntryMeta entry = entriesList.get(i);
                // 基于元数据和数据体构造 LogEntry 对象
                final LogEntry logEntry = logEntryFromMeta(index, allData, entry);

                if (logEntry != null) {
                    // Validate checksum
                    // 是否启用checksum机制，若启用则checksum，失败则打印日志返回错误；
                    if (this.raftOptions.isEnableLogEntryChecksum() && logEntry.isCorrupted()) {
                        long realChecksum = logEntry.checksum();
                        LOG.error(
                            "Corrupted log entry received from leader, index={}, term={}, expectedChecksum={}, realChecksum={}",
                            logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(),
                            realChecksum);
                        return RpcFactoryHelper //
                            .responseFactory() //
                            .newResponse(AppendEntriesResponse.getDefaultInstance(), RaftError.EINVAL,
                                "The log entry is corrupted, index=%d, term=%d, expectedChecksum=%d, realChecksum=%d",
                                logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(),
                                realChecksum);
                    }
                    entries.add(logEntry);
                }
            }

            final FollowerStableClosure closure = new FollowerStableClosure(request, AppendEntriesResponse.newBuilder()
                .setTerm(this.currTerm), this, done, this.currTerm);
            // Follower 调用 appendEntries，将 LogEntry 数据写入本地磁盘
            this.logManager.appendEntries(entries, closure);
            // update configuration after _log_manager updated its memory status
            checkAndSetConfiguration(true);
            return null;
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
            this.metrics.recordLatency("handle-append-entries", Utils.monotonicMs() - startMs);
            this.metrics.recordSize("handle-append-entries-count", entriesCount);
        }
    }

    private LogEntry logEntryFromMeta(final long index, final ByteBuffer allData, final RaftOutter.EntryMeta entry) {
        // 忽略 ENTRY_TYPE_UNKNOWN 类型的 LogEntry 数据
        if (entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_UNKNOWN) {
            // 给 LogEntry 对象填充基本的元数据信息
            final LogEntry logEntry = new LogEntry();
            logEntry.setId(new LogId(index, entry.getTerm()));
            logEntry.setType(entry.getType());
            if (entry.hasChecksum()) {
                logEntry.setChecksum(entry.getChecksum()); // since 1.2.6
            }
            // 基于元数据中记录的数据长度获取对应的 LogEntry 数据体，并填充到 LogEntry 对象中
            final long dataLen = entry.getDataLen();
            if (dataLen > 0) {
                final byte[] bs = new byte[(int) dataLen];
                assert allData != null;
                allData.get(bs, 0, bs.length);
                logEntry.setData(ByteBuffer.wrap(bs));
            }

            // 针对 ENTRY_TYPE_CONFIGURATION 类型的 LogEntry，解析并填充集群节点配置数据
            if (entry.getPeersCount() > 0) {
                if (entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                    throw new IllegalStateException(
                        "Invalid log entry that contains peers but is not ENTRY_TYPE_CONFIGURATION type: "
                                + entry.getType());
                }

                // 填充集群节点配置信息
                fillLogEntryPeers(entry, logEntry);
            } else if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                throw new IllegalStateException(
                    "Invalid log entry that contains zero peers but is ENTRY_TYPE_CONFIGURATION type");
            }
            return logEntry;
        }
        return null;
    }

    private void fillLogEntryPeers(final RaftOutter.EntryMeta entry, final LogEntry logEntry) {
        // TODO refactor
        if (entry.getPeersCount() > 0) {
            final List<PeerId> peers = new ArrayList<>(entry.getPeersCount());
            for (final String peerStr : entry.getPeersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                peers.add(peer);
            }
            logEntry.setPeers(peers);
        }

        if (entry.getOldPeersCount() > 0) {
            final List<PeerId> oldPeers = new ArrayList<>(entry.getOldPeersCount());
            for (final String peerStr : entry.getOldPeersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                oldPeers.add(peer);
            }
            logEntry.setOldPeers(oldPeers);
        }

        if (entry.getLearnersCount() > 0) {
            final List<PeerId> peers = new ArrayList<>(entry.getLearnersCount());
            for (final String peerStr : entry.getLearnersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                peers.add(peer);
            }
            logEntry.setLearners(peers);
        }

        if (entry.getOldLearnersCount() > 0) {
            final List<PeerId> peers = new ArrayList<>(entry.getOldLearnersCount());
            for (final String peerStr : entry.getOldLearnersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                peers.add(peer);
            }
            logEntry.setOldLearners(peers);
        }
    }

    // called when leader receive greater term in AppendEntriesResponse
    void increaseTermTo(final long newTerm, final Status status) {
        this.writeLock.lock();
        try {
            if (newTerm < this.currTerm) {
                return;
            }
            stepDown(newTerm, false, status);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Peer catch up callback
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-11 2:10:02 PM
     */
    private static class OnCaughtUp extends CatchUpClosure {
        private final NodeImpl node;
        private final long     term;
        private final PeerId   peer;
        private final long     version;

        public OnCaughtUp(final NodeImpl node, final long term, final PeerId peer, final long version) {
            super();
            this.node = node;
            this.term = term;
            this.peer = peer;
            this.version = version;
        }

        @Override
        public void run(final Status status) {
            this.node.onCaughtUp(this.peer, this.term, this.version, status);
        }
    }

    private void onCaughtUp(final PeerId peer, final long term, final long version, final Status st) {
        this.writeLock.lock();
        try {
            // check current_term and state to avoid ABA problem
            if (term != this.currTerm && this.state != State.STATE_LEADER) {
                // term has changed and nothing should be done, otherwise there will be
                // an ABA problem.
                return;
            }
            if (st.isOk()) {
                // Caught up successfully
                this.confCtx.onCaughtUp(version, peer, true);
                return;
            }
            // Retry if this peer is still alive
            if (st.getCode() == RaftError.ETIMEDOUT.getNumber()
                && Utils.monotonicMs() - this.replicatorGroup.getLastRpcSendTimestamp(peer) <= this.options
                    .getElectionTimeoutMs()) {
                LOG.debug("Node {} waits peer {} to catch up.", getNodeId(), peer);
                final OnCaughtUp caughtUp = new OnCaughtUp(this, term, peer, version);
                final long dueTime = Utils.nowMs() + this.options.getElectionTimeoutMs();
                if (this.replicatorGroup.waitCaughtUp(peer, this.options.getCatchupMargin(), dueTime, caughtUp)) {
                    return;
                }
                LOG.warn("Node {} waitCaughtUp failed, peer={}.", getNodeId(), peer);
            }
            LOG.warn("Node {} caughtUp failed, status={}, peer={}.", getNodeId(), st, peer);
            this.confCtx.onCaughtUp(version, peer, false);
        } finally {
            this.writeLock.unlock();
        }
    }

    private boolean checkDeadNodes(final Configuration conf, final long monotonicNowMs,
                                   final boolean stepDownOnCheckFail) {
        // Check learner replicators at first.
        for (final PeerId peer : conf.getLearners()) {
            checkReplicator(peer);
        }
        // Ensure quorum nodes alive.
        final List<PeerId> peers = conf.listPeers();
        final Configuration deadNodes = new Configuration();
        if (checkDeadNodes0(peers, monotonicNowMs, true, deadNodes)) {
            return true;
        }
        if (stepDownOnCheckFail) {
            LOG.warn("Node {} steps down when alive nodes don't satisfy quorum, term={}, deadNodes={}, conf={}.",
                getNodeId(), this.currTerm, deadNodes, conf);
            final Status status = new Status();
            status.setError(RaftError.ERAFTTIMEDOUT, "Majority of the group dies: %d/%d", deadNodes.size(),
                peers.size());
            stepDown(this.currTerm, false, status);
        }
        return false;
    }

    private boolean checkDeadNodes0(final List<PeerId> peers, final long monotonicNowMs, final boolean checkReplicator,
                                    final Configuration deadNodes) {
        final int leaderLeaseTimeoutMs = this.options.getLeaderLeaseTimeoutMs();
        int aliveCount = 0;
        long startLease = Long.MAX_VALUE;
        for (final PeerId peer : peers) {
            if (peer.equals(this.serverId)) {
                aliveCount++;
                continue;
            }
            if (checkReplicator) {
                checkReplicator(peer);
            }
            final long lastRpcSendTimestamp = this.replicatorGroup.getLastRpcSendTimestamp(peer);
            if (monotonicNowMs - lastRpcSendTimestamp <= leaderLeaseTimeoutMs) {
                aliveCount++;
                if (startLease > lastRpcSendTimestamp) {
                    startLease = lastRpcSendTimestamp;
                }
                continue;
            }
            if (deadNodes != null) {
                deadNodes.addPeer(peer);
            }
        }
        if (aliveCount >= peers.size() / 2 + 1) {
            updateLastLeaderTimestamp(startLease);
            return true;
        }
        return false;
    }

    // in read_lock
    private List<PeerId> getAliveNodes(final Collection<PeerId> peers, final long monotonicNowMs) {
        final int leaderLeaseTimeoutMs = this.options.getLeaderLeaseTimeoutMs();
        final List<PeerId> alivePeers = new ArrayList<>();
        for (final PeerId peer : peers) {
            if (peer.equals(this.serverId)) {
                alivePeers.add(peer.copy());
                continue;
            }
            if (monotonicNowMs - this.replicatorGroup.getLastRpcSendTimestamp(peer) <= leaderLeaseTimeoutMs) {
                alivePeers.add(peer.copy());
            }
        }
        return alivePeers;
    }

    @SuppressWarnings({ "LoopStatementThatDoesntLoop", "ConstantConditions" })
    private void handleStepDownTimeout() {
        do {
            this.readLock.lock();
            try {
                if (this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                    LOG.debug("Node {} stop step-down timer, term={}, state={}.", getNodeId(), this.currTerm,
                        this.state);
                    return;
                }
                final long monotonicNowMs = Utils.monotonicMs();
                if (!checkDeadNodes(this.conf.getConf(), monotonicNowMs, false)) {
                    break;
                }
                if (!this.conf.getOldConf().isEmpty()) {
                    if (!checkDeadNodes(this.conf.getOldConf(), monotonicNowMs, false)) {
                        break;
                    }
                }
                return;
            } finally {
                this.readLock.unlock();
            }
        } while (false);

        this.writeLock.lock();
        try {
            if (this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.debug("Node {} stop step-down timer, term={}, state={}.", getNodeId(), this.currTerm, this.state);
                return;
            }
            final long monotonicNowMs = Utils.monotonicMs();
            checkDeadNodes(this.conf.getConf(), monotonicNowMs, true);
            if (!this.conf.getOldConf().isEmpty()) {
                checkDeadNodes(this.conf.getOldConf(), monotonicNowMs, true);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Configuration changed callback.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-11 2:53:43 PM
     */
    private class ConfigurationChangeDone implements Closure {
        private final long    term;
        private final boolean leaderStart;

        public ConfigurationChangeDone(final long term, final boolean leaderStart) {
            super();
            this.term = term;
            this.leaderStart = leaderStart;
        }

        @Override
        public void run(final Status status) {
            if (status.isOk()) {
                onConfigurationChangeDone(this.term);
                if (this.leaderStart) {
                    getOptions().getFsm().onLeaderStart(this.term);
                }
            } else {
                LOG.error("Fail to run ConfigurationChangeDone, status: {}.", status);
            }
        }
    }

    private void unsafeApplyConfiguration(final Configuration newConf, final Configuration oldConf,
                                          final boolean leaderStart) {
        Requires.requireTrue(this.confCtx.isBusy(), "ConfigurationContext is not busy");
        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.setId(new LogId(0, this.currTerm));
        entry.setPeers(newConf.listPeers());
        entry.setLearners(newConf.listLearners());
        if (oldConf != null) {
            entry.setOldPeers(oldConf.listPeers());
            entry.setOldLearners(oldConf.listLearners());
        }
        final ConfigurationChangeDone configurationChangeDone = new ConfigurationChangeDone(this.currTerm, leaderStart);
        // Use the new_conf to deal the quorum of this very log
        if (!this.ballotBox.appendPendingTask(newConf, oldConf, configurationChangeDone)) {
            Utils.runClosureInThread(configurationChangeDone, new Status(RaftError.EINTERNAL, "Fail to append task."));
            return;
        }
        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);
        this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
        checkAndSetConfiguration(false);
    }

    private void unsafeRegisterConfChange(final Configuration oldConf, final Configuration newConf, final Closure done) {

        Requires.requireTrue(newConf.isValid(), "Invalid new conf: %s", newConf);
        // The new conf entry(will be stored in log manager) should be valid
        Requires.requireTrue(new ConfigurationEntry(null, newConf, oldConf).isValid(), "Invalid conf entry: %s",
            newConf);

        if (this.state != State.STATE_LEADER) {
            LOG.warn("Node {} refused configuration changing as the state={}.", getNodeId(), this.state);
            if (done != null) {
                final Status status = new Status();
                if (this.state == State.STATE_TRANSFERRING) {
                    status.setError(RaftError.EBUSY, "Is transferring leadership.");
                } else {
                    status.setError(RaftError.EPERM, "Not leader");
                }
                Utils.runClosureInThread(done, status);
            }
            return;
        }
        // check concurrent conf change
        if (this.confCtx.isBusy()) {
            LOG.warn("Node {} refused configuration concurrent changing.", getNodeId());
            if (done != null) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Doing another configuration change."));
            }
            return;
        }
        // Return immediately when the new peers equals to current configuration
        if (this.conf.getConf().equals(newConf)) {
            Utils.runClosureInThread(done);
            return;
        }
        this.confCtx.start(oldConf, newConf, done);
    }

    private void afterShutdown() {
        List<Closure> savedDoneList = null;
        this.writeLock.lock();
        try {
            if (!this.shutdownContinuations.isEmpty()) {
                savedDoneList = new ArrayList<>(this.shutdownContinuations);
            }
            if (this.logStorage != null) {
                this.logStorage.shutdown();
            }
            this.state = State.STATE_SHUTDOWN;
        } finally {
            this.writeLock.unlock();
        }
        if (savedDoneList != null) {
            for (final Closure closure : savedDoneList) {
                Utils.runClosureInThread(closure);
            }
        }
    }

    @Override
    public NodeOptions getOptions() {
        return this.options;
    }

    public Scheduler getTimerManager() {
        return this.timerManager;
    }

    @Override
    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    @OnlyForTest
    long getCurrentTerm() {
        this.readLock.lock();
        try {
            return this.currTerm;
        } finally {
            this.readLock.unlock();
        }
    }

    @OnlyForTest
    ConfigurationEntry getConf() {
        this.readLock.lock();
        try {
            return this.conf;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        shutdown(null);
    }

    public void onConfigurationChangeDone(final long term) {
        this.writeLock.lock();
        try {
            if (term != this.currTerm || this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.warn("Node {} process onConfigurationChangeDone at term {} while state={}, currTerm={}.",
                    getNodeId(), term, this.state, this.currTerm);
                return;
            }
            this.confCtx.nextStage();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public PeerId getLeaderId() {
        this.readLock.lock();
        try {
            return this.leaderId.isEmpty() ? null : this.leaderId;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public String getGroupId() {
        return this.groupId;
    }

    public PeerId getServerId() {
        return this.serverId;
    }

    @Override
    public NodeId getNodeId() {
        if (this.nodeId == null) {
            this.nodeId = new NodeId(this.groupId, this.serverId);
        }
        return this.nodeId;
    }

    public RaftClientService getRpcService() {
        return this.rpcService;
    }

    public void onError(final RaftException error) {
        LOG.warn("Node {} got error: {}.", getNodeId(), error);
        if (this.fsmCaller != null) {
            // onError of fsmCaller is guaranteed to be executed once.
            this.fsmCaller.onError(error);
        }
        if (this.readOnlyService != null) {
            this.readOnlyService.setError(error);
        }
        this.writeLock.lock();
        try {
            // If it is leader, need to wake up a new one;
            // If it is follower, also step down to call on_stop_following.
            if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                stepDown(this.currTerm, this.state == State.STATE_LEADER, new Status(RaftError.EBADNODE,
                    "Raft node(leader or candidate) is in error."));
            }
            if (this.state.compareTo(State.STATE_ERROR) < 0) {
                this.state = State.STATE_ERROR;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    public void handleRequestVoteResponse(final PeerId peerId, final long term, final RequestVoteResponse response) {
        this.writeLock.lock();
        try {
            if (this.state != State.STATE_CANDIDATE) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {}, state not in STATE_CANDIDATE but {}.",
                    getNodeId(), peerId, this.state);
                return;
            }
            // check stale term
            if (term != this.currTerm) {
                LOG.warn("Node {} received stale RequestVoteResponse from {}, term={}, currTerm={}.", getNodeId(),
                    peerId, term, this.currTerm);
                return;
            }
            // check response term
            if (response.getTerm() > this.currTerm) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {}, term={}, expect={}.", getNodeId(),
                    peerId, response.getTerm(), this.currTerm);
                stepDown(response.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                    "Raft node receives higher term request_vote_response."));
                return;
            }
            // check granted quorum?
            if (response.getGranted()) {
                this.voteCtx.grant(peerId);
                if (this.voteCtx.isGranted()) {
                    becomeLeader();
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private class OnRequestVoteRpcDone extends RpcResponseClosureAdapter<RequestVoteResponse> {

        final long         startMs;
        final PeerId       peer;
        final long         term;
        final NodeImpl     node;
        RequestVoteRequest request;

        public OnRequestVoteRpcDone(final PeerId peer, final long term, final NodeImpl node) {
            super();
            this.startMs = Utils.monotonicMs();
            this.peer = peer;
            this.term = term;
            this.node = node;
        }

        @Override
        public void run(final Status status) {
            NodeImpl.this.metrics.recordLatency("request-vote", Utils.monotonicMs() - this.startMs);
            if (!status.isOk()) {
                LOG.warn("Node {} RequestVote to {} error: {}.", this.node.getNodeId(), this.peer, status);
            } else {
                this.node.handleRequestVoteResponse(this.peer, this.term, getResponse());
            }
        }
    }

    /**
     * [SSS-预选举投票结果处理器]
     */
    public void handlePreVoteResponse(final PeerId peerId, final long term, final RequestVoteResponse response) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // Follower才可以预选举（此时可能已经超过半数选票，预选举已经结束，节点处于Candidate、Leader或节点错误等），否则返回；
            if (this.state != State.STATE_FOLLOWER) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, state not in STATE_FOLLOWER but {}.",
                    getNodeId(), peerId, this.state);
                return;
            }
            // 选举票作废（在预选票发起时的最新term与现在的最新term已不一致，
            // 可能在收到选票结果的时候，已产生新的term（term++），说明已经存在Leader）
            if (term != this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, term={}, currTerm={}.", getNodeId(),
                    peerId, term, this.currTerm);
                return;
            }
            // 请求的任期（term+1）大于自己的最新term，强制Leader下线；
            // term+1，在预选举期间，term++ 不允许存在的原因，
            // 因为可能存在此种情况：某个Follower没有收到Leader的AppendEntries(心跳包)，从而term++，然后Leader被迫下线了
            if (response.getTerm() > this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, term {}, expect={}.", getNodeId(), peerId,
                    response.getTerm(), this.currTerm);
                stepDown(response.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                    "Raft node receives higher term pre_vote_response."));
                return;
            }
            LOG.info("Node {} received PreVoteResponse from {}, term={}, granted={}.", getNodeId(), peerId,
                response.getTerm(), response.getGranted());
            // check granted quorum?
            // 同意选取当前Follower当选Candidate，当投票箱中超过半数的Follower同意时，
            // 则开始第二轮选举，即进行选举自己成为Leader
            if (response.getGranted()) {
                this.prevVoteCtx.grant(peerId);
                if (this.prevVoteCtx.isGranted()) {
                    doUnlock = false;
                    electSelf();
                }
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private class OnPreVoteRpcDone extends RpcResponseClosureAdapter<RequestVoteResponse> {

        final long         startMs;
        final PeerId       peer;
        final long         term;
        RequestVoteRequest request;

        public OnPreVoteRpcDone(final PeerId peer, final long term) {
            super();
            this.startMs = Utils.monotonicMs();
            this.peer = peer;
            this.term = term;
        }

        @Override
        public void run(final Status status) {
            NodeImpl.this.metrics.recordLatency("pre-vote", Utils.monotonicMs() - this.startMs);
            if (!status.isOk()) {
                LOG.warn("Node {} PreVote to {} error: {}.", getNodeId(), this.peer, status);
            } else {
                handlePreVoteResponse(this.peer, this.term, getResponse());
            }
        }
    }

    // in writeLock
    private void preVote() {
        long oldTerm;
        try {
            LOG.info("Node {} term {} start preVote.", getNodeId(), this.currTerm);
            // 若当前正在加载Snapshot，则结束（表明没有最新的日志数据，当前任期内不能够参与Leader选举）
            if (this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn(
                    "Node {} term {} doesn't do preVote when installing snapshot as the configuration may be out of date.",
                    getNodeId(), this.currTerm);
                return;
            }
            // 节点不在配置中，不参与Leader选举
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do preVote as it is not in conf <{}>.", getNodeId(), this.conf);
                return;
            }
            // 获取当前任期
            oldTerm = this.currTerm;
        } finally {
            this.writeLock.unlock();
        }

        // 获取最新的日志ID
        final LogId lastLogId = this.logManager.getLastLogId(true);

        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // pre_vote need defense ABA after unlock&writeLock
            if (oldTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when get lastLogId.", getNodeId(), this.currTerm);
                return;
            }
            // 获取可以参与投票的有效数目（quorum）[Peer.size()/2 + 1]
            this.prevVoteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            for (final PeerId peer : this.conf.listPeers()) {
                if (peer.equals(this.serverId)) {
                    continue;
                }
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, address={}.", getNodeId(), peer.getEndpoint());
                    continue;
                }
                final OnPreVoteRpcDone done = new OnPreVoteRpcDone(peer, this.currTerm);

                // 创建预投票请求，只是将term+1（不允许++，
                // 否则会失去Pre-Vote的意义，在没有多数节点同意（有可能是自己失联）选举Leader的时候自己的term增多，
                // 此时就会抬高自己term，从而强制让Leader step down）
                done.request = RequestVoteRequest.newBuilder() //
                    .setPreVote(true) // it's a pre-vote request.
                    .setGroupId(this.groupId) //
                    .setServerId(this.serverId.toString()) //
                    .setPeerId(peer.toString()) //
                    .setTerm(this.currTerm + 1) // next term
                    .setLastLogIndex(lastLogId.getIndex()) //
                    .setLastLogTerm(lastLogId.getTerm()) //
                    .build();
                // 发送预选举投票，并使用done处理响应---> handlePreVoteRequest(final RequestVoteRequest request)
                this.rpcService.preVote(peer.getEndpoint(), done.request, done);
            }
            // 为自己投选一票
            this.prevVoteCtx.grant(this.serverId);
            // 若是获得大多数（Peers.size() /2 + 1）节点的同意，则进行下一轮选举Leader（通过预选之后，正式选举Leader）
            if (this.prevVoteCtx.isGranted()) {
                doUnlock = false;
                electSelf(); //竞选Leader
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private void handleVoteTimeout() {
        this.writeLock.lock();
        // 非Candidate状态，表明当前Raft集群有Leader或者是Follower身份不参与投票选举
        if (this.state != State.STATE_CANDIDATE) {
            this.writeLock.unlock();
            return;
        }

        // 选举达到超时时间，Candidate退出选举，不发起选举预投票
        if (this.raftOptions.isStepDownWhenVoteTimedout()) {
            LOG.warn(
                "Candidate node {} term {} steps down when election reaching vote timeout: fail to get quorum vote-granted.",
                this.nodeId, this.currTerm);
            stepDown(this.currTerm, false, new Status(RaftError.ETIMEDOUT,
                "Vote timeout: fail to get quorum vote-granted."));
            // unlock in preVote
            preVote();
        } else {
            LOG.debug("Node {} term {} retry to vote self.", getNodeId(), this.currTerm);
            // unlock in electSelf
            electSelf();
        }
    }

    @Override
    public boolean isLeader() {
        return isLeader(true);
    }

    @Override
    public boolean isLeader(final boolean blocking) {
        if (!blocking) {
            return this.state == State.STATE_LEADER;
        }
        this.readLock.lock();
        try {
            return this.state == State.STATE_LEADER;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void shutdown(final Closure done) {
        List<RepeatedTimer> timers = null;
        this.writeLock.lock();
        try {
            LOG.info("Node {} shutdown, currTerm={} state={}.", getNodeId(), this.currTerm, this.state);
            if (this.state.compareTo(State.STATE_SHUTTING) < 0) {
                NodeManager.getInstance().remove(this);
                // If it is leader, set the wakeup_a_candidate with true;
                // If it is follower, call on_stop_following in step_down
                if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                    stepDown(this.currTerm, this.state == State.STATE_LEADER,
                            new Status(RaftError.ESHUTDOWN, "Raft node is going to quit."));
                }
                this.state = State.STATE_SHUTTING;
                // Stop all timers
                timers = stopAllTimers();
                if (this.readOnlyService != null) {
                    this.readOnlyService.shutdown();
                }
                if (this.logManager != null) {
                    this.logManager.shutdown();
                }
                if (this.metaStorage != null) {
                    this.metaStorage.shutdown();
                }
                if (this.snapshotExecutor != null) {
                    this.snapshotExecutor.shutdown();
                }
                if (this.wakingCandidate != null) {
                    Replicator.stop(this.wakingCandidate);
                }
                if (this.fsmCaller != null) {
                    this.fsmCaller.shutdown();
                }
                if (this.rpcService != null) {
                    this.rpcService.shutdown();
                }
                if (this.applyQueue != null) {
                    final CountDownLatch latch = new CountDownLatch(1);
                    this.shutdownLatch = latch;
                    Utils.runInThread(
                        () -> this.applyQueue.publishEvent((event, sequence) -> event.shutdownLatch = latch));
                } else {
                    final int num = GLOBAL_NUM_NODES.decrementAndGet();
                    LOG.info("The number of active nodes decrement to {}.", num);
                }
                if (this.timerManager != null) {
                    this.timerManager.shutdown();
                }
            }

            if (this.state != State.STATE_SHUTDOWN) {
                if (done != null) {
                    this.shutdownContinuations.add(done);
                }
                return;
            }

            // This node is down, it's ok to invoke done right now. Don't invoke this
            // in place to avoid the dead writeLock issue when done.Run() is going to acquire
            // a writeLock which is already held by the caller
            if (done != null) {
                Utils.runClosureInThread(done);
            }
        } finally {
            this.writeLock.unlock();

            // Destroy all timers out of lock
            if (timers != null) {
                destroyAllTimers(timers);
            }
        }
    }

    // Should in lock
    private List<RepeatedTimer> stopAllTimers() {
        final List<RepeatedTimer> timers = new ArrayList<>();
        if (this.electionTimer != null) {
            this.electionTimer.stop();
            timers.add(this.electionTimer);
        }
        if (this.voteTimer != null) {
            this.voteTimer.stop();
            timers.add(this.voteTimer);
        }
        if (this.stepDownTimer != null) {
            this.stepDownTimer.stop();
            timers.add(this.stepDownTimer);
        }
        if (this.snapshotTimer != null) {
            this.snapshotTimer.stop();
            timers.add(this.snapshotTimer);
        }
        return timers;
    }

    private void destroyAllTimers(final List<RepeatedTimer> timers) {
        for (final RepeatedTimer timer : timers) {
            timer.destroy();
        }
    }

    @Override
    public synchronized void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            if (this.readOnlyService != null) {
                this.readOnlyService.join();
            }
            if (this.logManager != null) {
                this.logManager.join();
            }
            if (this.snapshotExecutor != null) {
                this.snapshotExecutor.join();
            }
            if (this.wakingCandidate != null) {
                Replicator.join(this.wakingCandidate);
            }
            this.shutdownLatch.await();
            this.applyDisruptor.shutdown();
            this.shutdownLatch = null;
        }
        if (this.fsmCaller != null) {
            this.fsmCaller.join();
        }
    }

    private static class StopTransferArg {
        final NodeImpl node;
        final long     term;
        final PeerId   peer;

        public StopTransferArg(final NodeImpl node, final long term, final PeerId peer) {
            super();
            this.node = node;
            this.term = term;
            this.peer = peer;
        }
    }

    private void handleTransferTimeout(final long term, final PeerId peer) {
        LOG.info("Node {} failed to transfer leadership to peer {}, reached timeout.", getNodeId(), peer);
        this.writeLock.lock();
        try {
            if (term == this.currTerm) {
                this.replicatorGroup.stopTransferLeadership(peer);
                if (this.state == State.STATE_TRANSFERRING) {
                    this.fsmCaller.onLeaderStart(term);
                    this.state = State.STATE_LEADER;
                    this.stopTransferArg = null;
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private void onTransferTimeout(final StopTransferArg arg) {
        arg.node.handleTransferTimeout(arg.term, arg.peer);
    }

    /**
     * Retrieve current configuration this node seen so far. It's not a reliable way to
     * retrieve cluster peers info, you should use {@link #listPeers()} instead.
     *
     * @return current configuration.
     *
     * @since 1.0.3
     */
    public Configuration getCurrentConf() {
        this.readLock.lock();
        try {
            if (this.conf != null && this.conf.getConf() != null) {
                return this.conf.getConf().copy();
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listPeers() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return this.conf.getConf().listPeers();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listAlivePeers() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return getAliveNodes(this.conf.getConf().getPeers(), Utils.monotonicMs());
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listLearners() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return this.conf.getConf().listLearners();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listAliveLearners() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return getAliveNodes(this.conf.getConf().getLearners(), Utils.monotonicMs());
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void addPeer(final PeerId peer, final Closure done) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            Requires.requireTrue(!this.conf.getConf().contains(peer), "Peer already exists in current configuration");

            final Configuration newConf = new Configuration(this.conf.getConf());
            newConf.addPeer(peer);
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void removePeer(final PeerId peer, final Closure done) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            Requires.requireTrue(this.conf.getConf().contains(peer), "Peer not found in current configuration");

            final Configuration newConf = new Configuration(this.conf.getConf());
            newConf.removePeer(peer);
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void changePeers(final Configuration newPeers, final Closure done) {
        Requires.requireNonNull(newPeers, "Null new peers");
        Requires.requireTrue(!newPeers.isEmpty(), "Empty new peers");
        this.writeLock.lock();
        try {
            LOG.info("Node {} change peers from {} to {}.", getNodeId(), this.conf.getConf(), newPeers);
            unsafeRegisterConfChange(this.conf.getConf(), newPeers, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public Status resetPeers(final Configuration newPeers) {
        Requires.requireNonNull(newPeers, "Null new peers");
        Requires.requireTrue(!newPeers.isEmpty(), "Empty new peers");
        Requires.requireTrue(newPeers.isValid(), "Invalid new peers: %s", newPeers);
        this.writeLock.lock();
        try {
            if (newPeers.isEmpty()) {
                LOG.warn("Node {} set empty peers.", getNodeId());
                return new Status(RaftError.EINVAL, "newPeers is empty");
            }
            if (!this.state.isActive()) {
                LOG.warn("Node {} is in state {}, can't set peers.", getNodeId(), this.state);
                return new Status(RaftError.EPERM, "Bad state: %s", this.state);
            }
            // bootstrap?
            if (this.conf.getConf().isEmpty()) {
                LOG.info("Node {} set peers to {} from empty.", getNodeId(), newPeers);
                this.conf.setConf(newPeers);
                stepDown(this.currTerm + 1, false, new Status(RaftError.ESETPEER, "Set peer from empty configuration"));
                return Status.OK();
            }
            if (this.state == State.STATE_LEADER && this.confCtx.isBusy()) {
                LOG.warn("Node {} set peers need wait current conf changing.", getNodeId());
                return new Status(RaftError.EBUSY, "Changing to another configuration");
            }
            // check equal, maybe retry direct return
            if (this.conf.getConf().equals(newPeers)) {
                return Status.OK();
            }
            final Configuration newConf = new Configuration(newPeers);
            LOG.info("Node {} set peers from {} to {}.", getNodeId(), this.conf.getConf(), newPeers);
            this.conf.setConf(newConf);
            this.conf.getOldConf().reset();
            stepDown(this.currTerm + 1, false, new Status(RaftError.ESETPEER, "Raft node set peer normally"));
            return Status.OK();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void addLearners(final List<PeerId> learners, final Closure done) {
        checkPeers(learners);
        this.writeLock.lock();
        try {
            final Configuration newConf = new Configuration(this.conf.getConf());
            for (final PeerId peer : learners) {
                newConf.addLearner(peer);
            }
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }

    }

    private void checkPeers(final List<PeerId> peers) {
        Requires.requireNonNull(peers, "Null peers");
        Requires.requireTrue(!peers.isEmpty(), "Empty peers");
        for (final PeerId peer : peers) {
            Requires.requireNonNull(peer, "Null peer");
        }
    }

    @Override
    public void removeLearners(final List<PeerId> learners, final Closure done) {
        checkPeers(learners);
        this.writeLock.lock();
        try {
            final Configuration newConf = new Configuration(this.conf.getConf());
            for (final PeerId peer : learners) {
                newConf.removeLearner(peer);
            }
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void resetLearners(final List<PeerId> learners, final Closure done) {
        checkPeers(learners);
        this.writeLock.lock();
        try {
            final Configuration newConf = new Configuration(this.conf.getConf());
            newConf.setLearners(new LinkedHashSet<>(learners));
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 快照：所谓快照就是对数据当前值的一个记录，Leader 生成快照有这么几个作用：
     *      1）当有新的 Node 加入集群的时候，不用只靠日志复制、回放去和 Leader 保持数据一致，
     *      而是通过安装 Leader 的快照来跳过早期大量日志的回放；
     *      2）Leader 用快照替代 Log 复制可以减少网络上的数据量；
     *      3）用快照替代早期的 Log 可以节省存储空间。
     *
     *
     * 除了依靠定时任务触发以外，SOFAJRaft 也支持实现自定义的 Closure 类的回调方法，
     * 通过 Node 接口主动触发 Snapshot，并将结果通过 Closure 回调。
     * 同时，用户在继承并实现业务状态机类“StateMachineAdapter”（该类为抽象类）时候需要，一并实现其中的  onSnapshotSave()/onSnapshotLoad()  方法：
     * onSnapshotSave() 方法：定期保存 Snapshot；
     * onSnapshotLoad() 方法：启动或者安装 Snapshot 后加载 Snapshot；
     * 需要注意的是，上面的  onSnapshotSave()  和  onSnapshotLoad()  方法均会阻塞 Raft 节点本身的状态机，
     * 应该尽量通过异步或其他方式进行优化，避免出现阻塞的情况。
     */
    @Override
    public void snapshot(final Closure done) {
        doSnapshot(done);
    }

    private void doSnapshot(final Closure done) {
        if (this.snapshotExecutor != null) {
            this.snapshotExecutor.doSnapshot(done);
        } else {
            if (done != null) {
                final Status status = new Status(RaftError.EINVAL, "Snapshot is not supported");
                Utils.runClosureInThread(done, status);
            }
        }
    }

    @Override
    public void resetElectionTimeoutMs(final int electionTimeoutMs) {
        Requires.requireTrue(electionTimeoutMs > 0, "Invalid electionTimeoutMs");
        this.writeLock.lock();
        try {
            this.options.setElectionTimeoutMs(electionTimeoutMs);
            this.replicatorGroup.resetHeartbeatInterval(heartbeatTimeout(this.options.getElectionTimeoutMs()));
            this.replicatorGroup.resetElectionTimeoutInterval(electionTimeoutMs);
            LOG.info("Node {} reset electionTimeout, currTimer {} state {} new electionTimeout {}.", getNodeId(),
                this.currTerm, this.state, electionTimeoutMs);
            this.electionTimer.reset(electionTimeoutMs);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public Status transferLeadershipTo(final PeerId peer) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                LOG.warn("Node {} can't transfer leadership to peer {} as it is in state {}.", getNodeId(), peer,
                    this.state);
                return new Status(this.state == State.STATE_TRANSFERRING ? RaftError.EBUSY : RaftError.EPERM,
                        "Not a leader");
            }
            if (this.confCtx.isBusy()) {
                // It's very messy to deal with the case when the |peer| received
                // TimeoutNowRequest and increase the term while somehow another leader
                // which was not replicated with the newest configuration has been
                // elected. If no add_peer with this very |peer| is to be invoked ever
                // after nor this peer is to be killed, this peer will spin in the voting
                // procedure and make the each new leader stepped down when the peer
                // reached vote timeout and it starts to vote (because it will increase
                // the term of the group)
                // To make things simple, refuse the operation and force users to
                // invoke transfer_leadership_to after configuration changing is
                // completed so that the peer's configuration is up-to-date when it
                // receives the TimeOutNowRequest.
                LOG.warn(
                    "Node {} refused to transfer leadership to peer {} when the leader is changing the configuration.",
                    getNodeId(), peer);
                return new Status(RaftError.EBUSY, "Changing the configuration");
            }

            PeerId peerId = peer.copy();
            // if peer_id is ANY_PEER(0.0.0.0:0:0), the peer with the largest
            // last_log_id will be selected.
            if (peerId.equals(PeerId.ANY_PEER)) {
                LOG.info("Node {} starts to transfer leadership to any peer.", getNodeId());
                if ((peerId = this.replicatorGroup.findTheNextCandidate(this.conf)) == null) {
                    return new Status(-1, "Candidate not found for any peer");
                }
            }
            if (peerId.equals(this.serverId)) {
                LOG.info("Node {} transferred leadership to self.", this.serverId);
                return Status.OK();
            }
            if (!this.conf.contains(peerId)) {
                LOG.info("Node {} refused to transfer leadership to peer {} as it is not in {}.", getNodeId(), peer,
                    this.conf);
                return new Status(RaftError.EINVAL, "Not in current configuration");
            }

            final long lastLogIndex = this.logManager.getLastLogIndex();
            if (!this.replicatorGroup.transferLeadershipTo(peerId, lastLogIndex)) {
                LOG.warn("No such peer {}.", peer);
                return new Status(RaftError.EINVAL, "No such peer %s", peer);
            }
            this.state = State.STATE_TRANSFERRING;
            final Status status = new Status(RaftError.ETRANSFERLEADERSHIP,
                "Raft leader is transferring leadership to %s", peerId);
            onLeaderStop(status);
            LOG.info("Node {} starts to transfer leadership to peer {}.", getNodeId(), peer);
            final StopTransferArg stopArg = new StopTransferArg(this, this.currTerm, peerId);
            this.stopTransferArg = stopArg;
            this.transferTimer = this.timerManager.schedule(() -> onTransferTimeout(stopArg),
                this.options.getElectionTimeoutMs(), TimeUnit.MILLISECONDS);

        } finally {
            this.writeLock.unlock();
        }
        return Status.OK();
    }

    private void onLeaderStop(final Status status) {
        this.replicatorGroup.clearFailureReplicators();
        this.fsmCaller.onLeaderStop(status);
    }

    @Override
    public Message handleTimeoutNowRequest(final TimeoutNowRequest request, final RpcRequestClosure done) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (request.getTerm() != this.currTerm) {
                final long savedCurrTerm = this.currTerm;
                if (request.getTerm() > this.currTerm) {
                    stepDown(request.getTerm(), false, new Status(RaftError.EHIGHERTERMREQUEST,
                        "Raft node receives higher term request"));
                }
                LOG.info("Node {} received TimeoutNowRequest from {} while currTerm={} didn't match requestTerm={}.",
                    getNodeId(), request.getPeerId(), savedCurrTerm, request.getTerm());
                return TimeoutNowResponse.newBuilder() //
                    .setTerm(this.currTerm) //
                    .setSuccess(false) //
                    .build();
            }
            if (this.state != State.STATE_FOLLOWER) {
                LOG.info("Node {} received TimeoutNowRequest from {}, while state={}, term={}.", getNodeId(),
                    request.getServerId(), this.state, this.currTerm);
                return TimeoutNowResponse.newBuilder() //
                    .setTerm(this.currTerm) //
                    .setSuccess(false) //
                    .build();
            }

            final long savedTerm = this.currTerm;
            final TimeoutNowResponse resp = TimeoutNowResponse.newBuilder() //
                .setTerm(this.currTerm + 1) //
                .setSuccess(true) //
                .build();
            // Parallelize response and election
            done.sendResponse(resp);
            doUnlock = false;
            electSelf();
            LOG.info("Node {} received TimeoutNowRequest from {}, term={}.", getNodeId(), request.getServerId(),
                savedTerm);
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
        return null;
    }

    @Override
    public Message handleInstallSnapshot(final InstallSnapshotRequest request, final RpcRequestClosure done) {
        if (this.snapshotExecutor == null) {
            return RpcFactoryHelper //
                .responseFactory() //
                .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL, "Not supported snapshot");
        }
        final PeerId serverId = new PeerId();
        if (!serverId.parse(request.getServerId())) {
            LOG.warn("Node {} ignore InstallSnapshotRequest from {} bad server id.", getNodeId(), request.getServerId());
            return RpcFactoryHelper //
                .responseFactory() //
                .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL,
                    "Parse serverId failed: %s", request.getServerId());
        }

        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                LOG.warn("Node {} ignore InstallSnapshotRequest as it is not in active state {}.", getNodeId(),
                    this.state);
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL,
                        "Node %s:%s is not in active state, state %s.", this.groupId, this.serverId, this.state.name());
            }

            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale InstallSnapshotRequest from {}, term={}, currTerm={}.", getNodeId(),
                    request.getPeerId(), request.getTerm(), this.currTerm);
                return InstallSnapshotResponse.newBuilder() //
                    .setTerm(this.currTerm) //
                    .setSuccess(false) //
                    .build();
            }

            checkStepDown(request.getTerm(), serverId);

            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                    serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                stepDown(request.getTerm() + 1, false, new Status(RaftError.ELEADERCONFLICT,
                    "More than one leader in the same term."));
                return InstallSnapshotResponse.newBuilder() //
                    .setTerm(request.getTerm() + 1) //
                    .setSuccess(false) //
                    .build();
            }

        } finally {
            this.writeLock.unlock();
        }
        final long startMs = Utils.monotonicMs();
        try {
            if (LOG.isInfoEnabled()) {
                LOG.info(
                    "Node {} received InstallSnapshotRequest from {}, lastIncludedLogIndex={}, lastIncludedLogTerm={}, lastLogId={}.",
                    getNodeId(), request.getServerId(), request.getMeta().getLastIncludedIndex(), request.getMeta()
                        .getLastIncludedTerm(), this.logManager.getLastLogId(false));
            }
            this.snapshotExecutor.installSnapshot(request, InstallSnapshotResponse.newBuilder(), done);
            return null;
        } finally {
            this.metrics.recordLatency("install-snapshot", Utils.monotonicMs() - startMs);
        }
    }

    public void updateConfigurationAfterInstallingSnapshot() {
        checkAndSetConfiguration(false);
    }

    private void stopReplicator(final Collection<PeerId> keep, final Collection<PeerId> drop) {
        if (drop != null) {
            for (final PeerId peer : drop) {
                if (!keep.contains(peer) && !peer.equals(this.serverId)) {
                    this.replicatorGroup.stopReplicator(peer);
                }
            }
        }
    }

    @Override
    public UserLog readCommittedUserLog(final long index) {
        if (index <= 0) {
            throw new LogIndexOutOfBoundsException("Request index is invalid: " + index);
        }

        final long savedLastAppliedIndex = this.fsmCaller.getLastAppliedIndex();

        if (index > savedLastAppliedIndex) {
            throw new LogIndexOutOfBoundsException("Request index " + index + " is greater than lastAppliedIndex: "
                                                   + savedLastAppliedIndex);
        }

        long curIndex = index;
        LogEntry entry = this.logManager.getEntry(curIndex);
        if (entry == null) {
            throw new LogNotFoundException("User log is deleted at index: " + index);
        }

        do {
            if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_DATA) {
                return new UserLog(curIndex, entry.getData());
            } else {
                curIndex++;
            }
            if (curIndex > savedLastAppliedIndex) {
                throw new IllegalStateException("No user log between index:" + index + " and last_applied_index:"
                                                + savedLastAppliedIndex);
            }
            entry = this.logManager.getEntry(curIndex);
        } while (entry != null);

        throw new LogNotFoundException("User log is deleted at index: " + curIndex);
    }

    @Override
    public void addReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener) {
        Requires.requireNonNull(replicatorStateListener, "replicatorStateListener");
        this.replicatorStateListeners.add(replicatorStateListener);
    }

    @Override
    public void removeReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener) {
        Requires.requireNonNull(replicatorStateListener, "replicatorStateListener");
        this.replicatorStateListeners.remove(replicatorStateListener);
    }

    @Override
    public void clearReplicatorStateListeners() {
        this.replicatorStateListeners.clear();
    }

    @Override
    public List<Replicator.ReplicatorStateListener> getReplicatorStatueListeners() {
        return this.replicatorStateListeners;
    }

    @Override
    public int getNodeTargetPriority() {
        return this.targetPriority;
    }

    @Override
    public void describe(final Printer out) {
        // node
        final String _nodeId;
        final String _state;
        final String _leaderId;
        final long _currTerm;
        final String _conf;
        final int _targetPriority;
        this.readLock.lock();
        try {
            _nodeId = String.valueOf(getNodeId());
            _state = String.valueOf(this.state);
            _leaderId = String.valueOf(this.leaderId);
            _currTerm = this.currTerm;
            _conf = String.valueOf(this.conf);
            _targetPriority = this.targetPriority;
        } finally {
            this.readLock.unlock();
        }
        out.print("nodeId: ") //
            .println(_nodeId);
        out.print("state: ") //
            .println(_state);
        out.print("leaderId: ") //
            .println(_leaderId);
        out.print("term: ") //
            .println(_currTerm);
        out.print("conf: ") //
            .println(_conf);
        out.print("targetPriority: ") //
            .println(_targetPriority);

        // timers
        out.println("electionTimer: ");
        this.electionTimer.describe(out);

        out.println("voteTimer: ");
        this.voteTimer.describe(out);

        out.println("stepDownTimer: ");
        this.stepDownTimer.describe(out);

        out.println("snapshotTimer: ");
        this.snapshotTimer.describe(out);

        // logManager
        out.println("logManager: ");
        this.logManager.describe(out);

        // fsmCaller
        out.println("fsmCaller: ");
        this.fsmCaller.describe(out);

        // ballotBox
        out.println("ballotBox: ");
        this.ballotBox.describe(out);

        // snapshotExecutor
        out.println("snapshotExecutor: ");
        if (this.snapshotExecutor != null) {
            this.snapshotExecutor.describe(out);
        }

        // replicators
        out.println("replicatorGroup: ");
        this.replicatorGroup.describe(out);

        // log storage
        if (this.logStorage instanceof Describer) {
            out.println("logStorage: ");
            ((Describer) this.logStorage).describe(out);
        }
    }

    @Override
    public String toString() {
        return "JRaftNode [nodeId=" + getNodeId() + "]";
    }
}
