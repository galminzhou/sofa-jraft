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
package com.alipay.sofa.jraft.storage.impl;

import com.alipay.sofa.jraft.core.FSMCallerImpl;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.error.LogEntryCorruptedException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.LogManagerOptions;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.ArrayDeque;
import com.alipay.sofa.jraft.util.DisruptorBuilder;
import com.alipay.sofa.jraft.util.DisruptorMetricSet;
import com.alipay.sofa.jraft.util.LogExceptionHandler;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.SegmentList;
import com.alipay.sofa.jraft.util.ThreadHelper;
import com.alipay.sofa.jraft.util.Utils;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * LogManager implementation.
 ----------------------日志复制--------------------------------------------------------
 一旦选出了leader, 它就开始接收客户端请求,
 每个客户端请求都包含一条需要被复制状态机(replicated state machine)执行的命令.
 leader把这条命令作为新的日志条目追加到自己的日志末尾, 然后并行向其他机器发送AppendEntries RPC请求要求复制日志,
 当半数以上机器复制成功后leader将当前条目应用到它的状态机并向客户端回复执行结果,
 如果某个follower崩溃或者网络问题丢包, leader会无限重试AppendEntries RPC(甚至在它已经响应客户端以后)直到所有follower都成功复制了所有日志条目

 ----------------------Raft日志机制的特性--------------------------------------------------------
    如果在不同的日志中的两个条目有着相同的索引和任期号, 那么他们存储的命令肯定是相同的
        a) 源于leader在一个任期里给定的一个日志索引最多创建一条日志条目, 同时该条目在日志中的位置也从不会改变

    如果在不同的日志中的两个条目有着相同的索引和任期号, 那么他们之前的所有日志条目都是完全一样的
        a) 源于AppendEntries RPC的一个简单的一致性检查: 当发送一个AppendEntries RPC时leader会把新日志之前的一个日志条目的索引位置和任期号都包含在里面, follower会检查是否与自己的日志中的索引和任期号是否匹配, 如果不匹配就会拒绝这个日志条目, 接下来就是归纳法来证明了

    Leader通过强制follower复制它的日志来处理日志的不一致
        a) 为了使follower的日志同自己的一致, leader需要找到follower与它日志一致的索引位置并让follower删除该位置之后的条目,
           然后再讲自己再该索引位置之后的条目发送给follower, 这些操作都在AppendEntries RPC进行一致性检查时完成
        b) leader为每一个follower维护了一个nextIndex, 用来表示将要发送给该follower的下一条日志条目索引,
           当一个leader开始掌权时, 会将nextIndex初始化为它的最新日志条目索引值+1,
           如果follower在一致性检查过程中发现自己的日志和leader不一致, 会在这个AppendEntries RPC请求中返回失败,
           leader收到响应之后会将nextIndex递减然后重试,
           最终nextIndex会达到一个leader和follower日志一致的位置，此时AppendEntries RPC 则将成功,
           follower中冲突的日志条目也被移除了, 此时follower和leader的日志就一致了
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 4:42:20 PM
 */
public class LogManagerImpl implements LogManager {
    private static final int                                 APPEND_LOG_RETRY_TIMES = 50;

    private static final Logger                              LOG                    = LoggerFactory
                                                                                        .getLogger(LogManagerImpl.class);

    private LogStorage                                       logStorage;
    private ConfigurationManager                             configManager;
    private FSMCaller                                        fsmCaller;
    private final ReadWriteLock                              lock                   = new ReentrantReadWriteLock();
    private final Lock                                       writeLock              = this.lock.writeLock();
    private final Lock                                       readLock               = this.lock.readLock();
    private volatile boolean                                 stopped;
    private volatile boolean                                 hasError;
    private long                                             nextWaitId;
    private LogId                                            diskId                 = new LogId(0, 0);
    private LogId                                            appliedId              = new LogId(0, 0);
    private final SegmentList<LogEntry>                      logsInMemory           = new SegmentList<>(true);
    private volatile long                                    firstLogIndex;
    private volatile long                                    lastLogIndex;
    private volatile LogId                                   lastSnapshotId         = new LogId(0, 0);
    private final Map<Long, WaitMeta>                        waitMap                = new HashMap<>();
    private Disruptor<StableClosureEvent>                    disruptor;
    private RingBuffer<StableClosureEvent>                   diskQueue;
    private RaftOptions                                      raftOptions;
    private volatile CountDownLatch                          shutDownLatch;
    private NodeMetrics                                      nodeMetrics;
    private final CopyOnWriteArrayList<LastLogIndexListener> lastLogIndexListeners  = new CopyOnWriteArrayList<>();

    private enum EventType {
        OTHER, // other event type.
        RESET, // reset
        TRUNCATE_PREFIX, // truncate log from prefix
        TRUNCATE_SUFFIX, // truncate log from suffix
        SHUTDOWN, //
        LAST_LOG_ID // get last log id
    }

    private static class StableClosureEvent {
        StableClosure done;
        EventType     type;

        void reset() {
            this.done = null;
            this.type = null;
        }
    }

    private static class StableClosureEventFactory implements EventFactory<StableClosureEvent> {

        @Override
        public StableClosureEvent newInstance() {
            return new StableClosureEvent();
        }
    }

    /**
     * Waiter metadata
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 5:05:04 PM
     */
    private static class WaitMeta {
        /** callback when new log come in*/
        NewLogCallback onNewLog;
        /** callback error code*/
        int            errorCode;
        /** the waiter pass-in argument */
        Object         arg;

        public WaitMeta(final NewLogCallback onNewLog, final Object arg, final int errorCode) {
            super();
            this.onNewLog = onNewLog;
            this.arg = arg;
            this.errorCode = errorCode;
        }

    }

    @Override
    public void addLastLogIndexListener(final LastLogIndexListener listener) {
        this.lastLogIndexListeners.add(listener);

    }

    @Override
    public void removeLastLogIndexListener(final LastLogIndexListener listener) {
        this.lastLogIndexListeners.remove(listener);
    }

    @Override
    public boolean init(final LogManagerOptions opts) {
        this.writeLock.lock();
        try {
            if (opts.getLogStorage() == null) {
                LOG.error("Fail to init log manager, log storage is null");
                return false;
            }
            this.raftOptions = opts.getRaftOptions();
            this.nodeMetrics = opts.getNodeMetrics();
            this.logStorage = opts.getLogStorage();
            this.configManager = opts.getConfigurationManager();

            LogStorageOptions lsOpts = new LogStorageOptions();
            lsOpts.setConfigurationManager(this.configManager);
            lsOpts.setLogEntryCodecFactory(opts.getLogEntryCodecFactory());

            if (!this.logStorage.init(lsOpts)) {
                LOG.error("Fail to init logStorage");
                return false;
            }
            this.firstLogIndex = this.logStorage.getFirstLogIndex();
            this.lastLogIndex = this.logStorage.getLastLogIndex();
            this.diskId = new LogId(this.lastLogIndex, getTermFromLogStorage(this.lastLogIndex));
            this.fsmCaller = opts.getFsmCaller();
            this.disruptor = DisruptorBuilder.<StableClosureEvent> newInstance() //
                    .setEventFactory(new StableClosureEventFactory()) //
                    .setRingBufferSize(opts.getDisruptorBufferSize()) //
                    .setThreadFactory(new NamedThreadFactory("JRaft-LogManager-Disruptor-", true)) //
                    .setProducerType(ProducerType.MULTI) //
                    /*
                     *  Use timeout strategy in log manager. If timeout happens, it will called reportError to halt the node.
                     */
                    .setWaitStrategy(new TimeoutBlockingWaitStrategy(
                        this.raftOptions.getDisruptorPublishEventWaitTimeoutSecs(), TimeUnit.SECONDS)) //
                    .build();
            this.disruptor.handleEventsWith(new StableClosureEventHandler());
            this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(this.getClass().getSimpleName(),
                    (event, ex) -> reportError(-1, "LogManager handle event error")));
            this.diskQueue = this.disruptor.start();
            if (this.nodeMetrics.getMetricRegistry() != null) {
                this.nodeMetrics.getMetricRegistry().register("jraft-log-manager-disruptor",
                    new DisruptorMetricSet(this.diskQueue));
            }
        } finally {
            this.writeLock.unlock();
        }
        return true;
    }

    private void stopDiskThread() {
        this.shutDownLatch = new CountDownLatch(1);
        Utils.runInThread(() -> this.diskQueue.publishEvent((event, sequence) -> {
            event.reset();
            event.type = EventType.SHUTDOWN;
        }));
    }

    @Override
    public void join() throws InterruptedException {
        if (this.shutDownLatch == null) {
            return;
        }
        this.shutDownLatch.await();
        this.disruptor.shutdown();
    }

    @Override
    public void shutdown() {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            doUnlock = false;
            wakeupAllWaiter(this.writeLock);
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
        stopDiskThread();
    }

    private void clearMemoryLogs(final LogId id) {
        this.writeLock.lock();
        try {
            this.logsInMemory.removeFromFirstWhen(entry -> entry.getId().compareTo(id) <= 0);
        } finally {
            this.writeLock.unlock();
        }
    }

    private static class LastLogIdClosure extends StableClosure {

        public LastLogIdClosure() {
            super(null);
        }

        private LogId lastLogId;

        void setLastLogId(final LogId logId) {
            Requires.requireTrue(logId.getIndex() == 0 || logId.getTerm() != 0);
            this.lastLogId = logId;
        }

        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run(final Status status) {
            this.latch.countDown();
        }

        void await() throws InterruptedException {
            this.latch.await();
        }

    }

    /**
     * 向节点追加日志数据的操作，除了会被Leader节点执行之外，也会被其它节点（Follower、Learner）执行;
     * 此方法会被所有Raft协议参与者调用执行；
     * 因为Leader节点的更替（election timeout），其中免不了存在的日志文件冲突，
     * 因此需要 LogManagerImpl#checkAndResolveConflict 检查和解决冲突：
     * 源码解读 {@link LogManagerImpl#checkAndResolveConflict(List, StableClosure)
     *      Raft 强一致性：
     *          虽然所有节点的数据并非实时一致性，但是在Raft算法中一定保证Leader节点的数据是最新最完整的；
     *          同时在Raft中所有的请求都由Leader处理，因此在Leader是不会存在数据冲突的；
     *          同时也是Follower节点的标杆，所以在客户端角度看是强一致性的。
     *     List：表示复制日志是批量（默认最大32条）；
     *     对于Follower和Learner节点而已，日志主要分为以下情况：
     *          1）待写入的日志数据与本地已有的日志数据存在断层，此时只能返回错误。
     *          2）待写入的日志数据相对于本地已有的日志数据更老，即最大的 logIndex 小于等于本地已经写入的日志数据的 logIndex，直接忽略。
     *          3）待写入的日志数据与本地已有的日志数据正好衔接上，直接递增 lastLogIndex 即可。
     *          4）待写入的日志数据与本地已有的日志数据存在重叠，此时需要判断是否存在冲突，并强行覆盖本地存在冲突的数据。
     * }
     * Disruptor队列 - StableClosureEvent（EventType.OTHER） - StableClosureEventHandler 接收RingBuffer事件处理，实现异步刷盘；
     * 源码解读 {@link StableClosureEventHandler#onEvent(StableClosureEvent, long, boolean)
     *      StableClosureEventHandler 定义了一个 AppendBatcher 类型的字段，用于缓存待写入的数据。
     * }
     * 调用flush 用于执行将缓存的数据写入存储系统:
     * 源码解读 {@link AppendBatcher#flush()
     *      调用LogManagerImpl#appendToStorage()，将数据写入存储系统的逻辑，默认为RocksDB 存储引擎；
     * }
     * 在完成一个批次的LogEntry的日志数据处理之后，使用done设置回调动作：
     * 源码解读 {@link com.alipay.sofa.jraft.core.NodeImpl.LeaderStableClosure#run(Status)
     *      1) 如果响应状态是OK，则执行 BallotBox#commitAt 方法；
     *      2) 否则，打印ERROR LOG；
     * }
     * 调用commitAt 检查该批次中的日志数据是否被过半数的节点所成功复制，
     * 如果存在复制成功的日志数据，则递增 lastCommittedIndex 值，并向状态机发布 COMMITTED 事件：
     * 源码解读 {@link com.alipay.sofa.jraft.core.BallotBox#commitAt(long, long, PeerId)
     *      1）检查LogEntry 是否已被过半的Raft参与者（Leader和Follower，不包含Learner）复制成功；
     *      2）若已有过半的Raft 参与者复制成功，则向状态机（StateMachine）发布一个 COMMITTED 事件；
     *      3）否则，继续等待 Raft参与者的granted检查。
     * }
     * 已有过半的Raft参与者复制LogEntry成功，向队列发布ApplyTask的COMMITTED类型事件；
     * 源码解读 {@link FSMCaller#onCommitted(long)
     *      向Disruptor队列发布一个ApplyTask的COMMITTED类型的事件；
     * }
     * Disruptor 队列 - 事件处理器接收事件，
     * 源码解读 {@link FSMCallerImpl.ApplyTaskHandler#onEvent(FSMCallerImpl.ApplyTask, long, boolean)
     *      在runApplyTask调用doCommitted方法；
     * }
     * 将LogEntry日志中存储的指令透传给业务状态机，通过doCommitted实现
     * 源码解读 {@link FSMCallerImpl#doCommitted(long)
     * }
     *
     * ---- [SSS-性能优化]------
     * Raft 日志复制：
     *      在Raft算法中，WriteRequest的时候，都是Leader需要先将Log Entry写到本地，
     *      然后再向其他节点（Follower）进行复制，这样WriteRequest的延迟就是IO_Leader + Min(IO_Others)，IO延迟较高；
     *      其实RAFT的模型要求的是一条LogEntry在多数节点上写入成功即可认为是Committed状态，就可以向状态机进行Apply；
     * JRaft日志复制：
     *      将Leader写本地和复制异步进行，只需要在内存中保存未Committed的Log Entry，
     *      在多数节点已经应答的情况下，无需等待Leader本地IO完成，将内存中的Log Entry直接Apply给状态机即可。
     *      即使会造成持久化的Base数据比Log数据新，因为节点启动都是先加载上一个Snapshot再加载其后的Log，对数据一致性也不会造成影响。
     *
     *      对于读取，在Single Client的模型下面，可以将最后写入成功的多数节点列表返回给Client，
     *      这样Client从这几个节点中就可以进行Backup Request了，就可以跳过Leader进行读取了，
     *      Client的读取中带上CommittedId，这样即使Follower节点还没有收到Leader的心跳或者是下一个AppendEntries，
     *      也可以将Log Entry转换为Committed，并Apply到状态机中，随后将Read也发往状态机。
     */
    @Override
    public void appendEntries(final List<LogEntry> entries, final StableClosure done) {
        Requires.requireNonNull(done, "done");
        // 运行期间错误
        if (this.hasError) {
            entries.clear();
            Utils.runClosureInThread(done, new Status(RaftError.EIO, "Corrupted LogStorage"));
            return;
        }
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 若当前节点是Leader：基于本地LastIndex 值设置List<LogEntry>中LogEntry的logIndex
            // 若当前节点是Follower：检查待复制的日志与本地已有的日志是否存在冲突，若存在冲突则强行覆盖本地日志；
            if (!entries.isEmpty() && !checkAndResolveConflict(entries, done)) {
                // If checkAndResolveConflict returns false, the done will be called in it.
                entries.clear();
                return;
            }
            for (int i = 0; i < entries.size(); i++) {
                final LogEntry entry = entries.get(i);
                // Set checksum after checkAndResolveConflict
                if (this.raftOptions.isEnableLogEntryChecksum()) {
                    entry.setChecksum(entry.checksum());
                }
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                    Configuration oldConf = new Configuration();
                    if (entry.getOldPeers() != null) {
                        oldConf = new Configuration(entry.getOldPeers(), entry.getOldLearners());
                    }
                    final ConfigurationEntry conf = new ConfigurationEntry(entry.getId(),
                        new Configuration(entry.getPeers(), entry.getLearners()), oldConf);
                    this.configManager.add(conf);
                }
            }
            // 更新内存数据（将Leader写入日志和Follower写入同时进行，不再等待Leader写入成功之后在通知Replicator复制日志）
            if (!entries.isEmpty()) {
                done.setFirstLogIndex(entries.get(0).getId().getIndex());
                this.logsInMemory.addAll(entries);
            }
            done.setEntries(entries);

            // 将修正之后的LogEntry数据，封装为事件投递给Disruptor队列，事件类型是Other
            int retryTimes = 0;
            final EventTranslator<StableClosureEvent> translator = (event, sequence) -> {
                event.reset();
                event.type = EventType.OTHER;
                event.done = done;
            };
            // 推送事件至Ring Buffer中，若缓冲区（RingBuffer）没有足够空间则失败，最大重试次数50次
            while (true) {
                if (tryOfferEvent(done, translator)) {
                    break;
                } else {
                    retryTimes++;
                    if (retryTimes > APPEND_LOG_RETRY_TIMES) {
                        reportError(RaftError.EBUSY.getNumber(), "LogManager is busy, disk queue overload.");
                        return;
                    }
                    // 每失败一次，就调用Thread.yield()；
                    // 将Thread Running状态 -> Runnable（就绪状态），让出 CPU，在下一个线程执行的时候再次参与竞争CPU；
                    // 向调度程序提示当前线程愿意让出其当前使用的处理器，以改善线程之间的相对进展，否则会过度利用CPU。
                    ThreadHelper.onSpinWait();
                }
            }
            doUnlock = false;
            // 更新内存成功，通知Raft Group中等待新数据到达的复制器 Replicator 实例，进行日志复制；
            if (!wakeupAllWaiter(this.writeLock)) {
                notifyLastLogIndexListeners();
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private void offerEvent(final StableClosure done, final EventType type) {
        if (this.stopped) {
            Utils.runClosureInThread(done, new Status(RaftError.ESTOP, "Log manager is stopped."));
            return;
        }
        if (!this.diskQueue.tryPublishEvent((event, sequence) -> {
            event.reset();
            event.type = type;
            event.done = done;
        })) {
            reportError(RaftError.EBUSY.getNumber(), "Log manager is overload.");
            Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Log manager is overload."));
        }
    }

    private boolean tryOfferEvent(final StableClosure done, final EventTranslator<StableClosureEvent> translator) {
        if (this.stopped) {
            Utils.runClosureInThread(done, new Status(RaftError.ESTOP, "Log manager is stopped."));
            return true;
        }
        return this.diskQueue.tryPublishEvent(translator);
    }

    private void notifyLastLogIndexListeners() {
        for (int i = 0; i < this.lastLogIndexListeners.size(); i++) {
            final LastLogIndexListener listener = this.lastLogIndexListeners.get(i);
            if (listener != null) {
                try {
                    listener.onLastLogIndexChanged(this.lastLogIndex);
                } catch (final Exception e) {
                    LOG.error("Fail to notify LastLogIndexListener, listener={}, index={}", listener, this.lastLogIndex);
                }
            }
        }
    }

    private boolean wakeupAllWaiter(final Lock lock) {
        if (this.waitMap.isEmpty()) {
            lock.unlock();
            return false;
        }
        final List<WaitMeta> wms = new ArrayList<>(this.waitMap.values());
        final int errCode = this.stopped ? RaftError.ESTOP.getNumber() : RaftError.SUCCESS.getNumber();
        this.waitMap.clear();
        lock.unlock();

        final int waiterCount = wms.size();
        for (int i = 0; i < waiterCount; i++) {
            final WaitMeta wm = wms.get(i);
            wm.errorCode = errCode;
            Utils.runInThread(() -> runOnNewLog(wm));
        }
        return true;
    }

    private LogId appendToStorage(final List<LogEntry> toAppend) {
        LogId lastId = null;
        if (!this.hasError) {
            final long startMs = Utils.monotonicMs();
            final int entriesCount = toAppend.size();
            this.nodeMetrics.recordSize("append-logs-count", entriesCount);
            try {
                int writtenSize = 0;
                for (int i = 0; i < entriesCount; i++) {
                    final LogEntry entry = toAppend.get(i);
                    writtenSize += entry.getData() != null ? entry.getData().remaining() : 0;
                }
                this.nodeMetrics.recordSize("append-logs-bytes", writtenSize);
                final int nAppent = this.logStorage.appendEntries(toAppend);
                if (nAppent != entriesCount) {
                    LOG.error("**Critical error**, fail to appendEntries, nAppent={}, toAppend={}", nAppent,
                        toAppend.size());
                    reportError(RaftError.EIO.getNumber(), "Fail to append log entries");
                }
                if (nAppent > 0) {
                    lastId = toAppend.get(nAppent - 1).getId();
                }
                toAppend.clear();
            } finally {
                this.nodeMetrics.recordLatency("append-logs", Utils.monotonicMs() - startMs);
            }
        }
        return lastId;
    }

    private class AppendBatcher {
        List<StableClosure> storage;
        int                 cap;
        int                 size;
        int                 bufferSize;
        List<LogEntry>      toAppend;
        LogId               lastId;

        public AppendBatcher(final List<StableClosure> storage, final int cap, final List<LogEntry> toAppend,
                             final LogId lastId) {
            super();
            this.storage = storage;
            this.cap = cap;
            this.toAppend = toAppend;
            this.lastId = lastId;
        }

        LogId flush() {
            if (this.size > 0) {
                // 将数据落盘并返回最新的LogId
                this.lastId = appendToStorage(this.toAppend);
                for (int i = 0; i < this.size; i++) {
                    // 清空缓存的LogEntry数据
                    this.storage.get(i).getEntries().clear();
                    Status st = null;
                    try {
                        // LogManager IO Exception（存储日志损坏）
                        if (LogManagerImpl.this.hasError) {
                            st = new Status(RaftError.EIO, "Corrupted LogStorage");
                        } else {
                            st = Status.OK();
                        }
                        // 应用回调 LeaderStableClosure#run(Status)
                        this.storage.get(i).run(st);
                    } catch (Throwable t) {
                        LOG.error("Fail to run closure with status: {}.", st, t);
                    }
                }
                this.toAppend.clear();
                this.storage.clear();

            }
            this.size = 0;
            this.bufferSize = 0;
            return this.lastId;
        }

        void append(final StableClosure done) {
            if (this.size == this.cap || this.bufferSize >= LogManagerImpl.this.raftOptions.getMaxAppendBufferSize()) {
                flush();
            }
            this.storage.add(done);
            this.size++;
            this.toAppend.addAll(done.getEntries());
            for (final LogEntry entry : done.getEntries()) {
                this.bufferSize += entry.getData() != null ? entry.getData().remaining() : 0;
            }
        }
    }

    private class StableClosureEventHandler implements EventHandler<StableClosureEvent> {
        LogId               lastId  = LogManagerImpl.this.diskId;
        List<StableClosure> storage = new ArrayList<>(256);
        AppendBatcher       ab      = new AppendBatcher(this.storage, 256, new ArrayList<>(),
                                        LogManagerImpl.this.diskId);

        @Override
        public void onEvent(final StableClosureEvent event, final long sequence, final boolean endOfBatch)
                                                                                                          throws Exception {
            if (event.type == EventType.SHUTDOWN) {
                this.lastId = this.ab.flush();
                setDiskId(this.lastId);
                LogManagerImpl.this.shutDownLatch.countDown();
                return;
            }
            final StableClosure done = event.done;

            if (done.getEntries() != null && !done.getEntries().isEmpty()) {
                this.ab.append(done);
            } else {
                this.lastId = this.ab.flush();
                boolean ret = true;
                switch (event.type) {
                    case LAST_LOG_ID:
                        ((LastLogIdClosure) done).setLastLogId(this.lastId.copy());
                        break;
                    case TRUNCATE_PREFIX:
                        long startMs = Utils.monotonicMs();
                        try {
                            final TruncatePrefixClosure tpc = (TruncatePrefixClosure) done;
                            LOG.debug("Truncating storage to firstIndexKept={}.", tpc.firstIndexKept);
                            ret = LogManagerImpl.this.logStorage.truncatePrefix(tpc.firstIndexKept);
                        } finally {
                            LogManagerImpl.this.nodeMetrics.recordLatency("truncate-log-prefix", Utils.monotonicMs()
                                                                                                 - startMs);
                        }
                        break;
                    case TRUNCATE_SUFFIX:
                        startMs = Utils.monotonicMs();
                        try {
                            final TruncateSuffixClosure tsc = (TruncateSuffixClosure) done;
                            LOG.warn("Truncating storage to lastIndexKept={}.", tsc.lastIndexKept);
                            ret = LogManagerImpl.this.logStorage.truncateSuffix(tsc.lastIndexKept);
                            if (ret) {
                                this.lastId.setIndex(tsc.lastIndexKept);
                                this.lastId.setTerm(tsc.lastTermKept);
                                Requires.requireTrue(this.lastId.getIndex() == 0 || this.lastId.getTerm() != 0);
                            }
                        } finally {
                            LogManagerImpl.this.nodeMetrics.recordLatency("truncate-log-suffix", Utils.monotonicMs()
                                                                                                 - startMs);
                        }
                        break;
                    case RESET:
                        final ResetClosure rc = (ResetClosure) done;
                        LOG.info("Resetting storage to nextLogIndex={}.", rc.nextLogIndex);
                        ret = LogManagerImpl.this.logStorage.reset(rc.nextLogIndex);
                        break;
                    default:
                        break;
                }

                if (!ret) {
                    reportError(RaftError.EIO.getNumber(), "Failed operation in LogStorage");
                } else {
                    done.run(Status.OK());
                }
            }
            if (endOfBatch) {
                this.lastId = this.ab.flush();
                setDiskId(this.lastId);
            }
        }

    }

    private void reportError(final int code, final String fmt, final Object... args) {
        this.hasError = true;
        final RaftException error = new RaftException(ErrorType.ERROR_TYPE_LOG);
        error.setStatus(new Status(code, fmt, args));
        this.fsmCaller.onError(error);
    }

    private void setDiskId(final LogId id) {
        if (id == null) {
            return;
        }
        LogId clearId;
        this.writeLock.lock();
        try {
            if (id.compareTo(this.diskId) < 0) {
                return;
            }
            this.diskId = id;
            clearId = this.diskId.compareTo(this.appliedId) <= 0 ? this.diskId : this.appliedId;
        } finally {
            this.writeLock.unlock();
        }
        if (clearId != null) {
            clearMemoryLogs(clearId);
        }
    }

    @Override
    public void setSnapshot(final SnapshotMeta meta) {
        LOG.debug("set snapshot: {}.", meta);
        this.writeLock.lock();
        try {
            if (meta.getLastIncludedIndex() <= this.lastSnapshotId.getIndex()) {
                return;
            }
            final Configuration conf = confFromMeta(meta);
            final Configuration oldConf = oldConfFromMeta(meta);

            final ConfigurationEntry entry = new ConfigurationEntry(new LogId(meta.getLastIncludedIndex(),
                meta.getLastIncludedTerm()), conf, oldConf);
            this.configManager.setSnapshot(entry);
            final long term = unsafeGetTerm(meta.getLastIncludedIndex());
            final long savedLastSnapshotIndex = this.lastSnapshotId.getIndex();

            this.lastSnapshotId.setIndex(meta.getLastIncludedIndex());
            this.lastSnapshotId.setTerm(meta.getLastIncludedTerm());

            if (this.lastSnapshotId.compareTo(this.appliedId) > 0) {
                this.appliedId = this.lastSnapshotId.copy();
            }
            // NOTICE: not to update disk_id here as we are not sure if this node really
            // has these logs on disk storage. Just leave disk_id as it was, which can keep
            // these logs in memory all the time until they are flushed to disk. By this
            // way we can avoid some corner cases which failed to get logs.
            // See https://github.com/baidu/braft/pull/224/commits/8ef6fdbf70d23f5a4ee147356a889e2c0fa22aac
            //            if (this.lastSnapshotId.compareTo(this.diskId) > 0) {
            //                this.diskId = this.lastSnapshotId.copy();
            //            }

            if (term == 0) {
                // last_included_index is larger than last_index
                // FIXME: what if last_included_index is less than first_index?
                truncatePrefix(meta.getLastIncludedIndex() + 1);
            } else if (term == meta.getLastIncludedTerm()) {
                // Truncating log to the index of the last snapshot.
                // We don't truncate log before the last snapshot immediately since
                // some log around last_snapshot_index is probably needed by some
                // followers
                // TODO if there are still be need?
                if (savedLastSnapshotIndex > 0) {
                    truncatePrefix(savedLastSnapshotIndex + 1);
                }
            } else {
                if (!reset(meta.getLastIncludedIndex() + 1)) {
                    LOG.warn("Reset log manager failed, nextLogIndex={}.", meta.getLastIncludedIndex() + 1);
                }
            }
        } finally {
            this.writeLock.unlock();
        }

    }

    private Configuration oldConfFromMeta(final SnapshotMeta meta) {
        final Configuration oldConf = new Configuration();
        for (int i = 0; i < meta.getOldPeersCount(); i++) {
            final PeerId peer = new PeerId();
            peer.parse(meta.getOldPeers(i));
            oldConf.addPeer(peer);
        }
        for (int i = 0; i < meta.getOldLearnersCount(); i++) {
            final PeerId peer = new PeerId();
            peer.parse(meta.getOldLearners(i));
            oldConf.addLearner(peer);
        }
        return oldConf;
    }

    private Configuration confFromMeta(final SnapshotMeta meta) {
        final Configuration conf = new Configuration();
        for (int i = 0; i < meta.getPeersCount(); i++) {
            final PeerId peer = new PeerId();
            peer.parse(meta.getPeers(i));
            conf.addPeer(peer);
        }
        for (int i = 0; i < meta.getLearnersCount(); i++) {
            final PeerId peer = new PeerId();
            peer.parse(meta.getLearners(i));
            conf.addLearner(peer);
        }
        return conf;
    }

    @Override
    public void clearBufferedLogs() {
        this.writeLock.lock();
        try {
            if (this.lastSnapshotId.getIndex() != 0) {
                truncatePrefix(this.lastSnapshotId.getIndex() + 1);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private String descLogsInMemory() {
        final StringBuilder sb = new StringBuilder();
        boolean wasFirst = true;
        for (int i = 0; i < this.logsInMemory.size(); i++) {
            LogEntry logEntry = this.logsInMemory.get(i);
            if (!wasFirst) {
                sb.append(",");
            } else {
                wasFirst = false;
            }
            sb.append("<id:(").append(logEntry.getId().getTerm()).append(",").append(logEntry.getId().getIndex())
                .append("),type:").append(logEntry.getType()).append(">");
        }
        return sb.toString();
    }

    protected LogEntry getEntryFromMemory(final long index) {
        LogEntry entry = null;
        if (!this.logsInMemory.isEmpty()) {
            final long firstIndex = this.logsInMemory.peekFirst().getId().getIndex();
            final long lastIndex = this.logsInMemory.peekLast().getId().getIndex();
            if (lastIndex - firstIndex + 1 != this.logsInMemory.size()) {
                throw new IllegalStateException(String.format("lastIndex=%d,firstIndex=%d,logsInMemory=[%s]",
                    lastIndex, firstIndex, descLogsInMemory()));
            }
            if (index >= firstIndex && index <= lastIndex) {
                entry = this.logsInMemory.get((int) (index - firstIndex));
            }
        }
        return entry;
    }

    @Override
    public LogEntry getEntry(final long index) {
        this.readLock.lock();
        try {
            if (index > this.lastLogIndex || index < this.firstLogIndex) {
                return null;
            }
            final LogEntry entry = getEntryFromMemory(index);
            if (entry != null) {
                return entry;
            }
        } finally {
            this.readLock.unlock();
        }
        final LogEntry entry = this.logStorage.getEntry(index);
        if (entry == null) {
            reportError(RaftError.EIO.getNumber(), "Corrupted entry at index=%d, not found", index);
        }
        // Validate checksum
        if (entry != null && this.raftOptions.isEnableLogEntryChecksum() && entry.isCorrupted()) {
            String msg = String.format("Corrupted entry at index=%d, term=%d, expectedChecksum=%d, realChecksum=%d",
                index, entry.getId().getTerm(), entry.getChecksum(), entry.checksum());
            // Report error to node and throw exception.
            reportError(RaftError.EIO.getNumber(), msg);
            throw new LogEntryCorruptedException(msg);
        }
        return entry;
    }

    @Override
    public long getTerm(final long index) {
        if (index == 0) {
            return 0;
        }
        this.readLock.lock();
        try {
            // check index equal snapshot_index, return snapshot_term
            if (index == this.lastSnapshotId.getIndex()) {
                return this.lastSnapshotId.getTerm();
            }
            // out of range, direct return 0
            if (index > this.lastLogIndex || index < this.firstLogIndex) {
                return 0;
            }
            final LogEntry entry = getEntryFromMemory(index);
            if (entry != null) {
                return entry.getId().getTerm();
            }
        } finally {
            this.readLock.unlock();
        }
        return getTermFromLogStorage(index);
    }

    private long getTermFromLogStorage(final long index) {
        final LogEntry entry = this.logStorage.getEntry(index);
        if (entry != null) {
            if (this.raftOptions.isEnableLogEntryChecksum() && entry.isCorrupted()) {
                // Report error to node and throw exception.
                final String msg = String.format(
                    "The log entry is corrupted, index=%d, term=%d, expectedChecksum=%d, realChecksum=%d", entry
                        .getId().getIndex(), entry.getId().getTerm(), entry.getChecksum(), entry.checksum());
                reportError(RaftError.EIO.getNumber(), msg);
                throw new LogEntryCorruptedException(msg);
            }

            return entry.getId().getTerm();
        }
        return 0;
    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            return this.firstLogIndex;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        return getLastLogIndex(false);
    }

    @Override
    public long getLastLogIndex(final boolean isFlush) {
        LastLogIdClosure c;
        this.readLock.lock();
        try {
            if (!isFlush) {
                return this.lastLogIndex;
            } else {
                if (this.lastLogIndex == this.lastSnapshotId.getIndex()) {
                    return this.lastLogIndex;
                }
                c = new LastLogIdClosure();
                offerEvent(c, EventType.LAST_LOG_ID);
            }
        } finally {
            this.readLock.unlock();
        }
        try {
            c.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        return c.lastLogId.getIndex();
    }

    private long unsafeGetTerm(final long index) {
        if (index == 0) {
            return 0;
        }

        final LogId lss = this.lastSnapshotId;
        if (index == lss.getIndex()) {
            return lss.getTerm();
        }
        if (index > this.lastLogIndex || index < this.firstLogIndex) {
            return 0;
        }
        final LogEntry entry = getEntryFromMemory(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return getTermFromLogStorage(index);
    }

    @Override
    public LogId getLastLogId(final boolean isFlush) {
        LastLogIdClosure c;
        this.readLock.lock();
        try {
            if (!isFlush) {
                if (this.lastLogIndex >= this.firstLogIndex) {
                    return new LogId(this.lastLogIndex, unsafeGetTerm(this.lastLogIndex));
                }
                return this.lastSnapshotId;
            } else {
                if (this.lastLogIndex == this.lastSnapshotId.getIndex()) {
                    return this.lastSnapshotId;
                }
                c = new LastLogIdClosure();
                offerEvent(c, EventType.LAST_LOG_ID);
            }
        } finally {
            this.readLock.unlock();
        }
        try {
            c.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        return c.lastLogId;
    }

    private static class TruncatePrefixClosure extends StableClosure {
        long firstIndexKept;

        public TruncatePrefixClosure(final long firstIndexKept) {
            super(null);
            this.firstIndexKept = firstIndexKept;
        }

        @Override
        public void run(final Status status) {

        }

    }

    private static class TruncateSuffixClosure extends StableClosure {
        long lastIndexKept;
        long lastTermKept;

        public TruncateSuffixClosure(final long lastIndexKept, final long lastTermKept) {
            super(null);
            this.lastIndexKept = lastIndexKept;
            this.lastTermKept = lastTermKept;
        }

        @Override
        public void run(final Status status) {

        }

    }

    private static class ResetClosure extends StableClosure {
        long nextLogIndex;

        public ResetClosure(final long nextLogIndex) {
            super(null);
            this.nextLogIndex = nextLogIndex;
        }

        @Override
        public void run(final Status status) {

        }
    }

    private boolean truncatePrefix(final long firstIndexKept) {

        this.logsInMemory.removeFromFirstWhen(entry -> entry.getId().getIndex() < firstIndexKept);

        // TODO  maybe it's fine here
        Requires.requireTrue(firstIndexKept >= this.firstLogIndex,
                "Try to truncate logs before %d, but the firstLogIndex is %d", firstIndexKept, this.firstLogIndex);

        this.firstLogIndex = firstIndexKept;
        if (firstIndexKept > this.lastLogIndex) {
            // The entry log is dropped
            this.lastLogIndex = firstIndexKept - 1;
        }
        LOG.debug("Truncate prefix, firstIndexKept is :{}", firstIndexKept);
        this.configManager.truncatePrefix(firstIndexKept);
        final TruncatePrefixClosure c = new TruncatePrefixClosure(firstIndexKept);
        offerEvent(c, EventType.TRUNCATE_PREFIX);
        return true;
    }

    private boolean reset(final long nextLogIndex) {
        this.writeLock.lock();
        try {
            this.logsInMemory.clear();
            this.firstLogIndex = nextLogIndex;
            this.lastLogIndex = nextLogIndex - 1;
            this.configManager.truncatePrefix(this.firstLogIndex);
            this.configManager.truncateSuffix(this.lastLogIndex);
            final ResetClosure c = new ResetClosure(nextLogIndex);
            offerEvent(c, EventType.RESET);
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    private void unsafeTruncateSuffix(final long lastIndexKept) {
        if (lastIndexKept < this.appliedId.getIndex()) {
            LOG.error("FATAL ERROR: Can't truncate logs before appliedId={}, lastIndexKept={}", this.appliedId,
                lastIndexKept);
            return;
        }

        this.logsInMemory.removeFromLastWhen(entry -> entry.getId().getIndex() > lastIndexKept);

        this.lastLogIndex = lastIndexKept;
        final long lastTermKept = unsafeGetTerm(lastIndexKept);
        Requires.requireTrue(this.lastLogIndex == 0 || lastTermKept != 0);
        LOG.debug("Truncate suffix :{}", lastIndexKept);
        this.configManager.truncateSuffix(lastIndexKept);
        final TruncateSuffixClosure c = new TruncateSuffixClosure(lastIndexKept, lastTermKept);
        offerEvent(c, EventType.TRUNCATE_SUFFIX);
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private boolean checkAndResolveConflict(final List<LogEntry> entries, final StableClosure done) {
        final LogEntry firstLogEntry = ArrayDeque.peekFirst(entries);
        // Leader节点，基于本地LastIndex 值设置List<LogEntry>中LogEntry的logIndex
        if (firstLogEntry.getId().getIndex() == 0) {
            // Node is currently the leader and |entries| are from the user who
            // don't know the correct indexes the logs should assign to. So we have
            // to assign indexes to the appending entries
            for (int i = 0; i < entries.size(); i++) {
                entries.get(i).getId().setIndex(++this.lastLogIndex);
            }
            return true;
        } else { // Follower 节点
            // Node is currently a follower and |entries| are from the leader. We
            // should check and resolve the conflicts between the local logs and
            // |entries|
            // 若是follower的lastLogIndex大于当前最新的lastLogIndex+1，则表示传入的日志与本地已有的日志存在断层（非连续日志）
            if (firstLogEntry.getId().getIndex() > this.lastLogIndex + 1) {
                Utils.runClosureInThread(done, new Status(RaftError.EINVAL,
                    "There's gap between first_index=%d and last_log_index=%d", firstLogEntry.getId().getIndex(),
                    this.lastLogIndex));
                return false;
            }
            final long appliedIndex = this.appliedId.getIndex();
            final LogEntry lastLogEntry = ArrayDeque.peekLast(entries);
            //待写入的所有日志的 logIndex 都小于已经应用的日志的最大 logIndex，直接返回
            if (lastLogEntry.getId().getIndex() <= appliedIndex) {
                LOG.warn(
                    "Received entries of which the lastLog={} is not greater than appliedIndex={}, return immediately with nothing changed.",
                    lastLogEntry.getId().getIndex(), appliedIndex);
                // Replicate old logs before appliedIndex should be considered successfully, response OK.
                Utils.runClosureInThread(done);
                return false;
            }
            // 待追加的日志与本地已有的日志之前正好衔接上，直接更新 lastLogIndex
            if (firstLogEntry.getId().getIndex() == this.lastLogIndex + 1) {
                // fast path
                this.lastLogIndex = lastLogEntry.getId().getIndex();
            } else {
            // 待追加的日志与本地已有的日志之间存在重叠（冲突，）
                // Appending entries overlap the local ones. We should find if there
                // is a conflicting index from which we should truncate the local
                // ones.
                int conflictingIndex = 0;
                // 从头开始遍历寻找第一个 term 值不匹配的 logIndex
                for (; conflictingIndex < entries.size(); conflictingIndex++) {
                    if (unsafeGetTerm(entries.get(conflictingIndex).getId().getIndex()) != entries
                        .get(conflictingIndex).getId().getTerm()) {
                        break;
                    }
                }
                // 日志数据存在冲突，将本地冲突之后的日志数据阶段
                if (conflictingIndex != entries.size()) {
                    if (entries.get(conflictingIndex).getId().getIndex() <= this.lastLogIndex) {
                        // Truncate all the conflicting entries to make local logs
                        // consensus with the leader.
                        unsafeTruncateSuffix(entries.get(conflictingIndex).getId().getIndex() - 1);
                    }
                    this.lastLogIndex = lastLogEntry.getId().getIndex();
                } // else this is a duplicated AppendEntriesRequest, we have
                  // nothing to do besides releasing all the entries
                // 将已经写入本地的日志数据从请求中剔除
                if (conflictingIndex > 0) {
                    // Remove duplication
                    entries.subList(0, conflictingIndex).clear();
                }
            }
            return true;
        }
    }

    @Override
    public ConfigurationEntry getConfiguration(final long index) {
        this.readLock.lock();
        try {
            return this.configManager.get(index);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public ConfigurationEntry checkAndSetConfiguration(final ConfigurationEntry current) {
        if (current == null) {
            return null;
        }
        this.readLock.lock();
        try {
            final ConfigurationEntry lastConf = this.configManager.getLastConfiguration();
            if (lastConf != null && !lastConf.isEmpty() && !current.getId().equals(lastConf.getId())) {
                return lastConf;
            }
        } finally {
            this.readLock.unlock();
        }
        return current;
    }

    @Override
    public long wait(final long expectedLastLogIndex, final NewLogCallback cb, final Object arg) {
        final WaitMeta wm = new WaitMeta(cb, arg, 0);
        return notifyOnNewLog(expectedLastLogIndex, wm);
    }

    private long notifyOnNewLog(final long expectedLastLogIndex, final WaitMeta wm) {
        this.writeLock.lock();
        try {
            if (expectedLastLogIndex != this.lastLogIndex || this.stopped) {
                wm.errorCode = this.stopped ? RaftError.ESTOP.getNumber() : 0;
                Utils.runInThread(() -> runOnNewLog(wm));
                return 0L;
            }
            if (this.nextWaitId == 0) { //skip 0
                ++this.nextWaitId;
            }
            final long waitId = this.nextWaitId++;
            this.waitMap.put(waitId, wm);
            return waitId;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean removeWaiter(final long id) {
        this.writeLock.lock();
        try {
            return this.waitMap.remove(id) != null;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void setAppliedId(final LogId appliedId) {
        LogId clearId;
        this.writeLock.lock();
        try {
            if (appliedId.compareTo(this.appliedId) < 0) {
                return;
            }
            this.appliedId = appliedId.copy();
            clearId = this.diskId.compareTo(this.appliedId) <= 0 ? this.diskId : this.appliedId;
        } finally {
            this.writeLock.unlock();
        }
        if (clearId != null) {
            clearMemoryLogs(clearId);
        }
    }

    void runOnNewLog(final WaitMeta wm) {
        wm.onNewLog.onNewLog(wm.arg, wm.errorCode);
    }

    @Override
    public Status checkConsistency() {
        this.readLock.lock();
        try {
            Requires.requireTrue(this.firstLogIndex > 0);
            Requires.requireTrue(this.lastLogIndex >= 0);
            if (this.lastSnapshotId.equals(new LogId(0, 0))) {
                if (this.firstLogIndex == 1) {
                    return Status.OK();
                }
                return new Status(RaftError.EIO, "Missing logs in (0, %d)", this.firstLogIndex);
            } else {
                if (this.lastSnapshotId.getIndex() >= this.firstLogIndex - 1
                    && this.lastSnapshotId.getIndex() <= this.lastLogIndex) {
                    return Status.OK();
                }
                return new Status(RaftError.EIO, "There's a gap between snapshot={%d, %d} and log=[%d, %d] ",
                    this.lastSnapshotId.toString(), this.lastSnapshotId.getTerm(), this.firstLogIndex,
                    this.lastLogIndex);
            }
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void describe(final Printer out) {
        final long _firstLogIndex;
        final long _lastLogIndex;
        final String _diskId;
        final String _appliedId;
        final String _lastSnapshotId;
        this.readLock.lock();
        try {
            _firstLogIndex = this.firstLogIndex;
            _lastLogIndex = this.lastLogIndex;
            _diskId = String.valueOf(this.diskId);
            _appliedId = String.valueOf(this.appliedId);
            _lastSnapshotId = String.valueOf(this.lastSnapshotId);
        } finally {
            this.readLock.unlock();
        }
        out.print("  storage: [") //
            .print(_firstLogIndex) //
            .print(", ") //
            .print(_lastLogIndex) //
            .println(']');
        out.print("  diskId: ") //
            .println(_diskId);
        out.print("  appliedId: ") //
            .println(_appliedId);
        out.print("  lastSnapshotId: ") //
            .println(_lastSnapshotId);
    }
}
