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
package com.alipay.sofa.jraft.storage.snapshot;

import com.alipay.sofa.jraft.core.FSMCallerImpl;
import com.alipay.sofa.jraft.core.Replicator;
import com.alipay.sofa.jraft.util.*;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.LoadSnapshotClosure;
import com.alipay.sofa.jraft.closure.SaveSnapshotClosure;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.EnumOutter.ErrorType;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.option.SnapshotExecutorOptions;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.SnapshotExecutor;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;

/**
 * Snapshot executor implementation.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-22 5:38:56 PM
 */
public class SnapshotExecutorImpl implements SnapshotExecutor {

    private static final Logger                        LOG                 = LoggerFactory
                                                                               .getLogger(SnapshotExecutorImpl.class);

    private final Lock                                 lock                = new ReentrantLock();

    /** 记录最近一次快照对应的 term 值，在生成快照成功之后需要更新 */
    private long                                       lastSnapshotTerm;
    /** 记录最近一次快照对应的 logIndex 值，在生成快照成功之后需要更新 */
    private long                                       lastSnapshotIndex;
    private long                                       term;
    private volatile boolean                           savingSnapshot;
    private volatile boolean                           loadingSnapshot;
    private volatile boolean                           stopped;
    private SnapshotStorage                            snapshotStorage;
    private SnapshotCopier                             curCopier;
    private FSMCaller                                  fsmCaller;
    private NodeImpl                                   node;
    private LogManager                                 logManager;
    private final AtomicReference<DownloadingSnapshot> downloadingSnapshot = new AtomicReference<>(null);
    private SnapshotMeta                               loadingSnapshotMeta;
    private final CountDownEvent                       runningJobs         = new CountDownEvent();

    /**
     * Downloading snapshot job.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-08 3:07:19 PM
     */
    static class DownloadingSnapshot {
        InstallSnapshotRequest          request;
        InstallSnapshotResponse.Builder responseBuilder;
        RpcRequestClosure               done;

        public DownloadingSnapshot(final InstallSnapshotRequest request,
                                   final InstallSnapshotResponse.Builder responseBuilder, final RpcRequestClosure done) {
            super();
            this.request = request;
            this.responseBuilder = responseBuilder;
            this.done = done;
        }
    }

    /**
     * Only for test
     */
    @OnlyForTest
    public long getLastSnapshotTerm() {
        return this.lastSnapshotTerm;
    }

    /**
     * Only for test
     */
    @OnlyForTest
    public long getLastSnapshotIndex() {
        return this.lastSnapshotIndex;
    }

    /**
     * Save snapshot done closure
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-08 3:07:52 PM
     */
    private class SaveSnapshotDone implements SaveSnapshotClosure {

        SnapshotWriter writer;
        Closure        done;
        SnapshotMeta   meta;

        public SaveSnapshotDone(final SnapshotWriter writer, final Closure done, final SnapshotMeta meta) {
            super();
            this.writer = writer;
            this.done = done;
            this.meta = meta;
        }

        @Override
        public void run(final Status status) {
            Utils.runInThread(() -> continueRun(status));
        }

        void continueRun(final Status st) {
            final int ret = onSnapshotSaveDone(st, this.meta, this.writer);
            if (ret != 0 && st.isOk()) {
                st.setError(ret, "node call onSnapshotSaveDone failed");
            }
            if (this.done != null) {
                Utils.runClosureInThread(this.done, st);
            }
        }

        @Override
        public SnapshotWriter start(final SnapshotMeta meta) {
            this.meta = meta;
            return this.writer;
        }
    }

    /**
     * Install snapshot done closure
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-08 3:08:09 PM
     */
    private class InstallSnapshotDone implements LoadSnapshotClosure {

        SnapshotReader reader;

        public InstallSnapshotDone(final SnapshotReader reader) {
            super();
            this.reader = reader;
        }

        @Override
        public void run(final Status status) {
            onSnapshotLoadDone(status);
        }

        @Override
        public SnapshotReader start() {
            return this.reader;
        }
    }

    /**
     * Load snapshot at first time closure
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-16 2:57:46 PM
     */
    private class FirstSnapshotLoadDone implements LoadSnapshotClosure {

        SnapshotReader reader;
        CountDownLatch eventLatch;
        Status         status;

        public FirstSnapshotLoadDone(final SnapshotReader reader) {
            super();
            this.reader = reader;
            this.eventLatch = new CountDownLatch(1);
        }

        @Override
        public void run(final Status status) {
            this.status = status;
            onSnapshotLoadDone(this.status);
            this.eventLatch.countDown();
        }

        public void waitForRun() throws InterruptedException {
            this.eventLatch.await();
        }

        @Override
        public SnapshotReader start() {
            return this.reader;
        }

    }

    /**
     * 从本地加载最新的快照文件数据
     */
    @Override
    public boolean init(final SnapshotExecutorOptions opts) {
        if (StringUtils.isBlank(opts.getUri())) {
            LOG.error("Snapshot uri is empty.");
            return false;
        }
        this.logManager = opts.getLogManager();
        this.fsmCaller = opts.getFsmCaller();
        this.node = opts.getNode();
        this.term = opts.getInitTerm();
        // 创建快照存储服务，基于 LocalSnapshotStorage 实现类
        this.snapshotStorage = this.node.getServiceFactory().createSnapshotStorage(opts.getUri(),
            this.node.getRaftOptions());
        if (opts.isFilterBeforeCopyRemote()) {
            this.snapshotStorage.setFilterBeforeCopyRemote();
        }
        if (opts.getSnapshotThrottle() != null) {
            this.snapshotStorage.setSnapshotThrottle(opts.getSnapshotThrottle());
        }
        // 初始化快照存储服务，主要工作是从本地删除除最后一次快照所生成的快照文件之外的其它快照数据文件
        if (!this.snapshotStorage.init(null)) {
            LOG.error("Fail to init snapshot storage.");
            return false;
        }
        final LocalSnapshotStorage tmp = (LocalSnapshotStorage) this.snapshotStorage;
        if (tmp != null && !tmp.hasServerAddr()) {
            tmp.setServerAddr(opts.getAddr());
        }
        // 打开快照文件读取器
        final SnapshotReader reader = this.snapshotStorage.open();
        if (reader == null) {
            return true;
        }
        // 加载快照元数据信息
        this.loadingSnapshotMeta = reader.load();
        if (this.loadingSnapshotMeta == null) {
            LOG.error("Fail to load meta from {}.", opts.getUri());
            Utils.closeQuietly(reader);
            return false;
        }
        LOG.info("Loading snapshot, meta={}.", this.loadingSnapshotMeta);
        this.loadingSnapshot = true;
        this.runningJobs.incrementAndGet();
        final FirstSnapshotLoadDone done = new FirstSnapshotLoadDone(reader);
        // 加载最近一次的快照数据
        Requires.requireTrue(this.fsmCaller.onSnapshotLoad(done));
        try {
            done.waitForRun();
        } catch (final InterruptedException e) {
            LOG.warn("Wait for FirstSnapshotLoadDone run is interrupted.");
            Thread.currentThread().interrupt();
            return false;
        } finally {
            Utils.closeQuietly(reader);
        }
        if (!done.status.isOk()) {
            LOG.error("Fail to load snapshot from {}, FirstSnapshotLoadDone status is {}.", opts.getUri(), done.status);
            return false;
        }
        return true;
    }

    @Override
    public void shutdown() {
        long savedTerm;
        this.lock.lock();
        try {
            savedTerm = this.term;
            this.stopped = true;
        } finally {
            this.lock.unlock();
        }
        interruptDownloadingSnapshots(savedTerm);
    }

    @Override
    public NodeImpl getNode() {
        return this.node;
    }

    /**
     * 如果校验通过则会创建并初始化快照写入器 SnapshotWriter 实例，并向状态机调度器发布一个 SNAPSHOT_SAVE 事件用于异步生成快照文件，
     * 同时会绑定一个 SaveSnapshotDone 回调以感知异步快照生成的状态。
     *
     * 将ApplyTask事件发布至缓冲区（RingBuffer）Disruptor队列进行异步处理，若指定容量不可用，则返回false，构建done失败，否则
     * 源码解读 {@link com.alipay.sofa.jraft.core.FSMCallerImpl.ApplyTaskHandler#onEvent(FSMCallerImpl.ApplyTask, long, boolean)
     *      若Task类型是SNAPSHOT_SAVE，则调用doSnapshotSave方法；
     * }
     * 保存快照：
     * 源码解读 {@link FSMCallerImpl#doSnapshotSave(SaveSnapshotClosure)
     * }
     * 获取快照的状态
     * 源码解读 {@link SaveSnapshotDone#continueRun(Status)
     *      更新已经被快照的 logIndex 和 term 状态值，更新 LogManager 状态
     * }
     *
     */
    @Override
    public void doSnapshot(final Closure done) {
        boolean doUnlock = true;
        this.lock.lock();
        try {
            // SnapshotExecutor 已被停止
            if (this.stopped) {
                Utils.runClosureInThread(done, new Status(RaftError.EPERM, "Is stopped."));
                return;
            }
            // 正在安装快照
            if (this.downloadingSnapshot.get() != null) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Is loading another snapshot."));
                return;
            }

            // 正在生成快照，不允许重复执行
            if (this.savingSnapshot) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Is saving another snapshot."));
                return;
            }

            //  状态机调度器最后应用的 LogEntry 已经被快照，说明没有新的数据可以被快照
            if (this.fsmCaller.getLastAppliedIndex() == this.lastSnapshotIndex) {
                // There might be false positive as the getLastAppliedIndex() is being
                // updated. But it's fine since we will do next snapshot saving in a
                // predictable time.
                doUnlock = false;
                this.lock.unlock();
                this.logManager.clearBufferedLogs();
                Utils.runClosureInThread(done);
                return;
            }

            // 可以被快照的数据量小于阈值，暂不生成快照
            final long distance = this.fsmCaller.getLastAppliedIndex() - this.lastSnapshotIndex;
            if (distance < this.node.getOptions().getSnapshotLogIndexMargin()) {
                // If state machine's lastAppliedIndex value minus lastSnapshotIndex value is
                // less than snapshotLogIndexMargin value, then directly return.
                if (this.node != null) {
                    LOG.debug(
                        "Node {} snapshotLogIndexMargin={}, distance={}, so ignore this time of snapshot by snapshotLogIndexMargin setting.",
                        this.node.getNodeId(), distance, this.node.getOptions().getSnapshotLogIndexMargin());
                }
                doUnlock = false;
                this.lock.unlock();
                Utils.runClosureInThread(done);
                return;
            }

            // 创建并初始化快照写入器，默认使用 LocalSnapshotWriter 实现类
            final SnapshotWriter writer = this.snapshotStorage.create();
            if (writer == null) {
                Utils.runClosureInThread(done, new Status(RaftError.EIO, "Fail to create writer."));
                reportError(RaftError.EIO.getNumber(), "Fail to create snapshot writer.");
                return;
            }
            // 标记当前正在安装快照
            this.savingSnapshot = true;
            // 创建一个回调，用于感知异步快照生成状态
            final SaveSnapshotDone saveSnapshotDone = new SaveSnapshotDone(writer, done, null);
            if (!this.fsmCaller.onSnapshotSave(saveSnapshotDone)) {
                // 设置done，向 Disruptor 队列投递事件失败
                Utils.runClosureInThread(done, new Status(RaftError.EHOSTDOWN, "The raft node is down."));
                return;
            }
            this.runningJobs.incrementAndGet();
        } finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }

    }

    /**
     * 更新已经被快照的 logIndex 和 term 状态值，更新 LogManager 状态
     */
    int onSnapshotSaveDone(final Status st, final SnapshotMeta meta, final SnapshotWriter writer) {
        int ret;
        this.lock.lock();
        try {
            ret = st.getCode();
            // InstallSnapshot can break SaveSnapshot, check InstallSnapshot when SaveSnapshot
            // because upstream Snapshot maybe newer than local Snapshot.
            if (st.isOk()) {
                // 已安装的快照相对于本次生成的快照数据要新
                if (meta.getLastIncludedIndex() <= this.lastSnapshotIndex) {
                    ret = RaftError.ESTALE.getNumber();
                    if (this.node != null) {
                        LOG.warn("Node {} discards an stale snapshot lastIncludedIndex={}, lastSnapshotIndex={}.",
                            this.node.getNodeId(), meta.getLastIncludedIndex(), this.lastSnapshotIndex);
                    }
                    writer.setError(RaftError.ESTALE, "Installing snapshot is older than local snapshot");
                }
            }
        } finally {
            this.lock.unlock();
        }

        // 生成快照成功
        if (ret == 0) {
            // 记录快照元数据信息
            if (!writer.saveMeta(meta)) {
                LOG.warn("Fail to save snapshot {}.", writer.getPath());
                ret = RaftError.EIO.getNumber();
            }
        }
        // 生成快照失败
        else {
            if (writer.isOk()) {
                writer.setError(ret, "Fail to do snapshot.");
            }
        }
        try {
            // 关闭快照写入器
            writer.close();
        } catch (final IOException e) {
            LOG.error("Fail to close writer", e);
            ret = RaftError.EIO.getNumber();
        }
        boolean doUnlock = true;
        this.lock.lock();
        try {
            // 生成快照成功
            if (ret == 0) {
                // 更新最新快照对应的 logIndex 和 term 值
                this.lastSnapshotIndex = meta.getLastIncludedIndex();
                this.lastSnapshotTerm = meta.getLastIncludedTerm();
                doUnlock = false;
                this.lock.unlock();
                // 更新 LogManager 状态，并将本地已快照的日志剔除
                this.logManager.setSnapshot(meta); // should be out of lock
                doUnlock = true;
                this.lock.lock();
            }
            if (ret == RaftError.EIO.getNumber()) {
                reportError(RaftError.EIO.getNumber(), "Fail to save snapshot.");
            }
            // 清除正在生成快照的标记
            this.savingSnapshot = false;
            this.runningJobs.countDown();
            return ret;

        } finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }
    }

    private void onSnapshotLoadDone(final Status st) {
        DownloadingSnapshot m;
        boolean doUnlock = true;
        this.lock.lock();
        try {
            Requires.requireTrue(this.loadingSnapshot, "Not loading snapshot");
            m = this.downloadingSnapshot.get();
            if (st.isOk()) {
                this.lastSnapshotIndex = this.loadingSnapshotMeta.getLastIncludedIndex();
                this.lastSnapshotTerm = this.loadingSnapshotMeta.getLastIncludedTerm();
                doUnlock = false;
                this.lock.unlock();
                this.logManager.setSnapshot(this.loadingSnapshotMeta); // should be out of lock
                doUnlock = true;
                this.lock.lock();
            }
            final StringBuilder sb = new StringBuilder();
            if (this.node != null) {
                sb.append("Node ").append(this.node.getNodeId()).append(" ");
            }
            sb.append("onSnapshotLoadDone, ").append(this.loadingSnapshotMeta);
            LOG.info(sb.toString());
            doUnlock = false;
            this.lock.unlock();
            if (this.node != null) {
                this.node.updateConfigurationAfterInstallingSnapshot();
            }
            doUnlock = true;
            this.lock.lock();
            this.loadingSnapshot = false;
            this.downloadingSnapshot.set(null);

        } finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }
        if (m != null) {
            // Respond RPC
            if (!st.isOk()) {
                m.done.run(st);
            } else {
                m.responseBuilder.setSuccess(true);
                m.done.sendResponse(m.responseBuilder.build());
            }
        }
        this.runningJobs.countDown();
    }

    /**
     * 安装快照文件的执行过程整体可以分为三个步骤：
     * 1) 尝试注册一个下载快照数据的 DownloadingSnapshot 任务；
     * 源码解读 {@link this#registerDownloadingSnapshot(DownloadingSnapshot)
     *      注册一个新的下载快照文件任务时需要考虑可能会与一个正在执行的下载任务相互冲突；
     * }
     * 2) 从 Leader 节点下载快照文件到本地，并阻塞等待文件下载完成；
     * 源码解读 {@link LocalSnapshotStorage#startToCopyFrom(String, SnapshotCopierOptions)
     *      通过RPC将数据从Leader拷贝一份至Follower；
     * }
     * 3) 从本地加载下载回来的快照文件。
     * 源码解读 {@link this#loadDownloadingSnapshot(DownloadingSnapshot, SnapshotMeta)
     *      向Disruptor队列
     *      将快照安装任务以事件的形式投递到RingBuffer中，若Disruptor队列接收到在 onEvent() 中处理
     * }
     * Disruptor 队列处理 源码解读 {@link com.alipay.sofa.jraft.core.FSMCallerImpl.ApplyTaskHandler#onEvent(FSMCallerImpl.ApplyTask, long, boolean)
     *      若Task类型是SNAPSHOT_LOAD，则调用 StateMachine#doSnapshotLoad方法；
     * }
     * 源码解读 {@link com.alipay.sofa.jraft.StateMachine#onSnapshotLoad(SnapshotReader)
     *      将快照数据透传给业务，并由业务决定如何在本地恢复快照所蕴含的状态数据。
     * }
     * SnapshotExecutor 在向 FSMCaller 发布 SNAPSHOT_LOAD 事件时会设置一个 InstallSnapshotDone 回调，
     * 用于感知加载快照数据的状态，如果操作正常则该回调会更新 SnapshotExecutor 本地的状态数据，
     * 包括最新被快照的 LogEntry 对应的 logIndex 和 term 值等。
     * 此外，还会调用 LogManager#setSnapshot 方法对本地已被快照的日志文件执行截断处理，以节省存储空间。
     * 源码解读 {@link com.alipay.sofa.jraft.core.Replicator#onInstallSnapshotReturned(ThreadId, Replicator, Status, InstallSnapshotRequest, InstallSnapshotResponse)
     *      如果给目标 Follower 或 Learner 节点安装快照成功，
     *      则对应的复制器 Replicator 会更新下一个待发送的 LogEntry 索引值 Replicator#nextIndex 字段，
     *      并切换运行状态为 Replicate ，接下去转为向目标 Follower 或 Learner 节点复制日志数据。
     * }
     */
    @Override
    public void installSnapshot(final InstallSnapshotRequest request, final InstallSnapshotResponse.Builder response,
                                final RpcRequestClosure done) {
        // 从请求中获取快照元数据信息
        final SnapshotMeta meta = request.getMeta();
        // 新建一个下载快照的任务
        final DownloadingSnapshot ds = new DownloadingSnapshot(request, response, done);
        // DON'T access request, response, and done after this point
        // as the retry snapshot will replace this one.
        // 尝试注册当前任务，可能存在有其它任务正在运行的情况
        if (!registerDownloadingSnapshot(ds)) {
            LOG.warn("Fail to register downloading snapshot.");
            // This RPC will be responded by the previous session
            return;
        }
        Requires.requireNonNull(this.curCopier, "curCopier");
        try {
            // 等待从 Leader 复制快照数据完成
            this.curCopier.join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Install snapshot copy job was canceled.");
            return;
        }

        // 加载刚刚从 Leader 复制过来的快照数据
        loadDownloadingSnapshot(ds, meta);
    }

    void loadDownloadingSnapshot(final DownloadingSnapshot ds, final SnapshotMeta meta) {
        SnapshotReader reader;
        this.lock.lock();
        try {
            // 当前任务已经失效，说明有新的任务被注册
            if (ds != this.downloadingSnapshot.get()) {
                // It is interrupted and response by other request,just return
                return;
            }
            Requires.requireNonNull(this.curCopier, "curCopier");
            reader = this.curCopier.getReader();
            // 从 leader 节点复制快照数据异常
            if (!this.curCopier.isOk()) {
                if (this.curCopier.getCode() == RaftError.EIO.getNumber()) {
                    reportError(this.curCopier.getCode(), this.curCopier.getErrorMsg());
                }
                Utils.closeQuietly(reader);
                ds.done.run(this.curCopier);
                Utils.closeQuietly(this.curCopier);
                this.curCopier = null;
                this.downloadingSnapshot.set(null);
                this.runningJobs.countDown();
                return;
            }
            Utils.closeQuietly(this.curCopier);
            this.curCopier = null;
            // 快照读取器状态异常
            if (reader == null || !reader.isOk()) {
                Utils.closeQuietly(reader);
                this.downloadingSnapshot.set(null);
                ds.done.sendResponse(RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINTERNAL,
                        "Fail to copy snapshot from %s", ds.request.getUri()));
                this.runningJobs.countDown();
                return;
            }
            // 标记正在加载快照数据
            this.loadingSnapshot = true;
            this.loadingSnapshotMeta = meta;
        } finally {
            this.lock.unlock();
        }
        // 创建一个回调，用于感知异步快照加载状态
        final InstallSnapshotDone installSnapshotDone = new InstallSnapshotDone(reader);
        // 往 Disruptor 队列投递事件失败
        if (!this.fsmCaller.onSnapshotLoad(installSnapshotDone)) {
            LOG.warn("Fail to call fsm onSnapshotLoad.");
            installSnapshotDone.run(new Status(RaftError.EHOSTDOWN, "This raft node is down"));
        }
    }

    /**
     * 注册新的快照文件下载任务的整体执行流程可以概括为：
     *
     * 1) 如果当前 SnapshotExecutor 已被停止，则放弃注册新的任务；
     * 2) 否则，如果当前正在生成快照文件，则放弃注册新的任务；
     * 3) 否则，校验安装快照请求中指定的 term 值是否与当前节点的 term 值相匹配，
     *    如果不匹配则说明请求来源节点已经不再是 LEADER 角色，放弃为本次安装快照请求注册新的任务；
     * 4) 否则，校验本次需要安装的快照数据是否已在本地被快照，如果是则放弃注册新的任务；
     * 5) 否则，尝试为本次安装快照请求注册新的下载快照文件任务，并开始下载快照文件。
     *    可能会遇到当前已有一个正在安装快照的任务在执行的情况，需要决策是让该任务继续执行，还是中断该任务并注册新的任务，具体决策过程如下：
     *    a) 如果当前待注册的任务与正在执行的任务安装的是同一份快照数据，则让正在执行的任务先响应，并标记待注册的任务为正在执行；
     *    b) 否则，如果正在执行的任务安装的快照文件相对于待注册的任务更新，则放弃注册；
     *    c) 否则，说明待注册的任务需要安装的快照文件更新，
     *       如果正在执行的任务已经进入了第三阶段（即已经从远程下载快照文件完成，并开始加载这些快照数据），
     *       则放弃注册，否则需要取消正在执行的任务。
     */
    @SuppressWarnings("all")
    boolean registerDownloadingSnapshot(final DownloadingSnapshot ds) {
        DownloadingSnapshot saved = null;
        boolean result = true;

        this.lock.lock();
        try {
            // SnapshotExecutor 已被停止
            if (this.stopped) {
                LOG.warn("Register DownloadingSnapshot failed: node is stopped.");
                ds.done
                    .sendResponse(RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EHOSTDOWN,
                            "Node is stopped."));
                return false;
            }
            // 正在生成快照
            if (this.savingSnapshot) {
                LOG.warn("Register DownloadingSnapshot failed: is saving snapshot.");
                ds.done.sendResponse(RpcFactoryHelper //
                    .responseFactory().newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EBUSY,
                        "Node is saving snapshot."));
                return false;
            }

            ds.responseBuilder.setTerm(this.term);
            // 安装快照请求中的 term 值与当前节点的 term 值不匹配
            if (ds.request.getTerm() != this.term) {
                LOG.warn("Register DownloadingSnapshot failed: term mismatch, expect {} but {}.", this.term,
                    ds.request.getTerm());
                ds.responseBuilder.setSuccess(false);
                ds.done.sendResponse(ds.responseBuilder.build());
                return false;
            }
            // 需要安装的快照数据已经被快照
            if (ds.request.getMeta().getLastIncludedIndex() <= this.lastSnapshotIndex) {
                LOG.warn(
                    "Register DownloadingSnapshot failed: snapshot is not newer, request lastIncludedIndex={}, lastSnapshotIndex={}.",
                    ds.request.getMeta().getLastIncludedIndex(), this.lastSnapshotIndex);
                ds.responseBuilder.setSuccess(true);
                ds.done.sendResponse(ds.responseBuilder.build());
                return false;
            }
            final DownloadingSnapshot m = this.downloadingSnapshot.get();
            // 表示当前没有正在进行中的安装快照操作
            if (m == null) {
                this.downloadingSnapshot.set(ds);
                Requires.requireTrue(this.curCopier == null, "Current copier is not null");
                // 从指定的 URI 下载快照数据
                this.curCopier = this.snapshotStorage.startToCopyFrom(ds.request.getUri(), newCopierOpts());
                if (this.curCopier == null) {
                    this.downloadingSnapshot.set(null);
                    LOG.warn("Register DownloadingSnapshot failed: fail to copy file from {}.", ds.request.getUri());
                    ds.done.sendResponse(RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL,
                            "Fail to copy from: %s", ds.request.getUri()));
                    return false;
                }
                this.runningJobs.incrementAndGet();
                return true;
            }

            // A previous snapshot is under installing, check if this is the same
            // snapshot and resume it, otherwise drop previous snapshot as this one is
            // newer
            // m 为正在安装快照的任务，ds 为当前任务
            // 当前新注册的任务与正在执行的任务安装的是同一份快照数据
            if (m.request.getMeta().getLastIncludedIndex() == ds.request.getMeta().getLastIncludedIndex()) {
                // m is a retry
                // Copy |*ds| to |*m| so that the former session would respond
                // this RPC.
                saved = m;
                this.downloadingSnapshot.set(ds);
                result = false;
            }
            // 正在执行的安装快照任务操作的数据更新，忽略当前任务
            else if (m.request.getMeta().getLastIncludedIndex() > ds.request.getMeta().getLastIncludedIndex()) {
                // |is| is older
                LOG.warn("Register DownloadingSnapshot failed: is installing a newer one, lastIncludeIndex={}.",
                    m.request.getMeta().getLastIncludedIndex());
                ds.done.sendResponse(RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL,
                        "A newer snapshot is under installing"));
                return false;
            }
            // 当前安装快照任务操作的数据相对于正在执行的任务更新
            else {
                // |is| is newer
                // 正在执行的任务已经进入了 loading 阶段
                if (this.loadingSnapshot) {
                    LOG.warn("Register DownloadingSnapshot failed: is loading an older snapshot, lastIncludeIndex={}.",
                        m.request.getMeta().getLastIncludedIndex());
                    ds.done.sendResponse(RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EBUSY,
                            "A former snapshot is under loading"));
                    return false;
                }
                Requires.requireNonNull(this.curCopier, "curCopier");
                // 停止当前正在执行的任务
                this.curCopier.cancel();
                LOG.warn(
                    "Register DownloadingSnapshot failed: an older snapshot is under installing, cancel downloading, lastIncludeIndex={}.",
                    m.request.getMeta().getLastIncludedIndex());
                ds.done.sendResponse(RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EBUSY,
                        "A former snapshot is under installing, trying to cancel"));
                return false;
            }
        } finally {
            this.lock.unlock();
        }
        if (saved != null) {
            // Respond replaced session
            LOG.warn("Register DownloadingSnapshot failed: interrupted by retry installling request.");
            saved.done.sendResponse(RpcFactoryHelper //
                .responseFactory() //
                .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINTR,
                    "Interrupted by the retry InstallSnapshotRequest"));
        }
        return result;
    }

    private SnapshotCopierOptions newCopierOpts() {
        final SnapshotCopierOptions copierOpts = new SnapshotCopierOptions();
        copierOpts.setNodeOptions(this.node.getOptions());
        copierOpts.setRaftClientService(this.node.getRpcService());
        copierOpts.setTimerManager(this.node.getTimerManager());
        copierOpts.setRaftOptions(this.node.getRaftOptions());
        return copierOpts;
    }

    @Override
    public void interruptDownloadingSnapshots(final long newTerm) {
        this.lock.lock();
        try {
            Requires.requireTrue(newTerm >= this.term);
            this.term = newTerm;
            if (this.downloadingSnapshot.get() == null) {
                return;
            }
            if (this.loadingSnapshot) {
                // We can't interrupt loading
                return;
            }
            Requires.requireNonNull(this.curCopier, "curCopier");
            this.curCopier.cancel();
            LOG.info("Trying to cancel downloading snapshot: {}.", this.downloadingSnapshot.get().request);
        } finally {
            this.lock.unlock();
        }
    }

    private void reportError(final int errCode, final String fmt, final Object... args) {
        final RaftException error = new RaftException(ErrorType.ERROR_TYPE_SNAPSHOT);
        error.setStatus(new Status(errCode, fmt, args));
        this.fsmCaller.onError(error);
    }

    @Override
    public boolean isInstallingSnapshot() {
        return this.downloadingSnapshot.get() != null;
    }

    @Override
    public SnapshotStorage getSnapshotStorage() {
        return this.snapshotStorage;
    }

    @Override
    public void join() throws InterruptedException {
        this.runningJobs.await();
    }

    @Override
    public void describe(final Printer out) {
        final long _lastSnapshotTerm;
        final long _lastSnapshotIndex;
        final long _term;
        final boolean _savingSnapshot;
        final boolean _loadingSnapshot;
        final boolean _stopped;
        this.lock.lock();
        try {
            _lastSnapshotTerm = this.lastSnapshotTerm;
            _lastSnapshotIndex = this.lastSnapshotIndex;
            _term = this.term;
            _savingSnapshot = this.savingSnapshot;
            _loadingSnapshot = this.loadingSnapshot;
            _stopped = this.stopped;
        } finally {
            this.lock.unlock();
        }
        out.print("  lastSnapshotTerm: ") //
            .println(_lastSnapshotTerm);
        out.print("  lastSnapshotIndex: ") //
            .println(_lastSnapshotIndex);
        out.print("  term: ") //
            .println(_term);
        out.print("  savingSnapshot: ") //
            .println(_savingSnapshot);
        out.print("  loadingSnapshot: ") //
            .println(_loadingSnapshot);
        out.print("  stopped: ") //
            .println(_stopped);
    }
}
