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

import java.util.List;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.option.LogManagerOptions;
import com.alipay.sofa.jraft.util.Describer;

/**
 * Log 存储，记录 raft 用户提交任务的日志，将日志从 leader 复制到其他节点上；
 *
 * LogStorage 是存储实现，默认实现基于 RocksDB 存储，也可以很容易扩展自己的日志存储实现；
 * LogManager 负责对底层存储的调用，对调用做缓存、批量提交、必要的检查和优化；
 *
 * 日志管理器 LogManager 负责调用 Log 日志存储 LogStorage，对 LogStorage 调用进行缓存管理、批量提交、检查优化。
 * Raft 分组节点 Node 初始化/启动时初始化日志存储 StorageFactory 构建日志管理器 LogManager，
 * 基于日志存储 LogStorage、配置管理器 ConfigurationManager、有限状态机调用者 FSMCaller、节点性能监控 NodeMetrics 等
 * LogManagerOptions 配置选项实例化 LogManager。
 * 根据 Raft 节点 Disruptor Buffer 大小配置生成稳定状态回调 StableClosure 事件 Disruptor 队列，
 * 设置稳定状态回调 StableClosure 事件处理器 StableClosureEventHandler 处理队列事件，
 * 其中 StableClosureEventHandler 处理器事件触发的时候判断任务回调 StableClosure 的 Log Entries 是否为空，
 * 如果任务回调的 Log Entries 为非空需积攒日志条目批量 Flush，
 * 空则检查 StableClosureEvent 事件类型并且调用底层存储 LogStorage#appendEntries(entries) 批量提交日志写入 RocksDB，
 * 当事件类型为SHUTDOWN、RESET、TRUNCATE_PREFIX、TRUNCATE_SUFFIX、LAST_LOG_ID 时调用底层日志存储 LogStorage 进行指定事件
 * 回调 ResetClosure、TruncatePrefixClosure、TruncateSuffixClosure、LastLogIdClosure 处理。
 *
 * 当 Client 向 SOFAJRaft 发送命令之后，
 * Raft 分组节点 Node 的日志管理器 LogManager 首先将命令以 Log 的形式存储到本地，
 * 调用 appendEntries(entries, done) 方法检查 Node 节点当前为 Leader
 * 并且 Entries 来源于用户未知分配到的正确日志索引时需要分配索引给添加的日志 Entries ，
 * 而当前为 Follower 时并且 Entries 来源于 Leader 必须检查以及解决本地日志和  Entries 之间的冲突。
 * 接着遍历日志条目 Log Entries 检查类型是否为配置变更，配置管理器 ConfigurationManager 缓存配置变更 Entry，
 * 将现有日志条目 Entries 添加到 logsInMemory 进行缓存，稳定状态回调 StableClosure 设置需要存储的日志，
 * 发布 OTHER 类型事件到稳定状态回调 StableClosure 事件队列，
 * 触发稳定状态回调 StableClosure 事件处理器 StableClosureEventHandler 处理该事件，
 * 处理器获取任务回调的 Log Entries 把日志条目积累到内存中以便后续统一批量 Flush，
 * 通过 appendToStorage(toAppend) 操作调用底层LogStorage 存储日志 Entries。
 * 同时 Replicator 把此条 Log 复制给其他的 Node 实现并发的日志复制，
 * 当 Node 接收集群中半数以上的 Node 返回的“复制成功”的响应将这条 Log 以及之前的 Log 有序的发送至状态机里面执行。
 *
 * Log manager.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 3:02:42 PM
 */
public interface  LogManager extends Lifecycle<LogManagerOptions>, Describer {

    /**
     * Closure to to run in stable state.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 4:35:29 PM
     */
    abstract class StableClosure implements Closure {

        protected long           firstLogIndex = 0;
        protected List<LogEntry> entries;
        protected int            nEntries;

        public StableClosure() {
            // NO-OP
        }

        public long getFirstLogIndex() {
            return this.firstLogIndex;
        }

        public void setFirstLogIndex(final long firstLogIndex) {
            this.firstLogIndex = firstLogIndex;
        }

        public List<LogEntry> getEntries() {
            return this.entries;
        }

        public void setEntries(final List<LogEntry> entries) {
            this.entries = entries;
            if (entries != null) {
                this.nEntries = entries.size();
            } else {
                this.nEntries = 0;
            }
        }

        public StableClosure(final List<LogEntry> entries) {
            super();
            setEntries(entries);
        }

    }

    /**
     * Listen on last log index change event, but it's not reliable,
     * the user should not count on this listener to receive all changed events.
     *
     * @author dennis
     */
    interface LastLogIndexListener {

        /**
         * Called when last log index is changed.
         *
         * @param lastLogIndex last log index
         */
        void onLastLogIndexChanged(final long lastLogIndex);
    }

    /**
     * Adds a last log index listener
     */
    void addLastLogIndexListener(final LastLogIndexListener listener);

    /**
     * Remove the last log index listener.
     */
    void removeLastLogIndexListener(final LastLogIndexListener listener);

    /**
     * Wait the log manager to be shut down.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    void join() throws InterruptedException;

    /**
     * Append log entry vector and wait until it's stable (NOT COMMITTED!)
     *
     * @param entries log entries
     * @param done    callback
     */
    void appendEntries(final List<LogEntry> entries, StableClosure done);

    /**
     * 最近的数据已经被快照，则表示对应的原生日志文件可以从本地存储系统中删除，从而节省存储空间。
     * Notify the log manager about the latest snapshot, which indicates the
     * logs which can be safely truncated.
     *
     * @param meta snapshot metadata
     */
    void setSnapshot(final SnapshotMeta meta);

    /**
     * We don't delete all the logs before last snapshot to avoid installing
     * snapshot on slow replica. Call this method to drop all the logs before
     * last snapshot immediately.
     */
    void clearBufferedLogs();

    /**
     * Get the log entry at index.
     *
     * @param index the index of log entry
     * @return the log entry with {@code index}
     */
    LogEntry getEntry(final long index);

    /**
     * Get the log term at index.
     *
     * @param index the index of log entry
     * @return the term of log entry
     */
    long getTerm(final long index);

    /**
     * Get the first log index of log
     */
    long getFirstLogIndex();

    /**
     * Get the last log index of log
     */
    long getLastLogIndex();

    /**
     * Get the last log index of log
     *
     * @param isFlush whether to flush from disk.
     */
    long getLastLogIndex(final boolean isFlush);

    /**
     * Return the id the last log.
     *
     * @param isFlush whether to flush all pending task.
     */
    LogId getLastLogId(final boolean isFlush);

    /**
     * Get the configuration at index.
     */
    ConfigurationEntry getConfiguration(final long index);

    /**
     * Check if |current| should be updated to the latest configuration
     * Returns the latest configuration, otherwise null.
     */
    ConfigurationEntry checkAndSetConfiguration(final ConfigurationEntry current);

    /**
     * New log notifier callback.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 4:40:04 PM
     */
    interface NewLogCallback {

        /**
         * Called while new log come in.
         *
         * @param arg       the waiter pass-in argument
         * @param errorCode error code
         */
        boolean onNewLog(final Object arg, final int errorCode);
    }

    /**
     * Wait until there are more logs since |last_log_index| and |on_new_log|
     * would be called after there are new logs or error occurs, return the waiter id.
     * 
     * @param expectedLastLogIndex  expected last index of log
     * @param cb                    callback
     * @param arg                   the waiter pass-in argument
     */
    long wait(final long expectedLastLogIndex, final NewLogCallback cb, final Object arg);

    /**
     * Remove a waiter.
     *
     * @param id waiter id
     * @return true on success
     */
    boolean removeWaiter(final long id);

    /**
     * Set the applied id, indicating that the log before applied_id (included)
     * can be dropped from memory logs.
     */
    void setAppliedId(final LogId appliedId);

    /**
     * Check log consistency, returns the status
     * @return status
     */
    Status checkConsistency();

}