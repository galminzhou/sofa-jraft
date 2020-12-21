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
package com.alipay.sofa.jraft.rhea.storage;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.util.Clock;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import com.alipay.sofa.jraft.util.BytesUtil;

/**
 * RaftRawKVStore 是 RheaKV 基于 Raft 复制状态机 KVStoreStateMachine 的 RawKVStore 接口 KV 存储实现，
 * 调用 applyOperation(kvOperation,kvStoreClosure) 方法根据读写请求申请指定 KVOperation 操作，
 * 申请键值操作处理逻辑：
 *      1) 检查当前节点的状态是否为 STATE_LEADER，
 *         如果当前节点不是 Leader 直接失败通知 Done Closure，通知失败(NOT_LEADER)后客户端刷新 Leader 地址并且重试。
 *         Raft 分组 Leader 节点调用 Node#apply(task) 提交申请基于键值操作的任务到相应 Raft Group，向 Raft Group 组成的复制状态机集群提交新任务应用到业务状态机，Raft Log 形成 Majority 后 StateMachine#onApply(iterator) 接口应用到状态机的时候会被获取调用。Node 节点构建申请任务日志封装成事件发布回调，发布节点服务事件到队列 applyQueue，依靠 Disruptor 的 MPSC 模型批量消费，对整体吞吐性能有着极大的提升。日志服务事件处理器以单线程 Batch 攒批的消费方式批量运行键值存储申请任务；
 *      2) Raft 副本节点 Node 执行申请任务检查当前状态是否为 STATE_LEADER，必须保证 Leader 节点操作申请任务。
 *         循环遍历节点服务事件判断任务的预估任期是否等于当前节点任期，
 *         Leader 没有发生变更的阶段内提交的日志拥有相同的 Term 编号，
 *         节点 Node 任期满足预期则 Raft 协议投票箱 BallotBox 调用 appendPendingTask(conf, oldConf, done) 日志复制之前保存应用上下文，
 *         即基于当前节点配置以及原始配置创建选票 Ballot 添加到选票双向队列 pendingMetaQueue；
 *      3) 日志管理器 LogManager 调用底层日志存储 LogStorage#appendEntries(entries) 批量提交申请任务日志写入 RocksDB，
 *         用于 Leader 向 Follower 复制日志包括心跳存活检查等。
 *         日志管理器发布 Leader 稳定状态回调 LeaderStableClosure 事件到队列 diskQueue 即 Disruptor 的 Ring Buffer，
 *         稳定状态回调事件处理器通过MPSC Queue 模型攒批消费触发提交节点选票；
 *      4) 投票箱 BallotBox 调用 commitAt(firstLogIndex, lastLogIndex, peerId) 方法提交当前 PeerId 节点选票到 Raft Group，
 *         更新日志索引在[first_log_index, last_log_index]范畴。
 *         通过 Node#apply(task) 提交的申请任务最终将会复制应用到所有 Raft 节点上的状态机，
 *         RheaKV 状态机通过继承 StateMachineAdapter 状态机适配器的 KVStoreStateMachine 表示；
 *      5) Raft 状态机 KVStoreStateMachine 调用 onApply(iterator) 方法按照提交顺序应用任务列表到状态机。
 *         当 onApply(iterator) 方法返回时认为此批申请任务都已经成功应用到状态机上，
 *         假如没有完全应用(比如错误、异常)将被当做 Critical 级别错误报告给状态机的 onError(raftException) 方法，
 *         错误类型为 ERROR_TYPE_STATE_MACHINE。
 *         Critical 错误导致终止状态机，为什么这里需要终止状态机，非业务逻辑异常的话(比如磁盘满了等 IO 异常)，
 *         代表可能某个节点成功应用到状态机，但是当前节点却应用状态机失败，是不是代表出现不一致的错误？
 *         解决办法只能终止状态机，需要手工介入重启，重启后依靠 Snapshot + Raft log 恢复状态机保证状态机数据的正确性。
 *         提交的任务在 SOFAJRaft 内部用来累积批量提交，应用到状态机的是 Task迭代器，通过 com.alipay.sofa.jraft.Iterator 接口表示；
 *      6) KVStoreStateMachine 状态机迭代状态输出列表积攒键值状态列表
 *         批量申请 RocksRawKVStore 调用 batch(kvStates) 方法运行相应键值操作存储到 RocksDB，为啥 Batch 批量存储呢？
 *         刷盘常用伎俩，攒批刷盘优于多次刷盘。
 *         通过 RecycleUtil 回收器工具回收状态输出列表，
 *         其中KVStateOutputList 是 Pooled ArrayList 实现，RecycleUtil 用于释放列表对象池化复用避免每次创建 List。
 *
 --------------------------------------------------------------------------------------------
 RheaKV 是一个要保证线性一致性的分布式 KV 存储引擎，
 所谓线性一致性，一个简单的例子是在 T1 的时间写入一个值，那么在 T1 之后读一定能读到这个值，不可能读到 T1 之前的值。
 因为 Raft 协议是为了实现分布式环境下面线性一致性的算法，所以通过 Raft 非常方便的实现线性 Read，
 即将任何的读请求走一次 Raft Log，等 Log 日志提交之后在 apply 的时候从状态机里面读取值，一定能够保证此读取到的值是满足线性要求的。

 因为每次 Read 都需要走 Raft 流程，所以性能是非常的低效的，
 SOFAJRaft 实现 Raft 论文提到 ReadIndex 和 Lease Read 优化，
 提供基于 Raft 协议的 ReadIndex 算法的更高效率的线性一致读实现，ReadIndex 省去磁盘的开销，
 结合 SOFAJRaft 的 Batch + Pipeline Ack + 全异步机制大幅度提升吞吐。
 --------------------------------------------------------------------------------------------
 * KVStore based on RAFT replica state machine.
 *
 * @author jiachun.fjc
 */
public class RaftRawKVStore implements RawKVStore {

    private static final Logger LOG = LoggerFactory.getLogger(RaftRawKVStore.class);

    private final Node          node;
    private final RawKVStore    kvStore;
    private final Executor      readIndexExecutor;

    public RaftRawKVStore(Node node, RawKVStore kvStore, Executor readIndexExecutor) {
        this.node = node;
        this.kvStore = kvStore;
        this.readIndexExecutor = readIndexExecutor;
    }

    @Override
    public KVIterator localIterator() {
        return this.kvStore.localIterator();
    }

    @Override
    public void get(final byte[] key, final KVStoreClosure closure) {
        get(key, true, closure);
    }

    /**
     * RaftRawKVStore 接收 get/multiGet/scan/getSequence 读请求
     * 都使用 Node#readIndex(requestContext, readIndexClosure) 发起一次线性一致读请求，
     * 当能够安全读取的时候传入的 ReadIndexClosure 将被调用，
     * 正常情况从状态机中读取数据返回给客户端，
     * readIndex 读取失败尝试应用键值读操作申请任务于 Leader 节点的状态机 KVStoreStateMachine，
     * SOFAJRaft 保证读取的线性一致性。
     *
     * 线性一致读在任何集群内的节点发起，并不需要强制要求放到 Leader 节点上面，将请求散列到集群内的所有节点上，降低 Leader 节点的读取压力。
     * RaftRawKVStore 的 get 读操作发起一次线性一致读请求的调用：
     */
    @Override
    public void get(final byte[] key, final boolean readOnlySafe, final KVStoreClosure closure) {
        if (!readOnlySafe) {
            this.kvStore.get(key, false, closure);
            return;
        }
        // 调用 readIndex 方法，等待回调执行
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    // ReadIndexClosure 回调成功，从 RawKVStore 调用 get 方法读取最新数据返回
                    RaftRawKVStore.this.kvStore.get(key, true, closure);
                    return;
                }
                // 特殊情况譬如发生选举读请求失败，尝试申请 Leader 节点的状态机
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [get] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createGet(key), closure);
                    } else {
                        LOG.warn("Fail to [get] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void multiGet(final List<byte[]> keys, final KVStoreClosure closure) {
        multiGet(keys, true, closure);
    }

    @Override
    public void multiGet(final List<byte[]> keys, final boolean readOnlySafe, final KVStoreClosure closure) {
        if (!readOnlySafe) {
            this.kvStore.multiGet(keys, false, closure);
            return;
        }
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.multiGet(keys, true, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [multiGet] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createMultiGet(keys), closure);
                    } else {
                        LOG.warn("Fail to [multiGet] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void containsKey(final byte[] key, final KVStoreClosure closure) {
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.containsKey(key, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [containsKey] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createContainsKey(key), closure);
                    } else {
                        LOG.warn("Fail to [containsKey] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe, final boolean returnValue,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, returnValue, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final KVStoreClosure closure) {
        scan(startKey, endKey, limit, true, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                     final KVStoreClosure closure) {
        scan(startKey, endKey, limit, readOnlySafe, true, closure);
    }

    @Override
    public void scan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                     final boolean returnValue, final KVStoreClosure closure) {
        if (!readOnlySafe) {
            this.kvStore.scan(startKey, endKey, limit, false, returnValue, closure);
            return;
        }
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.scan(startKey, endKey, limit, true, returnValue, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [scan] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createScan(startKey, endKey, limit, returnValue), closure);
                    } else {
                        LOG.warn("Fail to [scan] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        reverseScan(startKey, endKey, Integer.MAX_VALUE, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                            final KVStoreClosure closure) {
        reverseScan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                            final boolean returnValue, final KVStoreClosure closure) {
        reverseScan(startKey, endKey, Integer.MAX_VALUE, readOnlySafe, returnValue, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final KVStoreClosure closure) {
        reverseScan(startKey, endKey, limit, true, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                            final KVStoreClosure closure) {
        reverseScan(startKey, endKey, limit, readOnlySafe, true, closure);
    }

    @Override
    public void reverseScan(final byte[] startKey, final byte[] endKey, final int limit, final boolean readOnlySafe,
                            final boolean returnValue, final KVStoreClosure closure) {
        if (!readOnlySafe) {
            this.kvStore.reverseScan(startKey, endKey, limit, false, returnValue, closure);
            return;
        }
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.reverseScan(startKey, endKey, limit, true, returnValue, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [reverseScan] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createReverseScan(startKey, endKey, limit, returnValue), closure);
                    } else {
                        LOG.warn("Fail to [reverseScan] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void getSequence(final byte[] seqKey, final int step, final KVStoreClosure closure) {
        if (step > 0) {
            applyOperation(KVOperation.createGetSequence(seqKey, step), closure);
            return;
        }
        // read-only (step==0)
        this.node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {

            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                if (status.isOk()) {
                    RaftRawKVStore.this.kvStore.getSequence(seqKey, 0, closure);
                    return;
                }
                RaftRawKVStore.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.warn("Fail to [getSequence] with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // If 'read index' read fails, try to applying to the state machine at the leader node
                        applyOperation(KVOperation.createGetSequence(seqKey, 0), closure);
                    } else {
                        LOG.warn("Fail to [getSequence] with 'ReadIndex': {}.", status);
                        // Client will retry to leader node
                        new KVClosureAdapter(closure, null).run(status);
                    }
                });
            }
        });
    }

    @Override
    public void resetSequence(final byte[] seqKey, final KVStoreClosure closure) {
        applyOperation(KVOperation.createResetSequence(seqKey), closure);
    }

    @Override
    public void put(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        applyOperation(KVOperation.createPut(key, value), closure);
    }

    @Override
    public void getAndPut(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        applyOperation(KVOperation.createGetAndPut(key, value), closure);
    }

    @Override
    public void compareAndPut(final byte[] key, final byte[] expect, final byte[] update, final KVStoreClosure closure) {
        applyOperation(KVOperation.createCompareAndPut(key, expect, update), closure);
    }

    @Override
    public void merge(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        applyOperation(KVOperation.createMerge(key, value), closure);
    }

    @Override
    public void put(final List<KVEntry> entries, final KVStoreClosure closure) {
        applyOperation(KVOperation.createPutList(entries), closure);
    }

    @Override
    public void putIfAbsent(final byte[] key, final byte[] value, final KVStoreClosure closure) {
        applyOperation(KVOperation.createPutIfAbsent(key, value), closure);
    }

    @Override
    public void tryLockWith(final byte[] key, final byte[] fencingKey, final boolean keepLease,
                            final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        // The algorithm relies on the assumption that while there is no
        // synchronized clock across the processes, still the local time in
        // every process flows approximately at the same rate, with an error
        // which is small compared to the auto-release time of the lock.
        acquirer.setLockingTimestamp(Clock.defaultClock().getTime());
        applyOperation(KVOperation.createKeyLockRequest(key, fencingKey, Pair.of(keepLease, acquirer)), closure);
    }

    @Override
    public void releaseLockWith(final byte[] key, final DistributedLock.Acquirer acquirer, final KVStoreClosure closure) {
        applyOperation(KVOperation.createKeyLockReleaseRequest(key, acquirer), closure);
    }

    @Override
    public void delete(final byte[] key, final KVStoreClosure closure) {
        applyOperation(KVOperation.createDelete(key), closure);
    }

    @Override
    public void deleteRange(final byte[] startKey, final byte[] endKey, final KVStoreClosure closure) {
        applyOperation(KVOperation.createDeleteRange(startKey, endKey), closure);
    }

    @Override
    public void delete(final List<byte[]> keys, final KVStoreClosure closure) {
        applyOperation(KVOperation.createDeleteList(keys), closure);
    }

    @Override
    public void execute(final NodeExecutor nodeExecutor, final boolean isLeader, final KVStoreClosure closure) {
        applyOperation(KVOperation.createNodeExecutor(nodeExecutor), closure);
    }

    private void applyOperation(final KVOperation op, final KVStoreClosure closure) {
        if (!isLeader()) {
            closure.setError(Errors.NOT_LEADER);
            closure.run(new Status(RaftError.EPERM, "Not leader"));
            return;
        }
        final Task task = new Task();
        task.setData(ByteBuffer.wrap(Serializers.getDefault().writeObject(op)));
        task.setDone(new KVClosureAdapter(closure, op));
        this.node.apply(task);
    }

    private boolean isLeader() {
        return this.node.isLeader(false);
    }
}
