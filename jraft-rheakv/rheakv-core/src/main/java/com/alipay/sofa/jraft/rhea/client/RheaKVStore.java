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
package com.alipay.sofa.jraft.rhea.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.FollowerStateListener;
import com.alipay.sofa.jraft.rhea.LeaderStateListener;
import com.alipay.sofa.jraft.rhea.StateListener;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;

/**
 * 最上层 User API，
 * 默认实现为 DefaultRheaKVStore， RheaKVStore 为纯异步实现，
 * 所以通常阻塞调用导致的客户端出现瓶颈，理论上不会在RheaKV上遭遇，
 * DefaultRheaKVStore 实现了包括请求路由、request 分裂、response 聚合以及失败重试等功能。
 *
 * 整体上 rheaKV apis 分为异步和同步两类，
 * 其中以 b （block）开头的方法均为同步阻塞方法，
 * 其他为异步方法，异步方法均返回一个 CompletableFuture，
 * 对于 read method， 还有一个重要参数 readOnlySafe，为 true 时表示提供线性一致读，
 * 不包含该参数的 read method 均为默认提供线性一致读。
 * User layer KV store api.
 *
 * <pre>
 *                           ┌────────────────────────────┐
 *                           │                            │
 *                           │        RheaKVStore         │────────────────┐
 *                           │                            │                │
 *                           └────────────────────────────┘                ▼
 *                                        │ ▲               ┌────────────────────────────┐
 *                                        │ │               │   PlacementDriverClient    │
 *                                        │ │               └────────────────────────────┘
 *                                        │ │                              │
 *                                        │ │                              ▼
 *                                        │ │               ┌────────────────────────────┐
 *                                        │ │               │      RegionRouteTable      │
 *                                        │ │               └────────────────────────────┘
 *                    ┌───────────────────┘ │                              │
 *                    │                     │                              ▼
 *                    │                     │               ┌────────────────────────────┐
 *                    │                     └───────────────│        LoadBalancer        │
 *                  split                                   └────────────────────────────┘
 *                 request                             local
 *                    ├────────────────────────────────invoke──────────────────────────────────────────┐
 *                    │                                                                                │
 *                    ▼                                                                                │
 *     ┌────────────────────────────┐           ┌────────────────────────────┐                         │
 *     │      RheaKVRpcService      │───rpc────▶│     KVCommandProcessor     │                         │
 *     └────────────────────────────┘           └────────────────────────────┘                         │
 *                                                             │                                       │
 *                                                             ▼                                       ▼
 *                                              ┌────────────────────────────┐          ┌────────────────────────────┐
 *                                              │      RegionKVService       │────────▶ │     MetricsRawKVStore      │
 *                                              └────────────────────────────┘          └────────────────────────────┘
 *                                                                                                     │
 *                                                                                                     │
 *                                                                                                     ▼
 *     ┌────────────────────────────┐           ┌────────────────────────────┐          ┌────────────────────────────┐
 *     │      RocksRawKVStore       │◀──────────│    KVStoreStateMachine     │◀──raft───│       RaftRawKVStore       │
 *     └────────────────────────────┘           └────────────────────────────┘          └────────────────────────────┘
 * </pre>
 *
 * @author jiachun.fjc
 */
public interface RheaKVStore extends Lifecycle<RheaKVStoreOptions> {

    /**
     * Equivalent to {@code get(key, true)}.
     */
    CompletableFuture<byte[]> get(final byte[] key);

    /**
     * @see #get(byte[])
     */
    CompletableFuture<byte[]> get(final String key);

    /**
     * get/bGet
     *     1.String 类型入参，rheaKV 内部提供了更高效的 Utf8String encoder/decoder，
     *       业务 key 为 String 时， 推荐的做法是直接使用 String 参数的接口；
     *     2.不需要线性一致读语义的场景可以将 readOnlySafe 设置为 false，
     *       负载均衡器会优先选择本地调用，本地不能提供服务则轮询选择一台远程机器发起读请求；
     *
     * Get which returns a new byte array storing the value associated
     * with the specified input key if any.  null will be returned if
     * the specified key is not found.
     *
     * @param key          the key retrieve the value.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @return a byte array storing the value associated with the input key if
     * any.  null if it does not find the specified key.
     */
    CompletableFuture<byte[]> get(final byte[] key, final boolean readOnlySafe);

    /**
     * @see #get(byte[], boolean)
     */
    CompletableFuture<byte[]> get(final String key, final boolean readOnlySafe);

    /**
     * @see #get(byte[])
     */
    byte[] bGet(final byte[] key);

    /**
     * @see #get(String)
     */
    byte[] bGet(final String key);

    /**
     * @see #get(byte[], boolean)
     */
    byte[] bGet(final byte[] key, final boolean readOnlySafe);

    /**
     * @see #get(String, boolean)
     */
    byte[] bGet(final String key, final boolean readOnlySafe);

    /**
     * Equivalent to {@code multiGet(keys, true)}.
     */
    CompletableFuture<Map<ByteArray, byte[]>> multiGet(final List<byte[]> keys);

    /**
     *  multiGet/bMultiGet
     *     1.multiGet 支持跨分区查询，rheaKV 内部会自动计算每个 key 的所属分区（region）并行发起调用， 最后合并查询结果；
     *     2.为了可以将 byte[] 放进 HashMap，这里曲线救国，
     *     返回值中 Map 的 key 为 ByteArray 对象，是对 byte[] 的一层包装，实现了 byte[] 的 hashCode
     *
     * Returns a map of keys for which values were found in database.
     *
     * @param keys         list of keys for which values need to be retrieved.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @return a map where key of map is the key passed by user and value for map
     * entry is the corresponding value in database.
     */
    CompletableFuture<Map<ByteArray, byte[]>> multiGet(final List<byte[]> keys, final boolean readOnlySafe);

    /**
     * @see #multiGet(List)
     */
    Map<ByteArray, byte[]> bMultiGet(final List<byte[]> keys);

    /**
     * @see #multiGet(List, boolean)
     */
    Map<ByteArray, byte[]> bMultiGet(final List<byte[]> keys, final boolean readOnlySafe);

    /**
     * Returns whether database contains the specified input key.
     *
     * @param key the specified key database contains.
     * @return whether database contains the specified key.
     */
    CompletableFuture<Boolean> containsKey(final byte[] key);

    /**
     * @see #containsKey(byte[])
     */
    CompletableFuture<Boolean> containsKey(final String key);

    /**
     * @see #containsKey(byte[])
     */
    Boolean bContainsKey(final byte[] key);

    /**
     * @see #containsKey(byte[])
     */
    Boolean bContainsKey(final String key);

    /**
     * Equivalent to {@code scan(startKey, endKey, true)}.
     */
    CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey);

    /**
     * @see #scan(byte[], byte[])
     */
    CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey);

    /**
     * Equivalent to {@code scan(startKey, endKey, readOnlySafe, true)}.
     */
    CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe);

    /**
     * @see #scan(byte[], byte[], boolean)
     */
    CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey, final boolean readOnlySafe);

    /**
     * scan & iterator
     * 1.scan 和 iterator 都会包含两个入参 startKey， endKey，范围是一个左闭右开的区间： [startKey, endKey)
     * 2.iterator 与 scan 的不同点在于 iterator 是懒汉模式，在调用 hasNext() 时如果本地缓冲区无数据 （bufSize 为缓冲区大小）才会触发请求数据操作
     * 3.支持跨分区扫描，rheaKV 内部会自动计算 startKey ~ endKey 所覆盖的所有分区（region），
     *   并行发起调用， 对于单个分片数据量较大的情况，扫描整个分区一定是很慢的， 一定注意避免跨过多的分区
     * 4.startKey 可以为 null， 代表 minStartKey， 同理 endKey 也可以为 null，代表 maxEndKey，但如上一条所说，应尽量避免大范围的查询行为
     *
     * Query all data in the key of range [startKey, endKey).
     * <p>
     * Provide consistent reading if {@code readOnlySafe} is true.
     *
     * Scanning across multi regions maybe slower and devastating.
     *
     * @param startKey     first key to scan within database (included),
     *                     null means 'min-key' in the database.
     * @param endKey       last key to scan within database (excluded).
     *                     null means 'max-key' in the database.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @param returnValue  whether to return value.
     * @return a list where the key of range [startKey, endKey) passed by user
     * and value for {@code KVEntry}
     */
    CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                                          final boolean returnValue);

    /**
     * @see #scan(byte[], byte[], boolean, boolean)
     */
    CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey, final boolean readOnlySafe,
                                          final boolean returnValue);

    /**
     * @see #scan(byte[], byte[])
     */
    List<KVEntry> bScan(final byte[] startKey, final byte[] endKey);

    /**
     * @see #scan(String, String)
     */
    List<KVEntry> bScan(final String startKey, final String endKey);

    /**
     * @see #scan(String, String, boolean)
     */
    List<KVEntry> bScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe);

    /**
     * @see #scan(String, String, boolean)
     */
    List<KVEntry> bScan(final String startKey, final String endKey, final boolean readOnlySafe);

    /**
     * @see #scan(String, String, boolean, boolean)
     */
    List<KVEntry> bScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                        final boolean returnValue);

    /**
     * @see #scan(String, String, boolean, boolean)
     */
    List<KVEntry> bScan(final String startKey, final String endKey, final boolean readOnlySafe,
                        final boolean returnValue);

    /**
     * Equivalent to {@code reverseScan(startKey, endKey, true)}.
     */
    CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey);

    /**
     * @see #reverseScan(byte[], byte[])
     */
    CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey);

    /**
     * Equivalent to {@code reverseScan(startKey, endKey, readOnlySafe, true)}.
     */
    CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe);

    /**
     * @see #reverseScan(byte[], byte[], boolean)
     */
    CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey, final boolean readOnlySafe);

    /**
     * Reverse query all data in the key of range [startKey, endKey).
     * <p>
     * Provide consistent reading if {@code readOnlySafe} is true.
     *
     * Reverse scanning is usually much worse than forward scanning.
     *
     * Reverse scanning across multi regions maybe slower and devastating.
     *
     * @param startKey     first key to reverse scan within database (included),
     *                     null means 'max-key' in the database.
     * @param endKey       last key to reverse scan within database (excluded).
     *                     null means 'min-key' in the database.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @param returnValue  whether to return value.
     * @return a list where the key of range [startKey, endKey) passed by user
     * and value for {@code KVEntry}
     */
    CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey,
                                                 final boolean readOnlySafe, final boolean returnValue);

    /**
     * @see #reverseScan(byte[], byte[], boolean, boolean)
     */
    CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey,
                                                 final boolean readOnlySafe, final boolean returnValue);

    /**
     * @see #reverseScan(byte[], byte[])
     */
    List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey);

    /**
     * @see #scan(String, String)
     */
    List<KVEntry> bReverseScan(final String startKey, final String endKey);

    /**
     * @see #scan(String, String, boolean)
     */
    List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe);

    /**
     * @see #scan(String, String, boolean)
     */
    List<KVEntry> bReverseScan(final String startKey, final String endKey, final boolean readOnlySafe);

    /**
     * @see #reverseScan(String, String, boolean, boolean)
     */
    List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                               final boolean returnValue);

    /**
     * @see #reverseScan(String, String, boolean, boolean)
     */
    List<KVEntry> bReverseScan(final String startKey, final String endKey, final boolean readOnlySafe,
                               final boolean returnValue);

    /**
     * Equivalent to {@code iterator(startKey, endKey, bufSize, true)}.
     */
    RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize);

    /**
     * @see #iterator(byte[], byte[], int)
     */
    RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize);

    /**
     * Equivalent to {@code iterator(startKey, endKey, bufSize, true, true)}.
     */
    RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize,
                                   final boolean readOnlySafe);

    /**
     * @see #iterator(byte[], byte[], int, boolean)
     */
    RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize,
                                   final boolean readOnlySafe);

    /**
     * scan & iterator
     * 1.scan 和 iterator 都会包含两个入参 startKey， endKey，范围是一个左闭右开的区间： [startKey, endKey)
     * 2.iterator 与 scan 的不同点在于 iterator 是懒汉模式，在调用 hasNext() 时如果本地缓冲区无数据 （bufSize 为缓冲区大小）才会触发请求数据操作
     * 3.支持跨分区扫描，rheaKV 内部会自动计算 startKey ~ endKey 所覆盖的所有分区（region），
     *   并行发起调用， 对于单个分片数据量较大的情况，扫描整个分区一定是很慢的， 一定注意避免跨过多的分区
     *   4.startKey 可以为 null， 代表 minStartKey， 同理 endKey 也可以为 null，代表 maxEndKey，但如上一条所说，应尽量避免大范围的查询行为
     *
     * Returns a remote iterator over the contents of the database.
     *
     * Functionally similar to {@link #scan(byte[], byte[], boolean)},
     * but iterator only returns a small amount of data at a time, avoiding
     * a large amount of data returning to the client at one time causing
     * memory overflow, can think of it as a 'lazy scan' method.
     *
     * @param startKey     first key to scan within database (included),
     *                     null means 'min-key' in the database.
     * @param endKey       last key to scan within database (excluded),
     *                     null means 'max-key' in the database.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @param returnValue  whether to return value.
     * @return a iterator where the key of range [startKey, endKey) passed by
     * user and value for {@code KVEntry}
     */
    RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize,
                                   final boolean readOnlySafe, final boolean returnValue);

    /**
     * @see #iterator(byte[], byte[], int, boolean, boolean)
     */
    RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize,
                                   final boolean readOnlySafe, final boolean returnValue);

    /**
     * getSequence & resetSequence
     * 1. 通过 getSequence 可以获取一个全局的单调递增序列，step 作为步长，
     *    比如一个 step 为 10 的请求结果为 [n, n + 10)， 结果是一个左闭右开的区间，
     *    对于 sequence 的存储，是与普通 key-value 数据隔离的，所以无法使用普通 api 删除之，
     *    所以不用担心 sequence 数据被误删除， 但是也提供了手动重置 sequence 的方法，见下一条说明
     * 2. 需要强调的是，通常是不建议使用 resetSequence 系列方法的，提供这个 api 只是为了用于一些意外场景的 sequence 重置
     *
     * Get a globally unique auto-increment sequence.
     *
     * Be careful do not to try to get or update the value of {@code seqKey}
     * by other methods, you won't get it.
     *
     * @param seqKey the key of sequence
     * @param step   number of values obtained
     * @return a values range of [startValue, endValue)
     */
    CompletableFuture<Sequence> getSequence(final byte[] seqKey, final int step);

    /**
     * @see #getSequence(byte[], int)
     */
    CompletableFuture<Sequence> getSequence(final String seqKey, final int step);

    /**
     * @see #getSequence(byte[], int)
     */
    Sequence bGetSequence(final byte[] seqKey, final int step);

    /**
     * @see #getSequence(byte[], int)
     */
    Sequence bGetSequence(final String seqKey, final int step);

    /**
     * Gets the latest sequence start value, this is a read-only operation.
     *
     * Equivalent to {@code getSequence(seqKey, 0)}.
     *
     * @see #getSequence(byte[], int)
     *
     * @param seqKey the key of sequence
     * @return the latest sequence value
     */
    CompletableFuture<Long> getLatestSequence(final byte[] seqKey);

    /**
     * @see #getLatestSequence(byte[])
     */
    CompletableFuture<Long> getLatestSequence(final String seqKey);

    /**
     * @see #getLatestSequence(byte[])
     */
    Long bGetLatestSequence(final byte[] seqKey);

    /**
     * @see #getLatestSequence(byte[])
     */
    Long bGetLatestSequence(final String seqKey);

    /**
     * Reset the sequence to 0.
     *
     * @param seqKey the key of sequence
     */
    CompletableFuture<Boolean> resetSequence(final byte[] seqKey);

    /**
     * @see #resetSequence(byte[])
     */
    CompletableFuture<Boolean> resetSequence(final String seqKey);

    /**
     * @see #resetSequence(byte[])
     */
    Boolean bResetSequence(final byte[] seqKey);

    /**
     * @see #resetSequence(byte[])
     */
    Boolean bResetSequence(final String seqKey);

    /**
     * Set the database entry for "key" to "value".
     *
     * @param key   the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @return {@code true} if success.
     */
    CompletableFuture<Boolean> put(final byte[] key, final byte[] value);

    /**
     * @see #put(byte[], byte[])
     */
    CompletableFuture<Boolean> put(final String key, final byte[] value);

    /**
     * @see #put(byte[], byte[])
     */
    Boolean bPut(final byte[] key, final byte[] value);

    /**
     * @see #put(byte[], byte[])
     */
    Boolean bPut(final String key, final byte[] value);

    /**
     * getAndPut/bGetAndPut
     * 提供一个原子的 ‘get 旧值并 put 新值’ 的语义, 对于 String 类型的入参，请参考 get 相关说明。
     *
     * Set the database entry for "key" to "value", and return the
     * previous value associated with "key", or null if there was no
     * mapping for "key".
     * @param key   the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @return the previous value associated with "key", or null if
     * there was no mapping for "key".
     */
    CompletableFuture<byte[]> getAndPut(final byte[] key, final byte[] value);

    /**
     * @see #getAndPut(byte[], byte[])
     */
    CompletableFuture<byte[]> getAndPut(final String key, final byte[] value);

    /**
     * @see #getAndPut(byte[], byte[])
     */
    byte[] bGetAndPut(final byte[] key, final byte[] value);

    /**
     * @see #getAndPut(byte[], byte[])
     */
    byte[] bGetAndPut(final String key, final byte[] value);

    /**
     * compareAndPut/bCompareAndPut
     * 提供一个原子的 ‘compare 旧值并 put 新值’ 的语义, 其中 compare 语义表示 equals 而不是 ==。
     * 对于 String 类型的入参，请参考 get 相关说明。
     *
     * Atomically sets the value to the given updated value
     * if the current value equal (compare bytes) the expected value.
     *
     * @param key    the key retrieve the value
     * @param expect the expected value
     * @param update the new value
     * @return true if successful. False return indicates that the actual
     * value was not equal to the expected value.
     */
    CompletableFuture<Boolean> compareAndPut(final byte[] key, final byte[] expect, final byte[] update);

    /**
     * @see #compareAndPut(byte[], byte[], byte[])
     */
    CompletableFuture<Boolean> compareAndPut(final String key, final byte[] expect, final byte[] update);

    /**
     * @see #compareAndPut(byte[], byte[], byte[])
     */
    Boolean bCompareAndPut(final byte[] key, final byte[] expect, final byte[] update);

    /**
     * @see #compareAndPut(byte[], byte[], byte[])
     */
    Boolean bCompareAndPut(final String key, final byte[] expect, final byte[] update);

    /**
     * Add merge operand for key/value pair.
     *
     * <pre>
     *     // Writing aa under key
     *     db.put("key", "aa");
     *
     *     // Writing bb under key
     *     db.merge("key", "bb");
     *
     *     assertThat(db.get("key")).isEqualTo("aa,bb");
     * </pre>
     *
     * @param key   the specified key to be merged.
     * @param value the value to be merged with the current value for
     *              the specified key.
     * @return {@code true} if success.
     */
    CompletableFuture<Boolean> merge(final String key, final String value);

    /**
     * @see #merge(String, String)
     */
    Boolean bMerge(final String key, final String value);

    /**
     * batch put
     * 1. 支持跨分区操作的一个 batch put, rheakv 内部会自动计算每个 key 的所属分区并行发起调用
     * 2. 需要注意的是， 这个操作暂时无法提供事务保证，
     * 无法承诺 ‘要么全部成功要么全部失败’，不过由于 rheaKV 内部是支持 failover 自动重试的， 可以一定程度上减少上述情况的发生
     * The batch method of {@link #put(byte[], byte[])}
     */
    CompletableFuture<Boolean> put(final List<KVEntry> entries);

    /**
     * @see #put(List)
     */
    Boolean bPut(final List<KVEntry> entries);

    /**
     * 提供一种原子语义： 如果该 key 不存在则 put 如果该 key 已经存在， 那么只返回这个已存在的值;
     *
     * If the specified key is not already associated with a value
     * associates it with the given value and returns {@code null},
     * else returns the current value.
     *
     * @param key   the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @return the previous value associated with the specified key,
     * or {@code null} if there was no mapping for the key.
     * (A {@code null} return can also indicate that the database.
     * previously associated {@code null} with the key.
     */
    CompletableFuture<byte[]> putIfAbsent(final byte[] key, final byte[] value);

    /**
     * @see #putIfAbsent(byte[], byte[])
     */
    CompletableFuture<byte[]> putIfAbsent(final String key, final byte[] value);

    /**
     * @see #putIfAbsent(byte[], byte[])
     */
    byte[] bPutIfAbsent(final byte[] key, final byte[] value);

    /**
     * @see #putIfAbsent(byte[], byte[])
     */
    byte[] bPutIfAbsent(final String key, final byte[] value);

    /**
     * 删除指定 key 关联的值
     * Delete the database entry (if any) for "key".
     *
     * @param key key to delete within database.
     * @return {@code true} if success.
     */
    CompletableFuture<Boolean> delete(final byte[] key);

    /**
     * @see #delete(byte[])
     */
    CompletableFuture<Boolean> delete(final String key);

    /**
     * @see #delete(byte[])
     */
    Boolean bDelete(final byte[] key);

    /**
     * @see #delete(byte[])
     */
    Boolean bDelete(final String key);

    /**
     * 1. 移除 [startKey, endKey) 范围内所有的数据， 注意 key的 范围是一个左闭右开的区间，即不包含endKey
     * 2. 同样支持跨分区删除， rheaKV 内部会自动计算这个 key 区间的所覆盖的分区然后并行发起调用，
     *    同样需要强调，这是个较危险的操作，请慎重使用。
     * Removes the database entries in the range ["startKey", "endKey"), i.e.,
     * including "startKey" and excluding "endKey".
     *
     * @param startKey first key to delete within database (included)
     * @param endKey   last key to delete within database (excluded)
     * @return {@code true} if success.
     */
    CompletableFuture<Boolean> deleteRange(final byte[] startKey, final byte[] endKey);

    /**
     * @see #deleteRange(byte[], byte[])
     */
    CompletableFuture<Boolean> deleteRange(final String startKey, final String endKey);

    /**
     * @see #deleteRange(byte[], byte[])
     */
    Boolean bDeleteRange(final byte[] startKey, final byte[] endKey);

    /**
     * @see #deleteRange(byte[], byte[])
     */
    Boolean bDeleteRange(final String startKey, final String endKey);

    /**
     * The batch method of {@link #delete(byte[])}
     */
    CompletableFuture<Boolean> delete(final List<byte[]> keys);

    /**
     * @see #delete(List)
     */
    Boolean bDelete(final List<byte[]> keys);

    /**
     * @see #getDistributedLock(byte[], long, TimeUnit, ScheduledExecutorService)
     */
    DistributedLock<byte[]> getDistributedLock(final byte[] target, final long lease, final TimeUnit unit);

    /**
     * @see #getDistributedLock(String, long, TimeUnit, ScheduledExecutorService)
     */
    DistributedLock<byte[]> getDistributedLock(final String target, final long lease, final TimeUnit unit);

    /**
     * DistributedLock
     * 1. 获取一个分布式锁实例，rheaKV 的 distributedLock 实现了: 可重入锁、自动续租以及 fencing token
     * 2. target：可以为理解为分布式锁的 key, 不同锁的 key 不能重复，
     *    但是锁的存储空间是与其他 kv 数据隔离的，所以只需保证 key 在 ‘锁空间’ 内的唯一性即可
     * 3. lease：必须包含一个锁的租约（lease）时间，在锁到期之前，如果 watchdog 为空，那么锁会被自动释放，
     *    即没有 watchdog 配合的 lease，就是 timeout 的意思
     * 4. watchdog：一个自动续租的调度器，需要用户自行创建并销毁，框架内部不负责该调度器的生命周期管理，
     *    如果 watchdog 不为空，会定期（lease 的 2⁄3 时间为周期）主动为当前的锁不断进行续租，直到用户主动释放锁（unlock）
     * 5. 还有一个需要强调的是：因为 distributedLock 是可重入锁，所以 lock() 与 unlock() 必须成对出现，
     *    比如 lock() 2 次却只 unlock() 1 次是无法释放锁成功的
     * 6. String 类型入参: 见 get 相关说明
     * 7. 其中 boolean tryLock(final byte[] ctx) 包含一个 ctx 入参，
     *    作为当前的锁请求者的用户自定义上下文数据，如果它成功获取到锁，其他线程、进程也可以看得到它的 ctx
     *
     * Note: 还有一个重要的方法 long getFencingToken()，当成功上锁后，可以通过该接口获取当前的 fencing token，
     * 这是一个单调递增的数字，也就是说它的值大小可以代表锁拥有者们先来后到的顺序（避免 GC pause or 网络延迟的问题）。
     *
     * Creates a distributed lock implementation that provides
     * exclusive access to a shared resource.
     * <p>
     * <pre>
     *      DistributedLock<byte[]> lock = ...;
     *      if (lock.tryLock()) {
     *          try {
     *              // manipulate protected state
     *          } finally {
     *              lock.unlock();
     *          }
     *      } else {
     *          // perform alternative actions
     *      }
     * </pre>
     *
     * The algorithm relies on the assumption that while there is no
     * synchronized clock across the processes, still the local time in
     * every process flows approximately at the same rate, with an error
     * which is small compared to the auto-release time of the lock.
     *
     * @param target   key of the distributed lock that acquired.
     * @param lease    the lease time for the distributed lock to live.
     * @param unit     the time unit of the {@code expire} argument.
     * @param watchdog if the watchdog is not null, it will auto keep
     *                 lease of current lock, otherwise won't keep lease,
     *                 this method dose not pay attention to the life cycle
     *                 of watchdog, please maintain it yourself.
     * @return a distributed lock instance.
     */
    DistributedLock<byte[]> getDistributedLock(final byte[] target, final long lease, final TimeUnit unit,
                                               final ScheduledExecutorService watchdog);

    /**
     * @see #getDistributedLock(byte[], long, TimeUnit, ScheduledExecutorService)
     */
    DistributedLock<byte[]> getDistributedLock(final String target, final long lease, final TimeUnit unit,
                                               final ScheduledExecutorService watchdog);

    /**
     * Returns current placement driver client instance.
     */
    PlacementDriverClient getPlacementDriverClient();

    /**
     * Add a listener for the state change of the leader with
     * this region.
     * <p>
     * In a special case, if that is a single region environment,
     * then regionId = -1 as the input parameter.
     */
    void addLeaderStateListener(final long regionId, final LeaderStateListener listener);

    /**
     * Add a listener for the state change of the follower with
     * this region.
     * <p>
     * In a special case, if that is a single region environment,
     * then regionId = -1 as the input parameter.
     */
    void addFollowerStateListener(final long regionId, final FollowerStateListener listener);

    /**
     * Add a listener for the state change (leader, follower) with
     * this region.
     * <p>
     * In a special case, if that is a single region environment,
     * then regionId = -1 as the input parameter.
     */
    void addStateListener(final long regionId, final StateListener listener);
}
