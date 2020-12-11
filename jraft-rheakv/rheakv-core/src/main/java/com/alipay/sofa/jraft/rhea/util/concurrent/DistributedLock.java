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
package com.alipay.sofa.jraft.rhea.util.concurrent;

import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.alipay.sofa.jraft.rhea.util.UniqueIdUtil;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;

/**
 * 分布式锁是控制分布式系统之间同步访问共享资源的一种方式，用于在分布式系统中协调他们之间的动作。
 *
 * 如果不同的系统或是同一个系统的不同主机之间共享了一个或一组资源，
 * 那么访问这些资源的时候，往往需要互斥来防止彼此干扰来保证一致性，在此种情况下便需要使用到分布式锁。
 *
 * 分布式锁通过共享标识确定其唯一性，对共享标识进行修改时能够保证原子性和对锁服务调用方的可见性。
 *
 * 使用分布式锁的两种场景：
 *  1）为了效率（efficiency）：
 *      使用分布式锁能够避免不同节点重复相同的工作导致资源浪费；
 *      即使偶尔失效了，只是可能将某些操作多做了一遍而已，不会产生其它不良的后果。
 *      例如：在用户付款之后有可能不同的节点发出多条短信
 *
 *  2）为了正确性（correctness）：
 *      添加分布式锁同样避免破坏正确性事件的发生（在任何情况下都不允许锁失效的情况发生）；
 *      因为一旦发生（如果两个节点在同一条数据上面操作），就可能意味着数据不一致，数据丢失，文件损坏，或者其它严重的问题。
 *      例如：多个节点机器对同一个订单操作不同的流程有可能导致此笔订单最后的状态错误造成资金损失。
 *
 * 分布式锁需要具备的条件包括：
 *  1）获取锁和释放所的性能要好（）；
 *  2）判断获得锁是否是原子性的，否则可能导致多个请求都能获取到锁；
 *  3）网络中断或者宕机无法释放锁时，锁必须被清除；
 *  4）可重入一个线程中多次获取同一把锁
 *    （例如：一个线程在执行带锁的方法，该方法调用另一个需要相同锁的方法，则此线程直接执行调用的方法，而无需重新获得锁）
 *  5）阻塞锁和非阻塞锁，阻塞锁即没有获取到锁，则继续等待锁；非阻塞锁即没有获取到锁，不继续等待直接返回获取锁失败。
 *
 *  分布式锁实现：
 *  分布式CAP理论告诉我们："任何一个分布式系统都无法同时满足：
 *  一致性（Consistency）、可用性（Availability）和分区容错性（Partition Tolerance），最多只能同时满足两项"。
 *  在互联网领域的绝大多数场景中，都需要牺牲强一致性来换取系统的高可性，
 *  系统往往只需要保证"最终一致性"，只要这个最终时间在一个可接受的的范围内即可。
 *  在很多场景中为了保证数据的最终一致性，需要很多的技术方案来支持，比如分布式事务，分布式锁等。
 *
 *  有的时候需要保证一个方法在同一时间内只能被同一个线程执行。
 *
 *  分布式锁一般有三种实现方式：
 *  1）基于数据库实现分布式锁；
 *  2）基于缓存（Redis，Memcached，Tair）实现分布式锁；
 *  3）基于ZooKeeper（Paxos）实现分布式锁；
 *
 *  1）基于数据库实现分布式锁
 *      基于数据库实现的分布式锁的核心思想是：在数据库中创建一张表，表里面包含方法名等字段，并在此方法名字段上面建立唯一索引，
 *      执行某个方法需要使用此方法名向表中插入数据，成功插入则获取锁，
 *      执行结束则删除对应的行数据释放锁。
 *  2）基于缓存实现分布式锁
 *      基于缓存通常选用 Redis 实现分布式锁，考虑到Redis有非常高的性能（set 1s/30W write），
 *      Redis命令对分布式所支持友好('set key value EX PX 30000')，并且实现方便。
 *      基于单 Redis 节点的分布式锁在 Failover 的时候产生解决不了的安全性问题，
 *      Redlock 是 Redis 的作者 Antirez 提出的集群模式 Redis 分布式锁，
 *      基于 N 个完全独立的 Redis 节点(通常情况下 N 可以设置成5)，运行 Redlock 算法依次执行下面各个步骤完成获取锁的操作：
 *      a）获取当前时间（毫秒数）;
 *      b）按顺序依次向N个Redis节点执行 获取锁 的操作。
 *        这个获取操作和基于单Redis节点的 获取锁 的过程相同，
 *        包含value driverId ，也包含过期时间(比如 PX 30000 ，即锁的有效时间)。
 *        为了保证在某个Redis节点不可用的时候算法能够继续运行，
 *        这个 获取锁 的操作还有一个超时时间(time out)， 它要远小于锁的有效时间（几十毫秒量级）。
 *        客户端在向某个Redis节点获取锁失败以后，应该立即尝试下一个Redis节点。
 *        这里的失败，应该包含任何类型的失败，比如该Redis节点不可用，
 *        或者该Redis节点上的锁已经被其它客户端持有（注：Redlock原文中这里只提到了Redis节点不可用的情况，但也应该包含其它的失败情况）。
 *      c）计算整个获取锁的过程总共消耗了多长时间，计算方法是用当前时间减去第1步记录的时间。
 *        如果客户端从大多数Redis节点（>= N/2+1）成功获取到了锁，
 *        并且获取锁总共消耗的时间没有超过锁的有效时间(lock validity time)，
 *        那么这时客户端才认为最终获取锁成功；否则，认为最终获取锁失败。
 *      d）如果最终获取锁成功了，那么这个锁的有效时间应该重新计算，它等于最初的锁的有效时间减去第3步计算出来的获取锁消耗的时间。
 *      e）如果最终获取锁失败了（可能由于获取到锁的Redis节点个数少于N/2+1，或者整个获取锁的过程消耗的时间超过了锁的最初有效时间），
 *        那么客户端应该立即向所有Redis节点发起 释放锁 的操作。
 *  3）基于ZooKeeper实现分布式锁
 *      ZooKeeper是以Paxos算法为基础的分布式应用协调服务，为分布式应用提供一致性服务的开源组件，
 *      其内部是分层的文件系统目录树结构，规定同一个目录下只能有一个唯一的文件名。基于zookeeper实现的分布式锁步骤包括：
 *      a）创建一个锁目录 lock；
 *      b）希望获得锁的线程A在lock目录下创建临时顺序节点；
 *      c）当前线程获得锁目录下的所有子节点，然后获取比自己少的兄弟节点，如果不存在则表示当前线程顺序号最小，获得锁；
 *      d）线程B获取所有节点，判断自己不是最小的节点，
 *        设置监听（Watcher）比自己小的兄弟节点（只关注比自己次小的（前一个）节点是为了防止发生"羊群效应"）
 *      e）线程A处理完删除自己的节点，线程 B监听到变更事件判断自己是否为最小节点，如果是则获得锁。
 *
 *  RheaKV 分布式锁实现
 *   RheaKV 是基于 SOFARaft 和 RocksDB实现的嵌入式、分布式、高可用、强一致性的KV存储类库，
 *   RheaKV 提供DistributedLock实现可重入锁，自动续租以及 Fencing Token 功能特性。
 *
 *  DistributedLock 是可重入锁，tryLock(...) 和 unLock(...)必须成对出现，
 *  RheaKV 调用 getDistributedLock()获取分布式锁实例，其中参数：
 *    1）target 理解为分布式锁的key，不同锁的key不能重复，
 *      但是锁的存储空间是与其它KV数据隔离的，所以只需要保证key在"锁空间"内唯一性即可；
 *    2）lease 必须包含锁的租约（lease）时间，在锁到期之前如果 watcherdog 为空，那么锁会被自动释放，
 *      即没有 watcherdog 配合的lease就是timeout的意思；
 *    3）watcherdog 表示自动续租的调度器，需要开发人员自行创建并销毁，框架内不负责此调度器的生命管理周期，
 *      如果 watcher 不为空定期（lease的2/3时间为周期）主动为当前的锁不断进行续租，直到用户主动释放锁（unlock）。
 *
 *
 *  RheaKV Lock 流程：
 *  1）获得锁内部 Key 和 锁获取器 acquirer，调用 DefaultRheaKVStore#tryLockWith(key, keepLease, acquirer)方法进行设置分布式锁；
 *  2）检查RheaKVStore状态是否为已启动或者已关闭，PlacementDriverClient 按照分布式锁key定位所对应的分区 region，
 *    根据分区 region 的 id 获取 Leader节点分区引擎，
 *    基于分布式锁 key 和 锁获取器 acquirer 生成重试器 retryRunner 组建 Failover 回调 FailoverClosure；
 *  3）判断分区引擎 regionEngine 是否为空，
 *    如果regionEngine为空表示Leader节点不在本地，则构建 KeyLockRequest给RheaKVStore分区Leader节点发起异步RPC调用请求加锁；
 *    如果regionEngine非空则确保当前分布式锁对应的分区 region在合理 Epoch 期数范围内，
 *    获取分区引擎 regionEngine底层 MetricsRawKVStore 尝试加锁；
 *  4）MetricsRawKVStore 使用基于 Raft协议副本状态机的 RaftRawKVStore 设置分布式锁，
 *    其算法依赖于以下假设：尽管跨进程存在非同步时钟，但每个进程的本地时间仍以大致相同的速率流动，并且与锁的自动释放时间相比其错误较小。
 *    锁获取器 acquirer设置锁默认时钟为锁时间戳，申请基于分布式锁 key的加锁 KEY_LOCK操作 KVOperation;
 *  5）RheaKV 存储状态机 KVStoreStateMachine 按照操作类型为 KEY_LOCK 批量调用
 *    RocksRawKVStore的tryLockWith(key, fencingKey, keepLease, acquirer, closure) 基于 RocksDB执行加速操作。
 *    RocksRawKVStore获取读写锁 readWriteLock的读锁并且加读锁，查询 RocksDB分布式锁 key的锁持有锁 prevBytesVal。
 *    创建分布式锁持有者构造器 builder，通过锁持有者构造器构造锁持有者 owner并且回调 KVStoreClosure 返回其锁持有者owner，
 *    读写锁 readWriteLock的读锁进行解锁：
 *      1⃣ 检查此锁持有者 prevBytesVal 是否为空：
 *          prevBytesVal为空表示无其它锁请求者持有此锁，即首次尝试上锁或者此锁已删除，
 *          锁持有者构造器设置持有者 id 为锁获取器 acquirer 的 id 即表示将持有此锁，指定新的截止时间戳，
 *          定义租约剩余时间为首次获得锁成功 FIRST_TIME_SUCCESS 即 -1，
 *          按照 fencingKey 新建 fencing token，初始化锁重入 acquirer 为1，设置锁持有者上下文为锁获取器 acquirer 的上下文，
 *          设置上锁成功构建锁持有者 owner 基于分布式锁 key 键值对方式插入 RocksDB存储；
 *      2⃣ 锁持有者 prevBytesVal 非空检查其锁持有是否过期，即使用序列化器读取其之前锁持有者 prevOwner，判断距离锁持有截止剩余时间是否小于零：
 *          小于零（<0）表示锁持有者已经超出其租约，
 *          锁持有者构造器设置锁持有者 id 为锁获取器 acquirer的 id，指定新的截止时间戳，
 *          定义租约剩余时间为新获取锁成功 NEW_ACQUIRE_SUCCESS 即 -2，
 *          按照 fencingKey 新建 fencing token，初始化锁重入 acquirer 为1，设置锁持有者上下文为锁获取器 acquirer 的上下文，
 *          设置上锁成功构建锁持有者 owner 基于分布式锁 key 键值对方式插入 RocksDB存储；
 *      3⃣ 锁持有者未超出租约即剩余时间大于或者等于零（>=0），检查之前锁持有者的锁获取器与当前的锁获取器 acquirer 是否相同：
 *          a）获取器相同表示此分布式锁为重入锁，锁持有者构造器设置锁持有者 id 为之前锁持有者 id，
 *            更新截止时间戳保持续约，指定租约剩余时间为重入成功 REENTRANT_SUCCESS 即 -4，
 *            保持锁持有者 prevOwner 的 fencing token，修改锁重入 acquirer 自增1，
 *            更新锁持有者上下文为锁获取器 acquirer 的上下文，
 *            设置上锁成功构建锁持有者 owner 基于分布式锁 key 键值对方式插入 RocksDB存储；
 *          b）此锁已存在且之前锁持有者与当前锁请求者不同，表示非重入锁，
 *            表示其它锁请求者在尝试上已存在的锁，锁持有者构造器设置锁持有者 id 为之前锁持有者 id，
 *            更新租约剩余时间为当前锁持有者的租约剩余时间，指定锁持有者上下文为锁持有者 prevOwner的上下文，设置上锁失败构建锁持有者 owner；
 *  6）检查分布式锁持有者 owner 是否成功，获取锁持有者成功表示设置分布式锁成功，
 *    更新当前锁持有者的锁获取器 acquirer 的 fencing token，
 *    获取自动续租的调度器 watcherdog 调用 scheduleKeepingLease(watcherdog, internalKey, acquirer, period)
 *    以租约 lease 的 2/3 时间为调度周期给当前锁不断续租保持租约；
 *  7）当成功上锁后通过 getFencingToken() 接口获取当前的 fencing token，此为单调递增数字即值大小代表锁拥有者们先来后到的顺序。
 *    在时序图（https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html）中，
 *    假定锁服务本身是没有问题，它总是能保证任一时刻最多只有一个客户端获取到锁，
 *    客户端1获得锁之后发生很长时间的 GC pause（or 网络原因），期间其获得的锁已经过期，而客户端2获得锁。
 *    当客户端1从 GC pause（or 网络原因）中恢复过来时，它不知道自己持有的锁已过期，依然向共享资源的存储服务发起写数据请求，
 *    而这时锁实际上被客户端2持有，因此两个客户端的写请求有可能冲突即锁的互斥性失效，使用fencing token 主要解决此时序性问题。
 *
 * RheaKV unlock 流程：
 * RheaKV 调用 unlock() 方法释放分布式锁，使用分布式锁接口默认实现 DefaultDistributeLock#unlock() 方法尝试释放锁：
 *  1）获取分布式锁内部 key 和锁获取器 acquirer，调用 DefaultRheaKVStore 的 releaseLockWith(key, acquirer) 方法释放分布式锁；
 *  2）检查 RheaKVStore 状态是否为已启动或已关闭，根据 分布式锁 key 查找所对应分区的 region，
 *    根据分区 region 的 id 获取 Leader 节点分区引擎 regionEngine，
 *    基于分布式锁 key 和锁获取器 acquirer 创建重试器 retryRunner 构成 Failover 回调 failoverClosure；
 *  3）检查分区引擎 regionEngine 是否为空，
 *    假如 regionEngine 为空则构建 KeyUnlockRequest 发起对 RheaKVStore 分区 Leader 节点发起异步 RPC 调用请求解锁；
 *    如果 regionEngine 非空则确保当前分布式锁 key 所在分区的 region 在合理的 Epoch期数范围，
 *    获取分区引擎 regionEngine 底层 MetricsRawKVStore 尝试解除锁；
 *  4）MetricsRawKVStore 通过基于 Raft协议副本状态机的 RaftRawKVStore 解除分布式锁，
 *    申请基于分布式锁 key 的解锁 KEY_LOCK_RELEASE 操作 KVOperation；
 *  5）RheaKV 存储状态机 KVStoreStateMachine 按照操作类型为 KEY_LOCK_RELEASE 批量调用
 *    RocksRawKVStore 的 releaseLockWith(key, acquirer, closure) 基于 RocksDB 执行解锁操作。
 *    RocksRawKVStore 获取读写锁 readWriteLock 的读锁并加读锁 查询 RocksDB 分布式锁 key 的锁持有者 prevBytesVal。
 *    创建分布式锁持有者构造器 builder，通过锁持有者构造器构造锁持有者 owner 并且回调 KVStoreClosure 返回其锁持有者 owner，
 *    读写锁 readWriteLock 的读锁进行解锁：
 *       1⃣ 检查此锁持有者 prevBytesVal 是否为空：
 *          a）prevBytesVal 为空表示无其它锁请求持有此锁即此锁不存在，
 *          锁持有者构造器设置持有者 id 为锁获取器 acquirer 的 id 即表示持有此锁，
 *          指定 fencing token 为锁获取器 acquirer 的 fencing token，定义锁重入 acquires 为 0，
 *          设置解锁成功构建锁持有者 owner；
 *       2⃣ 锁持有者 prevBytesVal 非空，检查使用序列化器读取之前锁持有者 prevOwner，检查之前锁持有者的锁获取器是否当前锁获取器 acquire 是否相同：
 *          a）锁获取器相同表示此分布式锁为重入锁，锁持有者构造器设置锁持有者 id 为之前锁持有者 id，
 *            更新截止时间戳为锁持有者 prevOwner 的截止时间戳，保持锁持有者 prevOwner 的 fencing token，
 *            修改锁重入 acquirers 为之前锁持有减1，更新锁持有者的上下文为锁持有者 prevOwner 上下文，
 *            设置解锁成功构建锁持有者 owner，
 *            按照锁重入 acquirers 是否小于或者等于0基于分布式锁 key 删除 RocksDB 锁持有者（锁重入 acquires 小于或等于0）
 *            或者覆盖 RocksDB 更新锁持有者（锁重入 acquirers 大于0）；
 *          b）锁持有者 prevOwner 的锁获取器与当前锁获取器 acquirer 不同表示当前锁获取器不合理不能进行解锁，
 *            锁持有者构造器设置锁持有者 id 为之前锁持有者 id 通知真正的锁持有者，
 *            保持锁持有者 prevOwner 的 fencing token，保持锁重入 acquirers 为之前锁持有者，
 *            更新锁持有者上下文为锁持有者 prevOwner 的上下文，设置解锁失败构建锁持有者 owner；
 *  6）更新当前锁持有者owner，检查锁持有者的获取器是否为当前锁获取器 acquirer，使用 tryCancelScheduling() 方法取消自动续租调度；
 *
 *
 *
 *
 * {@link com.alipay.sofa.jraft.rhea.client.RheaKVStore
 *      最上层 User API，默认实现为 DefaultRheaKVStore，
 *      RheaKVStore 为纯异步实现，所以通常阻塞调用导致的客户端出现瓶颈， 理论上不会在RheaKV上遭遇，
 *      DefaultRheaKVStore 实现了包括请求路由、request 分裂、response 聚合以及失败重试等功能}
 * {@link com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore#tryLockWith(byte[], boolean, Acquirer)}
 *
 * {@link com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient
 *      非必须，作为与 PlacementDriver Server 集群沟通的客户端，可以通过它获取集群完整信息，
 *      包括但不仅限于”请求路由表”，对于无 PD 场景， rheaKV 提供一个 fake pd client}
 * {@link com.alipay.sofa.jraft.rhea.client.RegionRouteTable
 *      作为一个本地路由表缓存组件，RegionRouteTable 会根据 kv 请求的具体失败原因来决策是否从 PD Server 集群刷新数据，
 *      还提供对单个 key、多个 key 列表以及一个key range进行计算，返回对应的分区 ID}
 * {@link com.alipay.sofa.jraft.rhea.client.LoadBalancer
 *      在提供 follower 线性一致读的配置下有效，目前仅支持RR策略}
 * {@link com.alipay.sofa.jraft.rhea.storage.MetricsRawKVStore
 *      拦截请求做指标度量}
 * {@link com.alipay.sofa.jraft.rhea.storage.RaftRawKVStore}
 * {@link com.alipay.sofa.jraft.rhea.storage.KVStoreStateMachine}
 * {@link com.alipay.sofa.jraft.rhea.storage.RocksRawKVStore#tryLockWith(byte[], byte[], boolean, Acquirer, KVStoreClosure)
 *      原始的 rocksdb api 封装， 目前 rheaKV 也支持可插拔的 memoryDB 存储实现}
 *
 * A distributed lock that provides exclusive access to a
 * shared resource.
 *
 * <pre>
 *      DistributedLock<T> lock = ...;
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
 * @author jiachun.fjc
 */
public abstract class DistributedLock<T> {

    private final T                        internalKey;
    private final Acquirer                 acquirer;
    private final ScheduledExecutorService watchdog;

    private volatile Owner                 owner;

    protected DistributedLock(T target, long lease, TimeUnit unit, ScheduledExecutorService watchdog) {
        Requires.requireTrue(lease >= 0, "lease must >= 0");
        this.internalKey = withInternalKey(target);
        this.acquirer = new Acquirer(UniqueIdUtil.generateId(), unit.toMillis(lease));
        this.watchdog = watchdog;
    }

    /**
     * @see #tryLock(byte[])
     */
    public boolean tryLock() {
        return tryLock(null);
    }

    /**
     * Acquires the lock only if it is free at the time of invocation.
     *
     * Acquires the lock if it is available and returns immediately
     * with the value {@code true}.
     * If the lock is not available then this method will return
     * immediately with the value {@code false}.
     *
     * @param ctx the context of current lock request
     * @return {@code true} if the lock was acquired and {@code false}
     * otherwise
     */
    public boolean tryLock(final byte[] ctx) {
        return internalTryLock(ctx).isSuccess();
    }

    /**
     * @see #tryLock(byte[], long, TimeUnit)
     */
    public boolean tryLock(final long timeout, final TimeUnit unit) {
        return tryLock(null, timeout, unit);
    }

    /**
     * Acquires the lock if it is free within the given waiting time.
     *
     * If the lock is available this method returns immediately
     * with the value {@code true}.
     * If the lock is not available then
     * the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of two things happens:
     * <ul>
     * <li>The lock is acquired by the current thread; or
     * <li>The specified waiting time elapses
     *
     * @param ctx     the context of current lock request
     * @param timeout the maximum time to wait for the lock
     * @param unit    the time unit of the {@code time} argument
     * @return {@code true} if the lock was acquired and {@code false}
     * if the waiting time elapsed before the lock was acquired
     */
    public boolean tryLock(final byte[] ctx, final long timeout, final TimeUnit unit) {
        final long timeoutNs = unit.toNanos(timeout);
        final long startNs = System.nanoTime();
        int attempts = 1;
        try {
            for (;;) {
                final Owner owner = internalTryLock(ctx);
                if (owner.isSuccess()) {
                    return true;
                }
                if (System.nanoTime() - startNs >= timeoutNs) {
                    break;
                }
                if (attempts < 8) {
                    attempts++;
                }
                final long remaining = Math.max(0, owner.getRemainingMillis());
                // TODO optimize with notify?
                // avoid liveLock
                Thread.sleep(Math.min(remaining, 2 << attempts));
            }
        } catch (final Throwable t) {
            ThrowUtil.throwException(t);
        }
        return false;
    }

    /**
     * Attempts to release this lock.
     *
     * If the current caller is the holder of this lock then the hold
     * count is decremented.  If the hold count is now zero then the
     * lock is released.  If the current caller is not the holder of
     * this lock then InvalidLockAcquirerException is thrown.
     */
    public abstract void unlock();

    /**
     * Making the lock safe with fencing token.
     *
     * Is simply a number that increases (e.g. incremented by the lock
     * service) every time a client acquires the lock.
     */
    public long getFencingToken() {
        return this.acquirer.getFencingToken();
    }

    /**
     * Returns the 'lock-context' of current lock owner.
     */
    public byte[] getOwnerContext() {
        return getOwner().getContext();
    }

    public ScheduledExecutorService getWatchdog() {
        return watchdog;
    }

    public Acquirer getAcquirer() {
        return acquirer;
    }

    public Owner getOwner() {
        final Owner copy = this.owner;
        if (copy == null) {
            throw new IllegalStateException("must try to lock at first");
        }
        return copy;
    }

    public static OwnerBuilder newOwnerBuilder() {
        return new OwnerBuilder();
    }

    protected abstract Owner internalTryLock(final byte[] ctx);

    protected T withInternalKey(final T target) {
        // override this method to plastic with target, default do nothing
        return target;
    }

    protected T getInternalKey() {
        return internalKey;
    }

    protected void updateOwner(final Owner owner) {
        this.owner = owner;
    }

    protected void updateOwnerAndAcquirer(final Owner owner) {
        this.owner = owner;
        if (this.owner != null) {
            this.owner.updateAcquirerInfo(this.acquirer);
        }
    }

    public static class Acquirer implements Serializable {

        private static final long serialVersionUID = -9174459539789423607L;

        private final String      id;
        private final long        leaseMillis;

        // the time on trying to lock, it must be set by lock server
        private volatile long     lockingTimestamp;
        // making the lock safe with fencing token.
        //
        // is simply a number that increases (e.g. incremented by the lock service)
        // every time a client acquires the lock.
        private volatile long     fencingToken;
        // the context of current lock request
        private volatile byte[]   context;

        public Acquirer(String id, long leaseMillis) {
            this.id = id;
            this.leaseMillis = leaseMillis;
        }

        public String getId() {
            return id;
        }

        public long getLeaseMillis() {
            return leaseMillis;
        }

        public long getLockingTimestamp() {
            return lockingTimestamp;
        }

        public void setLockingTimestamp(long lockingTimestamp) {
            this.lockingTimestamp = lockingTimestamp;
        }

        public long getFencingToken() {
            return fencingToken;
        }

        public void setFencingToken(long fencingToken) {
            this.fencingToken = fencingToken;
        }

        public byte[] getContext() {
            return context;
        }

        public void setContext(byte[] context) {
            this.context = context;
        }

        @Override
        public String toString() {
            return "Acquirer{" + "id='" + id + '\'' + ", leaseMillis=" + leaseMillis + ", lockingTimestamp="
                   + lockingTimestamp + ", fencingToken=" + fencingToken + ", context=" + BytesUtil.toHex(context)
                   + '}';
        }
    }

    public static class Owner implements Serializable {

        private static final long serialVersionUID = 3939239434225894164L;

        // locker id
        private final String      id;
        // absolute time for this lock to expire
        private final long        deadlineMillis;
        // remainingMillis < 0 means lock successful
        private final long        remainingMillis;
        // making the lock safe with fencing token
        //
        // is simply a number that increases (e.g. incremented by the lock service)
        // every time a client acquires the lock.
        private final long        fencingToken;
        // for reentrant lock
        private final long        acquires;
        // the context of current lock owner
        private final byte[]      context;
        // if operation success
        private final boolean     success;

        public Owner(String id, long deadlineMillis, long remainingMillis, long fencingToken, long acquires,
                     byte[] context, boolean success) {
            this.id = id;
            this.deadlineMillis = deadlineMillis;
            this.remainingMillis = remainingMillis;
            this.fencingToken = fencingToken;
            this.acquires = acquires;
            this.context = context;
            this.success = success;
        }

        public boolean isSameAcquirer(final Acquirer acquirer) {
            return acquirer != null && this.fencingToken == acquirer.fencingToken
                   && Objects.equals(this.id, acquirer.id);
        }

        public void updateAcquirerInfo(final Acquirer acquirer) {
            if (acquirer == null) {
                return;
            }
            acquirer.setFencingToken(this.fencingToken);
        }

        public String getId() {
            return id;
        }

        public long getDeadlineMillis() {
            return deadlineMillis;
        }

        public long getRemainingMillis() {
            return remainingMillis;
        }

        public long getFencingToken() {
            return fencingToken;
        }

        public long getAcquires() {
            return acquires;
        }

        public byte[] getContext() {
            return context;
        }

        public boolean isSuccess() {
            return success;
        }

        @Override
        public String toString() {
            return "Owner{" + "id='" + id + '\'' + ", deadlineMillis=" + deadlineMillis + ", remainingMillis="
                   + remainingMillis + ", fencingToken=" + fencingToken + ", acquires=" + acquires + ", context="
                   + BytesUtil.toHex(context) + ", success=" + success + '}';
        }
    }

    public static class OwnerBuilder {

        public static long KEEP_LEASE_FAIL     = Long.MAX_VALUE;
        public static long FIRST_TIME_SUCCESS  = -1;
        public static long NEW_ACQUIRE_SUCCESS = -2;
        public static long KEEP_LEASE_SUCCESS  = -3;
        public static long REENTRANT_SUCCESS   = -4;

        private String     id;
        private long       deadlineMillis;
        private long       remainingMillis;
        private long       fencingToken;
        private long       acquires;
        private byte[]     context;
        private boolean    success;

        public Owner build() {
            return new Owner(this.id, this.deadlineMillis, this.remainingMillis, this.fencingToken, this.acquires,
                this.context, this.success);
        }

        public OwnerBuilder id(final String id) {
            this.id = id;
            return this;
        }

        public OwnerBuilder deadlineMillis(final long deadlineMillis) {
            this.deadlineMillis = deadlineMillis;
            return this;
        }

        public OwnerBuilder remainingMillis(final long remainingMillis) {
            this.remainingMillis = remainingMillis;
            return this;
        }

        public OwnerBuilder fencingToken(final long fencingToken) {
            this.fencingToken = fencingToken;
            return this;
        }

        public OwnerBuilder acquires(final long acquires) {
            this.acquires = acquires;
            return this;
        }

        public OwnerBuilder context(final byte[] context) {
            this.context = context;
            return this;
        }

        public OwnerBuilder success(final boolean success) {
            this.success = success;
            return this;
        }
    }
}
