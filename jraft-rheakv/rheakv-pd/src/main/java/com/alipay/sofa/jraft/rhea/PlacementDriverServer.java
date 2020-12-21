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
package com.alipay.sofa.jraft.rhea;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.cmd.pd.CreateRegionIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetClusterInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreIdRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.GetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.RegionHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.SetStoreInfoRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverServerOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.util.concurrent.CallerRunsPolicyWithReport;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.Utils;

/**
 * 全局的中心总控节点，负责整个集群的调度，
 * 一个 PD server 可以管理多个集群，集群之间基于 clusterId 隔离；
 * PD server 需要单独部署，当然，很多场景其实并不需要自管理，rheaKV 也支持不启用 PD。
 *
 * Store 集群中的一个物理存储节点，一个 store 包含一个或多个 region；
 * egion 最小的 KV 数据单元，每个 region 都有一个左闭右开的区间 [startKey, endKey),
 * 可根据请求流量/负载/数据量大小等指标自动分裂以及自动副本搬迁；
 *
 * PlacementDriverServer is a role responsible for overall global control.
 *
 * <pre>
 *     ┌───────────────────────────────────────────┐
 *     │ PlacementDriverClient->MetadataRpcClient  │
 *     └───────────────────────────────────────────┘
 *                           │
 *                           │              ┌───────────────────────────────────────────┐
 *                           │              │       StoreEngine->HeartbeatSender        │
 *                           │              └───────────────────────────────────────────┘
 *                           │                                    │
 *                           │                   ┌────────────────┴─────────┐
 *                           │                   │                          │
 *                           │                   ▼                          ▼
 *                           │       ┌──────────────────────┐   ┌───────────────────────┐
 *                  ┌────────┘       │  StoreHeartbeatTask  │   │  RegionHeartbeatTask  │──┐
 *                 rpc               └──────────────────────┘   └───────────────────────┘  │
 *                  │                        │           ▲                 ▲               │
 *                  │                        │           │                 │               │
 *                  │                        │           ▼                 ▼               │
 *                  │  ┌───────rpc───────────┘     ┌─────────────────────────────┐         │
 *                  │  │                           │       StatsCollector        │         │
 *                  │  │                           └─────────────────────────────┘         │
 *                  │  │                                                                   │
 *                  ▼  ▼                                                                   │
 *     ┌──────────────────────────────┐                                                    │
 *     │    PlacementDriverServer     │◀────────────────────────────rpc────────────────────┘
 *     └──────────────────────────────┘
 *                     │
 *                     │
 *                     ▼
 *     ┌──────────────────────────────┐       ┌──────────────────────────────┐
 *     │   PlacementDriverProcessor   │─────▶ │    PlacementDriverService    │
 *     └──────────────────────────────┘       └──────────────────────────────┘
 *                                                            │
 *                                 ┌──────────────────────────┴─────────────┐
 *                                 │                                        │
 *                                 ▼                                        ▼
 *                 ┌──────────────────────────────┐         ┌──────────────────────────────┐
 *                 │           Pipeline           │         │        MetadataStore         │
 *                 └──────────────────────────────┘         └──────────────────────────────┘
 *                                 │
 *                                 │
 *                                 ▼
 *                 ┌──────────────────────────────┐
 *                 │         Handlers ...         │
 *                 └──────────────────────────────┘
 * </pre>
 * ⭕️ PlacementDriverClient -> MetadataClient：MetadataClient 负责从 PD 获取集群元信息以及注册元信息。
 * ⭕️ StoreEngine -> HeartbeatSender：
 *      1) HeartbeatSender 负责发送当前存储节点的心跳，心跳中包含一些状态信息，
 *         心跳一共分为两类：StoreHeartbeat 和 RegionHeartbeat；
 *      2) PD 不断接受 RheaKV 集群这两类心跳消息，
 *         PD 在对 Region Leader 的心跳回复里面包含具体调度指令，再以这些信息作为决策依据。
 *         除此之外，PD 还应该可以通过管理接口接收额外的运维指令，用来人为执行更准确的决策。
 *      3) 两类心跳包含的状态信息详细内容如下：
 *          a) StoreHeartbeat 包括存储节点 Store 容量，
 *             Region 数量，
 *             Snapshot 数量以及写入/读取数据量等 StoreStats 统计明细；
 *          b) RegionHeartbeat 包括 Region 的 Leader 位置，
 *             掉线 Peer 列表，
 *             暂时不 Work 的 Follower 以及写入/读取数据量/Key 的个数等 RegionStats 统计明细。
 * ⭕️ Pipeline：是针对心跳上报 Stats 的计算以及存储处理流水线，处理单元 Handler 可插拔非常方便扩展。
 * ⭕️ MetadataStore：负责集群元信息存储以及查询，存储方面基于内嵌的 RheaKV。
 *
 *
 * @author jiachun.fjc
 */
public class PlacementDriverServer implements Lifecycle<PlacementDriverServerOptions> {

    private static final Logger      LOG = LoggerFactory.getLogger(PlacementDriverServer.class);

    private final ThreadPoolExecutor pdExecutor;

    private PlacementDriverService   placementDriverService;
    private RheaKVStore              rheaKVStore;
    private RegionEngine             regionEngine;

    private boolean                  started;

    public PlacementDriverServer() {
        this(null);
    }

    public PlacementDriverServer(ThreadPoolExecutor pdExecutor) {
        this.pdExecutor = pdExecutor != null ? pdExecutor : createDefaultPdExecutor();
    }

    @Override
    public synchronized boolean init(final PlacementDriverServerOptions opts) {
        if (this.started) {
            LOG.info("[PlacementDriverServer] already started.");
            return true;
        }
        Requires.requireNonNull(opts, "opts");
        final RheaKVStoreOptions rheaOpts = opts.getRheaKVStoreOptions();
        Requires.requireNonNull(rheaOpts, "opts.rheaKVStoreOptions");
        this.rheaKVStore = new DefaultRheaKVStore();
        if (!this.rheaKVStore.init(rheaOpts)) {
            LOG.error("Fail to init [RheaKVStore].");
            return false;
        }
        this.placementDriverService = new DefaultPlacementDriverService(this.rheaKVStore);
        if (!this.placementDriverService.init(opts)) {
            LOG.error("Fail to init [PlacementDriverService].");
            return false;
        }
        final StoreEngine storeEngine = ((DefaultRheaKVStore) this.rheaKVStore).getStoreEngine();
        Requires.requireNonNull(storeEngine, "storeEngine");
        final List<RegionEngine> regionEngines = storeEngine.getAllRegionEngines();
        if (regionEngines.isEmpty()) {
            throw new IllegalArgumentException("Non region for [PlacementDriverServer]");
        }
        if (regionEngines.size() > 1) {
            throw new IllegalArgumentException("Only support single region for [PlacementDriverServer]");
        }
        this.regionEngine = regionEngines.get(0);
        this.rheaKVStore.addLeaderStateListener(this.regionEngine.getRegion().getId(),
            ((DefaultPlacementDriverService) this.placementDriverService));
        addPlacementDriverProcessor(storeEngine.getRpcServer());
        LOG.info("[PlacementDriverServer] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (!this.started) {
            return;
        }
        if (this.rheaKVStore != null) {
            this.rheaKVStore.shutdown();
        }
        if (this.placementDriverService != null) {
            this.placementDriverService.shutdown();
        }
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.pdExecutor);
        this.started = false;
        LOG.info("[PlacementDriverServer] shutdown successfully.");
    }

    public ThreadPoolExecutor getPdExecutor() {
        return pdExecutor;
    }

    public PlacementDriverService getPlacementDriverService() {
        return placementDriverService;
    }

    public RheaKVStore getRheaKVStore() {
        return rheaKVStore;
    }

    public RegionEngine getRegionEngine() {
        return regionEngine;
    }

    public boolean isLeader() {
        return this.regionEngine.isLeader();
    }

    public PeerId getLeaderId() {
        return this.regionEngine.getLeaderId();
    }

    public boolean awaitReady(final long timeoutMillis) {
        final PlacementDriverClient pdClient = this.rheaKVStore.getPlacementDriverClient();
        final Endpoint endpoint = pdClient.getLeader(this.regionEngine.getRegion().getId(), true, timeoutMillis);
        return endpoint != null;
    }

    private void addPlacementDriverProcessor(final RpcServer rpcServer) {
        rpcServer.registerProcessor(new PlacementDriverProcessor<>(RegionHeartbeatRequest.class,
            this.placementDriverService, this.pdExecutor));
        rpcServer.registerProcessor(new PlacementDriverProcessor<>(StoreHeartbeatRequest.class,
            this.placementDriverService, this.pdExecutor));
        rpcServer.registerProcessor(new PlacementDriverProcessor<>(GetClusterInfoRequest.class,
            this.placementDriverService, this.pdExecutor));
        rpcServer.registerProcessor(new PlacementDriverProcessor<>(GetStoreIdRequest.class,
            this.placementDriverService, this.pdExecutor));
        rpcServer.registerProcessor(new PlacementDriverProcessor<>(GetStoreInfoRequest.class,
            this.placementDriverService, this.pdExecutor));
        rpcServer.registerProcessor(new PlacementDriverProcessor<>(SetStoreInfoRequest.class,
            this.placementDriverService, this.pdExecutor));
        rpcServer.registerProcessor(new PlacementDriverProcessor<>(CreateRegionIdRequest.class,
            this.placementDriverService, this.pdExecutor));
    }

    private ThreadPoolExecutor createDefaultPdExecutor() {
        final int corePoolSize = Math.max(Utils.cpus() << 2, 32);
        final String name = "rheakv-pd-executor";
        return ThreadPoolUtil.newBuilder() //
            .poolName(name) //
            .enableMetric(true) //
            .coreThreads(corePoolSize) //
            .maximumThreads(corePoolSize << 2) //
            .keepAliveSeconds(120L) //
            .workQueue(new ArrayBlockingQueue<>(4096)) //
            .threadFactory(new NamedThreadFactory(name, true)) //
            .rejectedHandler(new CallerRunsPolicyWithReport(name, name)) //
            .build();
    }
}
