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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.StampedLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.errors.RouteTableException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;

/**
 * 实现下图最适合的数据结构便是跳表或者二叉树（最接近匹配项查询）；
 * 选择 region 的 startKey 还是 endKey 作为 RegionRouteTable 的 key 也是有讲究的，
 * 比如为什么没有使用endKey? 这主要取决于 region split 的方式：
 *  1）假设 id 为 2 的 region2 [startKey2, endKey2) 分裂
 *  2）它分裂后的两个 region 分别为 id 继续为 2 的 region2 [startKey2, splitKey) 和 id 为 3 的 region3 [splitKey, endKey2)
 *  3）可以再看下图会发现，此时只需要再往 regionRouteTable 添加一个元素 <region3, splitKey> 即可，原来region2 对应的数据是不需要修改的
 *      ⭕️ Write-Operation
 *          a）单 key 写请求路由逻辑很简单，根据 key 查询对应的 region，再对该 region 发起请求即可。
 *          b）如果是一个批量操作写请求，比如 put(List)，那么会对所有 keys 进行 split，
 *            分组后再并行分别请求所属的regionEngine，要注意的是此时无法提供事务保证（提供retry）。
 *      ⭕️ Read-Operation
 *          a）单 key 读请求路由逻辑也很简单，根据 key 查询对应的 region，再对该 region 发起请求即可。
 *          b）如果是一个批量读请求，比如 scan(startKey, endKey)，那么会对所有 keys 进行 split，分组后并行再分别请求所属的 regionEngine。
 *
 * Region routing table.
 *
 * Enter a 'key' or a 'key range', which can calculate the region
 * in which the 'key' is located, and can also calculate all
 * regions of a 'key range' hit.
 *
 * If the pd server is enabled, the routing data will be refreshed
 * from the pd server, otherwise the routing data is completely
 * based on the local configuration.
 *
 * <pre>
 *
 *                                         ┌───────────┐
 *                                         │ input key │
 *                                         └─────┬─────┘
 *                                               │
 *                                               │
 *                                               │
 * ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─        ┌ ─ ─ ─ ─ ─ ─ ┐    │      ┌ ─ ─ ─ ─ ─ ─ ┐           ┌ ─ ─ ─ ─ ─ ─ ┐
 *  startKey1=byte[0] │          startKey2       │         startKey3                 startKey4
 * └ ─ ─ ─ ┬ ─ ─ ─ ─ ─        └ ─ ─ ─│─ ─ ─ ┘    │      └ ─ ─ ─│─ ─ ─ ┘           └ ─ ─ ─│─ ─ ─ ┘
 *         │                         │           │             │                         │
 *         ▼─────────────────────────▼───────────▼─────────────▼─────────────────────────▼─────────────────────────┐
 *         │                         │                         │                         │                         │
 *         │                         │                         │                         │                         │
 *         │         region1         │         region2         │          region3        │         region4         │
 *         │                         │                         │                         │                         │
 *         └─────────────────────────┴─────────────────────────┴─────────────────────────┴─────────────────────────┘
 *
 * </pre>
 *
 * You can seen that the most suitable data structure for implementing the
 * above figure is a skip list or a binary tree (for the closest matches for
 * given search).
 *
 * In addition, selecting the startKey or endKey of the region as the key of
 * the RegionRouteTable is also exquisite.
 *
 * For example, why not use endKey?
 * This depends mainly on the way the region splits:
 *  a) Suppose that region2[startKey2, endKey2) with id 2 is split
 *  b) The two regions after splitting are region2[startKey2, splitKey) with
 *      id continuing to 2 and region3[splitKey, endKey2) with id 3.
 *  c) At this point, you only need to add an element <region3, splitKey> to
 *      the RegionRouteTable. The data of region2 does not need to be modified.
 *
 * @author jiachun.fjc
 */
public class RegionRouteTable {

    private static final Logger              LOG                = LoggerFactory.getLogger(RegionRouteTable.class);

    private static final Comparator<byte[]>  keyBytesComparator = BytesUtil.getDefaultByteArrayComparator();

    private final StampedLock                stampedLock        = new StampedLock();
    private final NavigableMap<byte[], Long> rangeTable         = new TreeMap<>(keyBytesComparator);
    private final Map<Long, Region>          regionTable        = Maps.newHashMap();

    public Region getRegionById(final long regionId) {
        final StampedLock stampedLock = this.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        // validate() emit a load-fence, but no store-fence.  So you should only have
        // load instructions inside a block of tryOptimisticRead() / validate(),
        // because it is meant to the a read-only operation, and therefore, it is fine
        // to use the loadFence() function to avoid re-ordering.
        Region region = safeCopy(this.regionTable.get(regionId));
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                region = safeCopy(this.regionTable.get(regionId));
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return region;
    }

    public void addOrUpdateRegion(final Region region) {
        Requires.requireNonNull(region, "region");
        Requires.requireNonNull(region.getRegionEpoch(), "regionEpoch");
        final long regionId = region.getId();
        final byte[] startKey = BytesUtil.nullToEmpty(region.getStartKey());
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            this.regionTable.put(regionId, region.copy());
            this.rangeTable.put(startKey, regionId);
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    public void splitRegion(final long leftId, final Region right) {
        Requires.requireNonNull(right, "right");
        Requires.requireNonNull(right.getRegionEpoch(), "right.regionEpoch");
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            final Region left = this.regionTable.get(leftId);
            Requires.requireNonNull(left, "left");
            final byte[] leftStartKey = BytesUtil.nullToEmpty(left.getStartKey());
            final byte[] leftEndKey = left.getEndKey();
            final long rightId = right.getId();
            final byte[] rightStartKey = right.getStartKey();
            final byte[] rightEndKey = right.getEndKey();
            Requires.requireNonNull(rightStartKey, "rightStartKey");
            Requires.requireTrue(BytesUtil.compare(leftStartKey, rightStartKey) < 0,
                "leftStartKey must < rightStartKey");
            if (leftEndKey == null || rightEndKey == null) {
                Requires.requireTrue(leftEndKey == rightEndKey, "leftEndKey must == rightEndKey");
            } else {
                Requires.requireTrue(BytesUtil.compare(leftEndKey, rightEndKey) == 0, "leftEndKey must == rightEndKey");
                Requires.requireTrue(BytesUtil.compare(rightStartKey, rightEndKey) < 0,
                    "rightStartKey must < rightEndKey");
            }
            final RegionEpoch leftEpoch = left.getRegionEpoch();
            leftEpoch.setVersion(leftEpoch.getVersion() + 1);
            left.setEndKey(rightStartKey);
            this.regionTable.put(rightId, right.copy());
            this.rangeTable.put(rightStartKey, rightId);
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    public boolean removeRegion(final long regionId) {
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            final Region region = this.regionTable.remove(regionId);
            if (region != null) {
                final byte[] startKey = BytesUtil.nullToEmpty(region.getStartKey());
                return this.rangeTable.remove(startKey) != null;
            }
        } finally {
            stampedLock.unlockWrite(stamp);
        }
        return false;
    }

    /**
     * Returns the region to which the key belongs.
     */
    public Region findRegionByKey(final byte[] key) {
        Requires.requireNonNull(key, "key");
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            return findRegionByKeyWithoutLock(key);
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    private Region findRegionByKeyWithoutLock(final byte[] key) {
        // return the greatest key less than or equal to the given key
        final Map.Entry<byte[], Long> entry = this.rangeTable.floorEntry(key);
        if (entry == null) {
            reportFail(key);
            throw reject(key, "fail to find region by key");
        }
        return this.regionTable.get(entry.getValue());
    }

    /**
     * Returns the list of regions to which the keys belongs.
     */
    public Map<Region, List<byte[]>> findRegionsByKeys(final List<byte[]> keys) {
        Requires.requireNonNull(keys, "keys");
        final Map<Region, List<byte[]>> regionMap = Maps.newHashMap();
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            for (final byte[] key : keys) {
                final Region region = findRegionByKeyWithoutLock(key);
                regionMap.computeIfAbsent(region, k -> Lists.newArrayList()).add(key);
            }
            return regionMap;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    /**
     * Returns the list of regions to which the keys belongs.
     */
    public Map<Region, List<KVEntry>> findRegionsByKvEntries(final List<KVEntry> kvEntries) {
        Requires.requireNonNull(kvEntries, "kvEntries");
        final Map<Region, List<KVEntry>> regionMap = Maps.newHashMap();
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            for (final KVEntry kvEntry : kvEntries) {
                final Region region = findRegionByKeyWithoutLock(kvEntry.getKey());
                regionMap.computeIfAbsent(region, k -> Lists.newArrayList()).add(kvEntry);
            }
            return regionMap;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    /**
     * 一次 scan 的流程
     * 确定 key 区间 [startKey, endKey) 覆盖的 region list；
     * RegionRouteTable 是一个红黑树结构存储的 region 路由表，
     * startKey 为作为红黑树的key， 只要查找 [startKey, endKey) 的子视图再加上一个 floorEntry(startKey) 即可
     * 如下图示例计算得出 [startKey, endKey) 横跨 region1, region2, region3 一共 3 个分区(region1 为 floor entry, region2 和 region3 为子视图部分)
     *
     *                   scan startKey                                       scan endKey
     *                        │                                                  │
     *                        │                                                  │
     *                        │                                                  │
     * ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─    │   ┌ ─ ─ ─ ─ ─ ─ ┐           ┌ ─ ─ ─ ─ ─ ─ ┐      │    ┌ ─ ─ ─ ─ ─ ─ ┐
     *  startKey1=byte[0] │   │      startKey2                 startKey3         │       startKey4
     * └ ─ ─ ─ ┬ ─ ─ ─ ─ ─    │   └ ─ ─ ─│─ ─ ─ ┘           └ ─ ─ ─│─ ─ ─ ┘      │    └ ─ ─ ─│─ ─ ─ ┘
     *         │              │          │                         │             │           │
     *         ▼──────────────▼──────────▼─────────────────────────▼─────────────▼───────────▼─────────────────────────┐
     *         │                         │                         │                         │                         │
     *         │                         │                         │                         │                         │
     *         │         region1         │         region2         │          region3        │         region4         │
     *         │                         │                         │                         │                         │
     *         └─────────────────────────┴─────────────────────────┴─────────────────────────┴─────────────────────────┘
     *
     * 请求分裂: scan -> multi-region scan
     *      region1 -> regionScan(startKey, regionEndKey1)
     *      region2 -> regionScan(regionStartKey2, regionEndKey2)
     *      region3 -> regionScan(regionStartKey3, endKey)
     *
     *                 ┌ ─ ─ ─ ─ ─ ─ ┐         ┌ ─ ─ ─ ─ ─ ─ ┐          ┌ ─ ─ ─ ─ ─ ─ ┐
     *                  call region 1           call region 2            call region 3
     *                 └ ─ ─ ─│─ ─ ─ ┘         └ ─ ─ ─│─ ─ ─ ┘          └ ─ ─ ─│─ ─ ─ ┘
     *                        │                       │                        │
     *                        │                       │                        │
     *                        │                       │                        │
     *         ▼──────────────▼──────────▼────────────▼────────────▼───────────▼─────────────▼─────────────────────────┐
     *         │                         │                         │                         │                         │
     *         │                         │                         │                         │                         │
     *         │         region1         │         region2         │          region3        │         region4         │
     *         │                         │                         │                         │                         │
     *         └─────────────────────────┴─────────────────────────┴─────────────────────────┴─────────────────────────┘
     *
     * 遭遇 region split (分裂的标志是 region epoch 发生变化)
     *      刷新 RegionRouteTable，需要从 PD 获取最新的路由表，比如当前示例中 region2 分裂变成了 region2 + region5
     *          region2 -> regnonScan(regionStartKey2, regionEndKey2)  请求分裂并重试
     *              region2 -> regionScan(regionStartKey2, newRegionEndKey2)
     *              region5 -> regionScan(regionStartKey5, regionEndKey5)
     *
     *                 ┌ ─ ─ ─ ─ ─ ─ ┐         ┌ ─ ─ ─ ─ ─ ─ ┐               ┌ ─ ─ ─ ─ ─ ─ ┐
     *                  call region 1           call region 2                 call region 3
     *                 └ ─ ─ ─│─ ─ ─ ┘         └ ─ ─ ─│─ ─ ─ ┘               └ ─ ─ ─│─ ─ ─ ┘
     *                        │                       │      call region5           │
     *                        │                       │            │                │
     *                        │                       │            │                │
     *         ▼──────────────▼──────────▼────────────▼──────▼─────▼────▼───────────▼─────────────▼─────────────────────────┐
     *         │                         │                   │          │                         │                         │
     *         │                         │                   │          │                         │                         │
     *         │         region1         │         region2   │ region5  │          region3        │         region4         │
     *         │                         │                   │          │                         │                         │
     *         └─────────────────────────┴───────────────────┴──────────┴─────────────────────────┴─────────────────────────┘
     *
     * 遭遇 Invalid Peer (NOT_LEADER 等错误)
     *      这个就很简单了, 重新获取当前 key 区间所属的 raft-group 的最新 leader，再次发起调用即可
     *
     * Returns the list of regions covered by startKey and endKey.
     */
    public List<Region> findRegionsByKeyRange(final byte[] startKey, final byte[] endKey) {
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            final byte[] realStartKey = BytesUtil.nullToEmpty(startKey);
            final NavigableMap<byte[], Long> subRegionMap;
            if (endKey == null) {
                subRegionMap = this.rangeTable.tailMap(realStartKey, false);
            } else {
                subRegionMap = this.rangeTable.subMap(realStartKey, false, endKey, true);
            }
            final List<Region> regionList = Lists.newArrayListWithCapacity(subRegionMap.size() + 1);
            final Map.Entry<byte[], Long> headEntry = this.rangeTable.floorEntry(realStartKey);
            if (headEntry == null) {
                reportFail(startKey);
                throw reject(startKey, "fail to find region by startKey");
            }
            regionList.add(safeCopy(this.regionTable.get(headEntry.getValue())));
            for (final Long regionId : subRegionMap.values()) {
                regionList.add(safeCopy(this.regionTable.get(regionId)));
            }
            return regionList;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    /**
     * Returns the startKey of next region.
     */
    public byte[] findStartKeyOfNextRegion(final byte[] key) {
        Requires.requireNonNull(key, "key");
        final StampedLock stampedLock = this.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        // get the least key strictly greater than the given key
        byte[] nextStartKey = this.rangeTable.higherKey(key);
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                // get the least key strictly greater than the given key
                nextStartKey = this.rangeTable.higherKey(key);
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return nextStartKey;
    }

    // Should be in lock
    //
    // If this method is called, either because the registered region table is incomplete (by user)
    // or because of a bug.
    private void reportFail(final byte[] relatedKey) {
        if (LOG.isErrorEnabled()) {
            LOG.error("There is a high probability that the data in the region table is corrupted.");
            LOG.error("---------------------------------------------------------------------------");
            LOG.error("* RelatedKey:  {}.", BytesUtil.toHex(relatedKey));
            LOG.error("* RangeTable:  {}.", this.rangeTable);
            LOG.error("* RegionTable: {}.", this.regionTable);
            LOG.error("---------------------------------------------------------------------------");
        }
    }

    private static Region safeCopy(final Region region) {
        if (region == null) {
            return null;
        }
        return region.copy();
    }

    private static RouteTableException reject(final byte[] relatedKey, final String message) {
        return new RouteTableException("key: " + BytesUtil.toHex(relatedKey) + ", message: " + message);
    }
}
