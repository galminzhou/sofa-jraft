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
package com.alipay.sofa.jraft.rhea.metadata;

import java.io.Serializable;

/**
 * StoreHeartbeat
 *
 *
 * StoreEngine -> HeartbeatSender
 *
 * HeartbeatSender 负责发送当前存储节点的心跳，心跳中包含一些状态信息，心跳一共分为两类：StoreHeartbeat 和 RegionHeartbeat
 *
 * PD 不断接受 rheaKV 集群这两类心跳消息，PD 在对 region leader 的心跳回复里面包含了具体调度指令，再以这些信息作为决策依据。
 * 除此之外，PD 还应该可以通过管理接口接收额外的运维指令，用来人为执行更准确的决策。
 * @author jiachun.fjc
 */
public class StoreStats implements Serializable {

    private static final long serialVersionUID = -7818958068467754379L;

    private long              storeId;
    // Store总容量(磁盘)
    // Capacity for the store
    private long              capacity;
    // Store可用容量
    // Available size for the store
    private long              available;
    // Store承载的region数量
    // Total region count in this store
    private int               regionCount;
    // Leader region count in this store
    private int               leaderRegionCount;
    // 正在发送的snapshot数量
    // Current sending snapshot count
    private int               sendingSnapCount;
    // 正在接收的snapshot数量
    // Current receiving snapshot count
    private int               receivingSnapCount;
    // 有多少region正在apply snapshot
    // How many region is applying snapshot
    private int               applyingSnapCount;
    // Store的启动时间 (unix timestamp in milliseconds)
    // When the store is started (unix timestamp in milliseconds)
    private long              startTime;
    // Store是否忙碌
    // If the store is busy
    private boolean           busy;
    // 被Store实际使用的磁盘大小
    // Actually used space by db
    private long              usedSize;
    // 当前一个周期内的写入数据量
    // Bytes written for the store during this period
    private long              bytesWritten;
    // 当前一个周期内的读取数据量
    // Bytes read for the store during this period
    private long              bytesRead;
    // 当前一个周期内写入的key的个数
    // Keys written for the store during this period
    private long              keysWritten;
    // 当前一个周期内读取的key的个数
    // Keys read for the store during this period
    private long              keysRead;
    // 一个周期的具体时间长度
    // Actually reported time interval
    private TimeInterval      interval;

    public long getStoreId() {
        return storeId;
    }

    public void setStoreId(long storeId) {
        this.storeId = storeId;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public long getAvailable() {
        return available;
    }

    public void setAvailable(long available) {
        this.available = available;
    }

    public int getRegionCount() {
        return regionCount;
    }

    public void setRegionCount(int regionCount) {
        this.regionCount = regionCount;
    }

    public int getLeaderRegionCount() {
        return leaderRegionCount;
    }

    public void setLeaderRegionCount(int leaderRegionCount) {
        this.leaderRegionCount = leaderRegionCount;
    }

    public int getSendingSnapCount() {
        return sendingSnapCount;
    }

    public void setSendingSnapCount(int sendingSnapCount) {
        this.sendingSnapCount = sendingSnapCount;
    }

    public int getReceivingSnapCount() {
        return receivingSnapCount;
    }

    public void setReceivingSnapCount(int receivingSnapCount) {
        this.receivingSnapCount = receivingSnapCount;
    }

    public int getApplyingSnapCount() {
        return applyingSnapCount;
    }

    public void setApplyingSnapCount(int applyingSnapCount) {
        this.applyingSnapCount = applyingSnapCount;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public boolean isBusy() {
        return busy;
    }

    public void setBusy(boolean busy) {
        this.busy = busy;
    }

    public long getUsedSize() {
        return usedSize;
    }

    public void setUsedSize(long usedSize) {
        this.usedSize = usedSize;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public long getKeysWritten() {
        return keysWritten;
    }

    public void setKeysWritten(long keysWritten) {
        this.keysWritten = keysWritten;
    }

    public long getKeysRead() {
        return keysRead;
    }

    public void setKeysRead(long keysRead) {
        this.keysRead = keysRead;
    }

    public TimeInterval getInterval() {
        return interval;
    }

    public void setInterval(TimeInterval interval) {
        this.interval = interval;
    }

    @Override
    public String toString() {
        return "StoreStats{" + "storeId=" + storeId + ", capacity=" + capacity + ", available=" + available
               + ", regionCount=" + regionCount + ", leaderRegionCount=" + leaderRegionCount + ", sendingSnapCount="
               + sendingSnapCount + ", receivingSnapCount=" + receivingSnapCount + ", applyingSnapCount="
               + applyingSnapCount + ", startTime=" + startTime + ", busy=" + busy + ", usedSize=" + usedSize
               + ", bytesWritten=" + bytesWritten + ", bytesRead=" + bytesRead + ", keysWritten=" + keysWritten
               + ", keysRead=" + keysRead + ", interval=" + interval + '}';
    }
}
