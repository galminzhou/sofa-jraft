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

import com.alipay.sofa.jraft.Closure;
import java.util.Set;

import com.alipay.sofa.jraft.Status;
import com.google.protobuf.Message;

/**
 * 当一个 raft 节点重启的时候，内存中的状态机的状态将会丢失，在启动过程中将重放日志存储中的所有日志，重建整个状态机实例。这就导致 3 个问题：
 *
 * 1. 如果任务提交比较频繁，比如消息中间件这个场景，那么会导致整个重建过程很长，启动缓慢。
 * 2. 如果日志很多，节点需要存储所有的日志，这对存储是一个资源占用，不可持续。
 * 3. 如果增加一个节点，新节点需要从 leader 获取所有的日志重放到状态机，这对 leader 和网络带宽都是不小的负担。
 *
 * 因此，通过引入 snapshot 机制来解决这 3 个问题，所谓 snapshot 就是为当前状态机的最新状态打一个”镜像“单独保存，
 * 在保存成功后，在这个时刻之前的日志就可以删除，减少了日志存储占用；
 * 启动的时候，可以直接加载最新的 snapshot 镜像，然后重放在此之后的日志即可，
 * 如果 snapshot 间隔合理，那么整个重放过程会比较快，加快了启动过程。
 * 最后，新节点的加入，可以先从 leader 拷贝最新的 snapshot 安装到本地状态机，
 * 然后只要拷贝后续的日志即可，可以快速跟上整个 raft group 的进度。
 *
 * 启用 snapshot 需要设置 NodeOptions 的 snapshotUri 属性，也就是 snapshot 存储的路径。
 * 默认会启动一个定时器自动做 snapshot，间隔通过 NodeOptions 的 snapshotIntervalSecs 属性指定，默认 3600 秒，也就是一个小时。
 *
 * 用户也可以主动触发 snapshot，通过 Node 接口的
 *      Node node = ...
 *      Closure done = ...
 *      node.snapshot(done);
 * 结果将通知到 closure 回调。
 *
 * 状态机需要实现下列两个方法：
 * {@link com.alipay.sofa.jraft.StateMachine#onSnapshotSave(SnapshotWriter, Closure)}
 * {@link com.alipay.sofa.jraft.StateMachine#onSnapshotLoad(SnapshotReader)}
 *
 *
 * Represents a state machine snapshot.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-07 10:17:52 AM
 */
public abstract class Snapshot extends Status {

    /**
     * Snapshot metadata file name.
     */
    public static final String JRAFT_SNAPSHOT_META_FILE   = "__raft_snapshot_meta";
    /**
     * Snapshot file prefix.
     */
    public static final String JRAFT_SNAPSHOT_PREFIX      = "snapshot_";
    /** Snapshot uri scheme for remote peer */
    public static final String REMOTE_SNAPSHOT_URI_SCHEME = "remote://";

    /**
     * Get the path of the Snapshot
     */
    public abstract String getPath();

    /**
     * List all the existing files in the Snapshot currently
     */
    public abstract Set<String> listFiles();

    /**
     * Get file meta by fileName.
     */
    public abstract Message getFileMeta(final String fileName);
}
