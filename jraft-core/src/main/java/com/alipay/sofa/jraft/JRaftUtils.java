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
package com.alipay.sofa.jraft;

import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.lang.StringUtils;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.BootstrapOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.JRaftServiceLoader;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.timer.RaftTimerFactory;

/**
 * 工具类，为了方便创建 Endpoint/PeerId/Configuration 等对象， jraft 提供了 JRaftUtils 来快捷地从字符串创建出所需要的对象：
 *      Endpoint addr = JRaftUtils.getEndpoint("localhost:8080");
 *      PeerId peer = JRaftUtils.getPeerId("localhost:8080");
 *      // 三个节点组成的 raft group 配置，注意节点之间用逗号隔开
 *      Configuration conf = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083");
 *
 * 从 1.3.0 版本开始， SOFAJRaft 引入了只读成员（学习者：Learner）支持，只读的节点类似 Follower，
 * 将从 Leader 复制日志并应用到本地状态机，但是不参与选举，复制成功也不被认为是多数派的一员。
 * 简而言之，除了复制日志以外，只读成员不参与其他任何 raft 算法过程。
 * 一般应用在为某个服务创建一个只读服务的时候，实现类似读写分离的效果，或者数据冷备等场景。
 *
 * 为一个 raft group 设置一个只读节点非常容易，任何以 /learner 为后缀的节点都将被认为是只读节点：
 *      // 3 节点 raft group 带一个只读节点
 *      Configuration conf = JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083,localhost:8084/learner");
 *
 * 上面就创建了一个 raft 分组，其中普通成员是 localhost:8081,localhost:8082,localhost:8083，
 * 而 localhost:8084 就是一个 learner 只读节点，它带有 /learner 后缀。
 * 可以指定任意多个只读节点，但是由于日志复制都是从 leader 到 follower/learner，
 * 如果有大量学习者的话，可能 leader 的带宽会是一个问题，需要适当留意。
 *
 * Learner 节点的启动和其他 raft node 没有区别，同样可以有 StateMachine 和 Snapshot 机制。同时，只读节点也同样支持线性一致读 readIndex 调用。
 * Some helper methods for jraft usage.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-23 3:48:45 PM
 */
public final class JRaftUtils {

    private final static RaftTimerFactory TIMER_FACTORY = JRaftServiceLoader.load(RaftTimerFactory.class) //
                                                            .first();

    /**
     * Get raft timer factory.
     *
     * @return {@link RaftTimerFactory}
     */
    public static RaftTimerFactory raftTimerFactory() {
        return TIMER_FACTORY;
    }

    /**
     * Bootstrap a non-empty raft node.
     *
     * @param opts options of bootstrap
     * @return true if bootstrap success
     */
    public static boolean bootstrap(final BootstrapOptions opts) throws InterruptedException {
        final NodeImpl node = new NodeImpl();
        final boolean ret = node.bootstrap(opts);
        node.shutdown();
        node.join();
        return ret;
    }

    /**
     * Create a executor with size.
     *
     * @param prefix thread name prefix
     * @param number thread number
     * @return a new {@link ThreadPoolExecutor} instance
     */
    public static Executor createExecutor(final String prefix, final int number) {
        if (number <= 0) {
            return null;
        }
        return ThreadPoolUtil.newBuilder() //
            .poolName(prefix) //
            .enableMetric(true) //
            .coreThreads(number) //
            .maximumThreads(number) //
            .keepAliveSeconds(60L) //
            .workQueue(new SynchronousQueue<>()) //
            .threadFactory(createThreadFactory(prefix)) //
            .build();
    }

    /**
     * Create a thread factory.
     *
     * @param prefixName the prefix name of thread
     * @return a new {@link ThreadFactory} instance
     *
     * @since 0.0.3
     */
    public static ThreadFactory createThreadFactory(final String prefixName) {
        return new NamedThreadFactory(prefixName, true);
    }

    /**
     * Create a configuration from a string in the form of "host1:port1[:idx],host2:port2[:idx]......",
     * returns a empty configuration when string is blank.
     */
    public static Configuration getConfiguration(final String s) {
        final Configuration conf = new Configuration();
        if (StringUtils.isBlank(s)) {
            return conf;
        }
        if (conf.parse(s)) {
            return conf;
        }
        throw new IllegalArgumentException("Invalid conf str:" + s);
    }

    /**
     * Create a peer from a string in the form of "host:port[:idx]",
     * returns a empty peer when string is blank.
     */
    public static PeerId getPeerId(final String s) {
        final PeerId peer = new PeerId();
        if (StringUtils.isBlank(s)) {
            return peer;
        }
        if (peer.parse(s)) {
            return peer;
        }
        throw new IllegalArgumentException("Invalid peer str:" + s);
    }

    /**
     * Create a Endpoint instance from  a string in the form of "host:port",
     * returns null when string is blank.
     */
    public static Endpoint getEndPoint(final String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        final String[] tmps = StringUtils.split(s, ':');
        if (tmps.length != 2) {
            throw new IllegalArgumentException("Invalid endpoint string: " + s);
        }
        return new Endpoint(tmps[0], Integer.parseInt(tmps[1]));
    }

    private JRaftUtils() {
    }
}
