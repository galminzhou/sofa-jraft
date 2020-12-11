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
package com.alipay.sofa.jraft.entity;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.core.ElectionPriority;
import com.alipay.sofa.jraft.util.AsciiStringUtil;
import com.alipay.sofa.jraft.util.Copiable;
import com.alipay.sofa.jraft.util.CrcUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Utils;

/**
 * 表示一个 raft 协议的参与者（leader/follower/candidate/learner(1.3之后新增，不参与选举不参与投票) etc.)，
 * 它由三元素组成： ip:port:index， IP 就是节点的 IP， port 就是端口，
 * index 表示同一个端口的序列号，目前没有用到，总被认为是 0。
 * 预留此字段是为了支持同一个端口启动不同的 raft 节点，通过 index 区分。
 *
 -----------------------服务器要遵守的规则-------------------------------------------
    所有服务器:
        如果commitIndex > lastApplied, 那么将lastApplied自增并把对应日志log[lastApplied]应用到状态机
        如果RPC请求或响应包含一个term T大于currentTerm, 那么将currentTerm赋值为T并立即切换状态为follower
    Follower（追随者）:
        无条件响应来自candidate和leader的RPC
        如果在选举超时之前没收到任何来自leader的AppendEntries RPC或RequestVote RPC, 那么自己转换状态为candidate
    Candidate（候选者）:
        转变为candidate之后开始发起选举
        currentTerm自增 –> 重置选举计时器 –> 给自己投票 –> 向其他服务器发起RequestVote RPC
        如果收到了来自大多数服务器的投票, 转换状态成为leader
        如果收到了来自新leader的AppendEntries RPC(Heartbeat), 转换状态为follower
        如果选举超时, 开始新一轮的选举
    Leader（领导者）:
        一旦成为leader, 想其他所有服务器发送空的AppendEntries RPC(Heartbeat), 并在空闲时间重复发送以防选举超时
        如果收到来自客户端的请求, 向本地日志追加条目并向所有服务器发送AppendEntries RPC, 在收到大多数响应后将该条目应用到状态机并回复响应给客户端
        如果leader上一次收到的日志索引大于一个follower的nextIndex, 那么通过AppendEntries RPC将nextIndex之后的所有日志发送出去; 如果发送成功, 将follower的nextIndex和matchIndex更新, 如果由于日志不一致导致失败, 那么将nextIndex递减并重新发送
        如果存在一个N > commitIndex和半数以上的matchIndex[i] >= N并且log[N].term == currentTerm, 将commitIndex赋值为N
    Learner（学习者）：
        除了接受AppendEntries 日志条目和 snapshot 快照，不参与其它事件
 ------------------------------------------------------------------
 *
 * 创建一个 PeerId, index 指定为 0， ip 和端口分别是 localhost 和 8080:
 *      PeerId peer = new PeerId("localhost", 8080);
 *      EndPoint addr = peer.getEndpoint(); // 获取节点地址
 *      int index = peer.getIdx(); // 获取节点序号，目前一直为 0
 *
 *      String s = peer.toString(); // 结果为 localhost:8080
 *      boolean success = peer.parse(s);  // 可以从字符串解析出 PeerId，结果为 true
 *
 * Represent a participant in a replicating group.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:27:37 PM
 */
public class PeerId implements Copiable<PeerId>, Serializable, Checksum {

    private static final long   serialVersionUID = 8083529734784884641L;

    private static final Logger LOG              = LoggerFactory.getLogger(PeerId.class);

    /** Peer address. */
    private Endpoint            endpoint         = new Endpoint(Utils.IP_ANY, 0);
    /** Index in same addr, default is 0. */
    private int                 idx;
    /** Cached toString result. */
    private String              str;

    /** Node's local priority value, if node don't support priority election, this value is -1. */
    private int                 priority         = ElectionPriority.Disabled;

    public static final PeerId  ANY_PEER         = new PeerId();

    private long                checksum;

    public PeerId() {
        super();
    }

    @Override
    public long checksum() {
        if (this.checksum == 0) {
            this.checksum = CrcUtil.crc64(AsciiStringUtil.unsafeEncode(toString()));
        }
        return this.checksum;
    }

    /**
     * Create an empty peer.
     * @return empty peer
     */
    public static PeerId emptyPeer() {
        return new PeerId();
    }

    @Override
    public PeerId copy() {
        return new PeerId(this.endpoint.copy(), this.idx, this.priority);
    }

    /**
     * Parse a peer from string in the format of "ip:port:idx",
     * returns null if fail to parse.
     *
     * @param s input string with the format of "ip:port:idx"
     * @return parsed peer
     */
    public static PeerId parsePeer(final String s) {
        final PeerId peer = new PeerId();
        if (peer.parse(s)) {
            return peer;
        }
        return null;
    }

    public PeerId(final Endpoint endpoint, final int idx) {
        super();
        this.endpoint = endpoint;
        this.idx = idx;
    }

    public PeerId(final String ip, final int port) {
        this(ip, port, 0);
    }

    public PeerId(final String ip, final int port, final int idx) {
        super();
        this.endpoint = new Endpoint(ip, port);
        this.idx = idx;
    }

    public PeerId(final Endpoint endpoint, final int idx, final int priority) {
        super();
        this.endpoint = endpoint;
        this.idx = idx;
        this.priority = priority;
    }

    public PeerId(final String ip, final int port, final int idx, final int priority) {
        super();
        this.endpoint = new Endpoint(ip, port);
        this.idx = idx;
        this.priority = priority;
    }

    public Endpoint getEndpoint() {
        return this.endpoint;
    }

    public String getIp() {
        return this.endpoint.getIp();
    }

    public int getPort() {
        return this.endpoint.getPort();
    }

    public int getIdx() {
        return this.idx;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
        this.str = null;
    }

    /**
     * Returns true when ip is ANY_IP, port is zero and idx is zero too.
     */
    public boolean isEmpty() {
        return getIp().equals(Utils.IP_ANY) && getPort() == 0 && this.idx == 0;
    }

    @Override
    public String toString() {
        if (this.str == null) {
            final StringBuilder buf = new StringBuilder(this.endpoint.toString());

            if (this.idx != 0) {
                buf.append(':').append(this.idx);
            }

            if (this.priority != ElectionPriority.Disabled) {
                if (this.idx == 0) {
                    buf.append(':');
                }
                buf.append(':').append(this.priority);
            }

            this.str = buf.toString();
        }
        return this.str;
    }

    /**
     * Parse peerId from string that generated by {@link #toString()}
     * This method can support parameter string values are below:
     *
     * <pre>
     * PeerId.parse("a:b")          = new PeerId("a", "b", 0 , -1)
     * PeerId.parse("a:b:c")        = new PeerId("a", "b", "c", -1)
     * PeerId.parse("a:b::d")       = new PeerId("a", "b", 0, "d")
     * PeerId.parse("a:b:c:d")      = new PeerId("a", "b", "c", "d")
     * </pre>
     *
     */
    public boolean parse(final String s) {
        if (StringUtils.isEmpty(s)) {
            return false;
        }

        final String[] tmps = Utils.parsePeerId(s);
        if (tmps.length < 2 || tmps.length > 4) {
            return false;
        }
        try {
            final int port = Integer.parseInt(tmps[1]);
            this.endpoint = new Endpoint(tmps[0], port);

            switch (tmps.length) {
                case 3:
                    this.idx = Integer.parseInt(tmps[2]);
                    break;
                case 4:
                    if (tmps[2].equals("")) {
                        this.idx = 0;
                    } else {
                        this.idx = Integer.parseInt(tmps[2]);
                    }
                    this.priority = Integer.parseInt(tmps[3]);
                    break;
                default:
                    break;
            }
            this.str = null;
            return true;
        } catch (final Exception e) {
            LOG.error("Parse peer from string failed: {}.", s, e);
            return false;
        }
    }

    /**
     * To judge whether this node can participate in election or not.
     *
     * @return the restul that whether this node can participate in election or not.
     */
    public boolean isPriorityNotElected() {
        return this.priority == ElectionPriority.NotElected;
    }

    /**
     * To judge whether the priority election function is disabled or not in this node.
     *
     * @return the result that whether this node has priority election function or not.
     */
    public boolean isPriorityDisabled() {
        return this.priority <= ElectionPriority.Disabled;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.endpoint == null ? 0 : this.endpoint.hashCode());
        result = prime * result + this.idx;
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PeerId other = (PeerId) obj;
        if (this.endpoint == null) {
            if (other.endpoint != null) {
                return false;
            }
        } else if (!this.endpoint.equals(other.endpoint)) {
            return false;
        }
        return this.idx == other.idx;
    }
}
