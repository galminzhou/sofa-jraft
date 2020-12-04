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
package com.alipay.sofa.jraft.util;

import java.io.Serializable;

/**
 * 表示一个服务地址，包括 IP 和端口，raft 节点不允许启动在 0.0.0.0 所有的 IPv4 上，需要明确指定启动的 IP地址。
 * 创建一个地址，绑定在 localhost 的 8080 端口上，如下例：
 *   Endpoint addr = new Endpoint("localhost", 8080);
 *   String s = addr.toString(); // 结果为 localhost:8080
 *   PeerId peer = new PeerId();
 *   boolean success = peer.parse(s);  // 可以从字符串解析出地址，结果为 true
 *
 * A IP address with port.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:29:12 PM
 */
public class Endpoint implements Copiable<Endpoint>, Serializable {

    private static final long serialVersionUID = -7329681263115546100L;

    private String            ip               = Utils.IP_ANY;
    private int               port;
    private String            str;

    public Endpoint() {
        super();
    }

    public Endpoint(String address, int port) {
        super();
        this.ip = address;
        this.port = port;
    }

    public String getIp() {
        return this.ip;
    }

    public int getPort() {
        return this.port;
    }

    @Override
    public String toString() {
        if (str == null) {
            str = this.ip + ":" + this.port;
        }
        return str;
    }

    @Override
    public Endpoint copy() {
        return new Endpoint(this.ip, this.port);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.ip == null ? 0 : this.ip.hashCode());
        result = prime * result + this.port;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Endpoint other = (Endpoint) obj;
        if (this.ip == null) {
            if (other.ip != null) {
                return false;
            }
        } else if (!this.ip.equals(other.ip)) {
            return false;
        }
        return this.port == other.port;
    }
}
