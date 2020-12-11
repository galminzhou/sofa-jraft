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
package com.alipay.sofa.jraft.example.counter.rpc;

import java.io.Serializable;

/**
 * 在多个节点（机器）组成的一个 raft group 中保存一个分布式计数器，
 * 该计数器可以递增和获取，并且在所有节点之间保持一致，任何少数节点的挂掉都不会影响对外提供的两个服务：
 *
 * Increment value with delta and get the new value request.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 4:53:22 PM
 */
public class IncrementAndGetRequest implements Serializable {

    private static final long serialVersionUID = -5623664785560971849L;

    private long              delta;

    public long getDelta() {
        return this.delta;
    }

    public void setDelta(long delta) {
        this.delta = delta;
    }
}
