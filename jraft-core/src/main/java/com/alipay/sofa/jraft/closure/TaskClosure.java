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
package com.alipay.sofa.jraft.closure;

import com.alipay.sofa.jraft.Closure;

/**
 * 当 jraft 发现 task 的 done 是 TaskClosure 的时候，
 * 会在 RAFT 日志提交到 RAFT group 之后（并复制到多数节点），应用到状态机之前调用 onCommitted 方法。
 *
 * Closure for task applying.
 * @author dennis
 */
public interface TaskClosure extends Closure {

    /**
     * Called when task is committed to majority peers of the
     * RAFT group but before it is applied to state machine.
     * 
     * <strong>Note: user implementation should not block
     * this method and throw any exceptions.</strong>
     */
    void onCommitted();
}
