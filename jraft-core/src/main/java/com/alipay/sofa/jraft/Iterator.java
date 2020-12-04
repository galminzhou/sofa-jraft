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

import java.nio.ByteBuffer;

/**
 * 提交的 task ，在 jraft 内部会做累积批量提交，应用到状态机的是一个 task 迭代器，
 * 通过 com.alipay.sofa.jraft.Iterator 接口表示，一个典型的例子：
 *
 *      Iterator it = ....
 *      //遍历迭代任务列表
 *      while(it.hasNext()){
 *          ByteBuffer data = it.getData(); // 获取当前任务数据
 *          Closure done = it.done();  // 获取当前任务的 closure 回调
 *          long index = it.getIndex();  // 获取任务的唯一日志编号，单调递增， jraft 自动分配
 *          long term = it.getTerm();  // 获取任务的 leader term
 *          ...逻辑处理...
 *          it.next(); // 移到下一个task
 *      }
 * 注意， 如果 task 没有设置 closure，
 * 那么 done 可能会是 null，另外在 follower 节点上， done 也是 null，
 * 因为 done 不会被复制到除了 leader 节点之外的其他 raft 节点。
 *
 * 优化技巧，通常 leader 获取到的 done closure，可以扩展包装一个 closure 类 包含了没有序列化的用户请求，
 * 那么在逻辑处理部分可以直接从 closure 获取到用户请求，无需通过 data 反序列化得到，减少了 leader 的 CPU 开销。
 * 具体参考：Counter
 *
 *
 *
 * Iterator over a batch of committed tasks.
 * @see StateMachine#onApply(Iterator)
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 3:20:15 PM
 */
public interface Iterator extends java.util.Iterator<ByteBuffer> {

    /**
     * Return the data whose content is the same as what was passed to
     * Node#apply(Task) in the leader node.
     */
    ByteBuffer getData();

    /**
     * Return a unique and monotonically increasing identifier of the current task:
     * - Uniqueness guarantees that committed tasks in different peers with
     *    the same index are always the same and kept unchanged.
     * - Monotonicity guarantees that for any index pair i, j (i < j), task
     *    at index |i| must be applied before task at index |j| in all the
     *    peers from the group.
     */
    long getIndex();

    /**
     * Returns the term of the leader which to task was applied to.
     */
    long getTerm();

    /**
     * If done() is non-NULL, you must call done()->Run() after applying this
     * task no matter this operation succeeds or fails, otherwise the
     * corresponding resources would leak.
     *
     * If this task is proposed by this Node when it was the leader of this
     * group and the leadership has not changed before this point, done() is
     * exactly what was passed to Node#apply(Task) which may stand for some
     * continuation (such as respond to the client) after updating the
     * StateMachine with the given task. Otherwise done() must be NULL.
     * */
    Closure done();

    /**
     * Invoked when some critical error occurred. And we will consider the last
     * |ntail| tasks (starting from the last iterated one) as not applied. After
     * this point, no further changes on the StateMachine as well as the Node
     * would be allowed and you should try to repair this replica or just drop it.
     *
     * @param ntail the number of tasks (starting from the last iterated one)  considered as not to be applied.
     * @param st    Status to describe the detail of the error.
     */
    void setErrorAndRollback(final long ntail, final Status st);
}
