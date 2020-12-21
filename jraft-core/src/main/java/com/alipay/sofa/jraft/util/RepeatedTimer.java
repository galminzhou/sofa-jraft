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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.util.timer.HashedWheelTimer;
import com.alipay.sofa.jraft.util.timer.Timeout;
import com.alipay.sofa.jraft.util.timer.Timer;
import com.alipay.sofa.jraft.util.timer.TimerTask;

/**
 * {@link HashedWheelTimer }
 *
 *
 * Repeatable timer based on java.util.Timer.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-30 3:45:37 PM
 */
public abstract class RepeatedTimer implements Describer {

    public static final Logger LOG  = LoggerFactory.getLogger(RepeatedTimer.class);

    private final Lock         lock = new ReentrantLock();
    /**
     * 实例Timer = HashedWheelTimer
     * @Link com.alipay.sofa.jraft.util.timer.HashedWheelTimer
     */
    private final Timer        timer;
    /**
     * 实例timeout = hashedWheelTimeout
     * @Link com.alipay.sofa.jraft.util.timer.HashedWheelTimer.HashedWheelTimeout
     */
    private Timeout            timeout;
    private boolean            stopped;
    private volatile boolean   running;
    private volatile boolean   destroyed;
    private volatile boolean   invoking;
    private volatile int       timeoutMs;
    private final String       name;

    public int getTimeoutMs() {
        return this.timeoutMs;
    }

    /**
     * 实例化一个timer，RepeatedTimer的run方法是由timer进行回调
     * 在RepeatedTimer中会持有两个对象，一个是timer，一个是timeout
     * @param name
     * @param timeoutMs
     */
    public RepeatedTimer(final String name, final int timeoutMs) {
        this(name, timeoutMs, new HashedWheelTimer(new NamedThreadFactory(name, true), 1, TimeUnit.MILLISECONDS, 2048));
    }

    public RepeatedTimer(final String name, final int timeoutMs, final Timer timer) {
        super();
        this.name = name;
        this.timeoutMs = timeoutMs;
        this.stopped = true;
        this.timer = Requires.requireNonNull(timer, "timer");
    }

    /**
     * 钩子方法，子类实现自己的业务逻辑
     * Subclasses should implement this method for timer trigger.
     */
    protected abstract void onTrigger();

    /**
     * 不同于常规计时器始终按照相同的时间间隔调度任务，
     * RepeatedTimer 定义了一个 RepeatedTimer#adjustTimeout 方法，子类可实现随机时间范围以支持在运行期间对调度间隔进行动态调整。
     * Adjust timeoutMs before every scheduling.
     *
     * @param timeoutMs timeout millis
     * @return timeout millis
     */
    protected int adjustTimeout(final int timeoutMs) {
        return timeoutMs;
    }

    public void run() {
        this.invoking = true;
        try {
            // 调用业务逻辑
            onTrigger();
        } catch (final Throwable t) {
            LOG.error("Run timer failed.", t);
        }
        boolean invokeDestroyed = false;
        this.lock.lock();
        try {
            this.invoking = false;
            // 计时器被停止
            if (this.stopped) {
                this.running = false;
                invokeDestroyed = this.destroyed;
            }
            // 本次任务调度完成，重新发起调度下一轮任务
            else {
                // 重置timeout为Null，提交下一轮任务，以此实现周期性任务调度
                this.timeout = null;
                schedule();
            }
        } finally {
            this.lock.unlock();
        }
        // 在计时器被停止时回调 onDestroy 方法
        if (invokeDestroyed) {
            onDestroy();
        }
    }

    /**
     * Run the timer at once, it will cancel the timer and re-schedule it.
     */
    public void runOnceNow() {
        this.lock.lock();
        try {
            if (this.timeout != null && this.timeout.cancel()) {
                this.timeout = null;
                run();
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Called after destroy timer.
     */
    protected void onDestroy() {
        LOG.info("Destroy timer: {}.", this);
    }

    /**
     * 启动方法的加锁、校验和赋值，目的只为了让start启动一次，而后调用schedule()
     * Start the timer.
     */
    public void start() {
        // ReentrantLock lock, TODO【volatile + CAS】？
        this.lock.lock();
        try {
            // 计时器已经被销毁，不允许再被启动
            if (this.destroyed) {
                return;
            }
            // 计时器处于运行中，不需要再启动
            if (!this.stopped) {
                return;
            }
            this.stopped = false;
            // 标识计时器已经在运行
            if (this.running) {
                return;
            }
            // 启动一次之后失效，下次不允许执行
            this.running = true;
            // 调度执行周期性任务
            schedule();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Restart the timer.
     * It will be started if it's stopped, and it will be restarted if it's running.
     *
     * @author Qing Wang (kingchin1218@gmail.com)
     *
     * 2020-Mar-26 20:38:37 PM
     */
    public void restart() {
        this.lock.lock();
        try {
            if (this.destroyed) {
                return;
            }
            this.stopped = false;
            this.running = true;
            schedule();
        } finally {
            this.lock.unlock();
        }
    }

    private void schedule() {
        // 若Timeout不是Null，说明上一轮的任务还未执行完毕，则调用HashedWheelTimeout的cancel方法（）尝试取消执行
        if (this.timeout != null) {
            this.timeout.cancel();
        }
        // 创建一个新的任务
        final TimerTask timerTask = timeout -> {
            try {
                RepeatedTimer.this.run();
            } catch (final Throwable t) {
                LOG.error("Run timer task failed, taskName={}.", RepeatedTimer.this.name, t);
            }
        };
        // adjustTimeout(this.timeoutMs): 再次获取一个范围随机的超时时间（1s ~ 2s）
        // 提交给HashedWheelTimer延迟运行（以时间轮的方式，有且仅运行一次），
        this.timeout = this.timer.newTimeout(timerTask, adjustTimeout(this.timeoutMs), TimeUnit.MILLISECONDS);
    }

    /**
     * Reset timer with new timeoutMs.
     *
     * @param timeoutMs timeout millis
     */
    public void reset(final int timeoutMs) {
        this.lock.lock();
        this.timeoutMs = timeoutMs;
        try {
            if (this.stopped) {
                return;
            }
            if (this.running) {
                schedule();
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Reset timer with current timeoutMs
     */
    public void reset() {
        this.lock.lock();
        try {
            reset(this.timeoutMs);
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Destroy timer
     */
    public void destroy() {
        boolean invokeDestroyed = false;
        this.lock.lock();
        try {
            if (this.destroyed) {
                return;
            }
            this.destroyed = true;
            if (!this.running) {
                invokeDestroyed = true;
            }
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timeout != null) {
                if (this.timeout.cancel()) {
                    invokeDestroyed = true;
                    this.running = false;
                }
                this.timeout = null;
            }
        } finally {
            this.lock.unlock();
            this.timer.stop();
            if (invokeDestroyed) {
                onDestroy();
            }
        }
    }

    /**
     * Stop timer
     */
    public void stop() {
        this.lock.lock();
        try {
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timeout != null) {
                this.timeout.cancel();
                this.running = false;
                this.timeout = null;
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void describe(final Printer out) {
        final String _describeString;
        this.lock.lock();
        try {
            _describeString = toString();
        } finally {
            this.lock.unlock();
        }
        out.print("  ") //
            .println(_describeString);
    }

    @Override
    public String toString() {
        return "RepeatedTimer{" + "timeout=" + this.timeout + ", stopped=" + this.stopped + ", running=" + this.running
               + ", destroyed=" + this.destroyed + ", invoking=" + this.invoking + ", timeoutMs=" + this.timeoutMs
               + ", name='" + this.name + '\'' + '}';
    }
}
