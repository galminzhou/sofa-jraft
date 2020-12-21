/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.alipay.sofa.jraft.util.timer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 暴露类，主要负责启动Worker线程，添加/取消 Timeout任务，是一个单线程工作环境；
 *
 ------------------------------------------------------------------------------------
 一个Hash Wheel Timer是一个环形结构，可以想象成时钟，
 分为很多格子，一个格子代表一段时间（越短Timer精度越高，在DefaultRaftTimerFactory中，默认Timer: 1ms，共有2048个格子），
 并用一个链表（HashedWheelTimeout 双向链表）保存在该格子上到期的所有任务，
 同时一个指针随着时间流逝一格一格转动（tick++），并执行对应链表（HashedWheelTimeout）中所有到期的任务；
 ------------------------------------------------------------------------------------
 *
 *
 * <h3>Implementation Details</h3>
 * <p>
 * {@link HashedWheelTimer} is based on
 * <a href="http://cseweb.ucsd.edu/users/varghese/">George Varghese</a> and
 * Tony Lauck's paper,
 * <a href="http://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>.  More comprehensive slides are located
 * <a href="http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">here</a>.
 * <p>
 *
 * Forked from <a href="https://github.com/netty/netty">Netty</a>.
 */
public class HashedWheelTimer implements Timer {

    private static final Logger                                      LOG                    = LoggerFactory
                                                                                                .getLogger(HashedWheelTimer.class);
    /** HashedWheelTimer 是一个共享资源（数据比较消耗系统资源，跨JVM使用），
     * 因此不允许创建过多的实例，过多则打印Error日志（配合参数：warnedTooManyInstances），日志打印一次 */
    private static final int                                         INSTANCE_COUNT_LIMIT   = 256;
    /** 实例计数器 */
    private static final AtomicInteger                               instanceCounter        = new AtomicInteger();
    private static final AtomicBoolean                               warnedTooManyInstances = new AtomicBoolean();
    /** workerState WorkerThread，用于控制线程的状态在CAS中使用 */
    private static final AtomicIntegerFieldUpdater<HashedWheelTimer> workerStateUpdater     = AtomicIntegerFieldUpdater
                                                                                                .newUpdater(
                                                                                                    HashedWheelTimer.class,
                                                                                                    "workerState");
    /** mask & (tick++)，推动数组（时钟）进入下一个slot的工作线程 */
    private final Worker                                             worker                 = new Worker();
    private final Thread                                             workerThread;

    /** WORKER_STATE 使用CAS控制线程start */
    public static final int                                          WORKER_STATE_INIT      = 0;
    public static final int                                          WORKER_STATE_STARTED   = 1;
    public static final int                                          WORKER_STATE_SHUTDOWN  = 2;
    @SuppressWarnings({ "unused", "FieldMayBeFinal" })
    private volatile int                                             workerState;                                                  // 0 - init, 1 - started, 2 - shut down

    /** 时间轮数组从wheel[0]到wheel[1]的时间间隔数值，即一个格子代表的时间 */
    private final long                                               tickDuration;
    /** 一个Wheel含有多少个格子， */
    private final HashedWheelBucket[]                                wheel;
    /** wheel.length -1 ，在Worker.run() --- worker.tick & mask, tick++，推动 wheel 到下一个slot */
    private final int                                                mask;
    /** 在start中使用，在HashedWheelTimer.start() 中，因为接收到Task才启动线程，
     * CountDownLatch 确保线程start之后，由runnable -> running 状态（
     * start让线程进入就绪状态，CountDownLatch.await() 确保进入Running状态 ） */
    private final CountDownLatch                                     startTimeInitialized   = new CountDownLatch(1);
    /** TimerTask 并发非阻塞队列，
     * 在每次Tick中允许从此队列中最多获取100_000个元素，根据deadline计算slot放入至wheel数组中；
     * 按照先进先出原则对元素进行排序，新元素从队列尾部插入，而获取队列元素，则需要从队列头部获取。 */
    private final Queue<HashedWheelTimeout>                          timeouts               = new ConcurrentLinkedQueue<>();
    /** TimerTask 存储取消任务的队列，在每次Tick中处理队列内容 */
    private final Queue<HashedWheelTimeout>                          cancelledTimeouts      = new ConcurrentLinkedQueue<>();
    /** 调用newTimer() 添加TimerTask，将统计数加一，若取消则减一，用于统计挂起TimerTask的数量 */
    private final AtomicLong                                         pendingTimeouts        = new AtomicLong(0);
    /** 若maxPendingTimeouts < 0，true则表示在调用newTimers时不抛出挂起任务过多的拒绝异常 */
    private final long                                               maxPendingTimeouts;
    /** 存储WorkThread线程进入Running状态的时间点（纳秒级别），用于计算TimerTask中deadline的值 */
    private volatile long                                            startTime;

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}), default tick duration, and
     * default number of ticks per wheel.
     */
    public HashedWheelTimer() {
        this(Executors.defaultThreadFactory());
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}) and default number of ticks
     * per wheel.
     *
     * @param tickDuration the duration between tick
     * @param unit         the time unit of the {@code tickDuration}
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit) {
        this(Executors.defaultThreadFactory(), tickDuration, unit);
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}).
     *
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
    }

    /**
     * Creates a new timer with the default tick duration and default number of
     * ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @throws NullPointerException if {@code threadFactory} is {@code null}
     */
    public HashedWheelTimer(ThreadFactory threadFactory) {
        this(threadFactory, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new timer with the default number of ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(threadFactory, tickDuration, unit, 512);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, -1);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory      a {@link ThreadFactory} that creates a
     *                           background {@link Thread} which is dedicated to
     *                           {@link TimerTask} execution.
     * @param tickDuration       the duration between tick
     * @param unit               the time unit of the {@code tickDuration}
     * @param ticksPerWheel      the size of the wheel
     * @param maxPendingTimeouts The maximum number of pending timeouts after which call to
     *                           {@code newTimeout} will result in
     *                           {@link RejectedExecutionException}
     *                           being thrown. No maximum pending timeouts limit is assumed if
     *                           this value is 0 or negative.
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit, int ticksPerWheel,
                            long maxPendingTimeouts) {

        if (threadFactory == null) {
            throw new NullPointerException("threadFactory");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (tickDuration <= 0) {
            throw new IllegalArgumentException("tickDuration must be greater than 0: " + tickDuration);
        }
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }

        // Normalize ticksPerWheel to power of two and initialize the wheel.
        // 创建一个时间轮数组，数组长度是不少于ticksPerWheel的2^n，并且不超过2^30，
        // 2^N 主要是用于位运算（例如：或运算（&）），
        // 因为一个2^n的值，都是1...00000（1开头后面都是0）， 2^n-1 则是 0....11111（0开头后面都是1）
        wheel = createWheel(ticksPerWheel);
        mask = wheel.length - 1;

        // Convert tickDuration to nanos.
        this.tickDuration = unit.toNanos(tickDuration);

        // Prevent overflow.
        // 检查是否溢出，即指针转动的时间间隔不能太长而导致 tickDuration*wheel.length>Long.MAX_VALUE
        if (this.tickDuration >= Long.MAX_VALUE / wheel.length) {
            throw new IllegalArgumentException(String.format(
                "tickDuration: %d (expected: 0 < tickDuration in nanos < %d", tickDuration, Long.MAX_VALUE
                                                                                            / wheel.length));
        }
        // 将worker包装成WorkerThread，并不会立即启动时间轮，而是等待第一个任务加入到时间轮的时候才会启动
        workerThread = threadFactory.newThread(worker);
        // maxPendingTimeouts = -1
        this.maxPendingTimeouts = maxPendingTimeouts;
        // 若HashedWheelTimer实例数超过256，则打印一个Error日志
        if (instanceCounter.incrementAndGet() > INSTANCE_COUNT_LIMIT
            && warnedTooManyInstances.compareAndSet(false, true)) {
            reportTooManyInstances();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
        } finally {
            // This object is going to be GCed and it is assumed the ship has sailed to do a proper shutdown. If
            // we have not yet shutdown then we want to make sure we decrement the active instance count.
            if (workerStateUpdater.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                instanceCounter.decrementAndGet();
            }
        }
    }

    private static HashedWheelBucket[] createWheel(int ticksPerWheel) {
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        // 时间轮的数组长度不能够超过 2^30（1073741824）
        if (ticksPerWheel > 1073741824) {
            throw new IllegalArgumentException("ticksPerWheel may not be greater than 2^30: " + ticksPerWheel);
        }

        ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);
        HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
        for (int i = 0; i < wheel.length; i++) {
            wheel[i] = new HashedWheelBucket();
        }
        return wheel;
    }

    /**
     * 获取大于等于ticksPerWheel的2^N的数值
     */
    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        int normalizedTicksPerWheel = 1;
        while (normalizedTicksPerWheel < ticksPerWheel) {
            normalizedTicksPerWheel <<= 1;
        }
        return normalizedTicksPerWheel;
    }

    /**
     * Starts the background thread explicitly.  The background thread will
     * start automatically on demand even if you did not call this method.
     *
     * @throws IllegalStateException if this timer has been
     *                               {@linkplain #stop() stopped} already
     */
    public void start() {
        switch (workerStateUpdater.get(this)) {
            // 若 WorkerThread的状态是初始化，则通过CAS设置为启动状态，CAS=true 则表示当前线程获取到排它锁，启动线程
            case WORKER_STATE_INIT:
                if (workerStateUpdater.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
                    workerThread.start();
                }
                break;
            case WORKER_STATE_STARTED:
                break;
            case WORKER_STATE_SHUTDOWN:
                throw new IllegalStateException("cannot be started once stopped");
            default:
                throw new Error("Invalid WorkerState");
        }

        // Wait until the startTime is initialized by the worker.
        while (startTime == 0) {
            try {
                // ThreadWorker调用start之后，使用await等待run方法中的countDown（ThreadWorker已经处于Running状态）
                startTimeInitialized.await();
            } catch (InterruptedException ignore) {
                // Ignore - it will be ready very soon.
            }
        }
    }

    @Override
    public Set<Timeout> stop() {
        if (Thread.currentThread() == workerThread) {
            throw new IllegalStateException(HashedWheelTimer.class.getSimpleName() + ".stop() cannot be called from "
                                            + TimerTask.class.getSimpleName());
        }

        if (!workerStateUpdater.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
            // workerState can be 0 or 2 at this moment - let it always be 2.
            if (workerStateUpdater.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                instanceCounter.decrementAndGet();
            }

            return Collections.emptySet();
        }

        try {
            boolean interrupted = false;
            while (workerThread.isAlive()) {
                workerThread.interrupt();
                try {
                    workerThread.join(100);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        } finally {
            instanceCounter.decrementAndGet();
        }
        return worker.unprocessedTimeouts();
    }

    /**
     * 若ThreadWorker没有启动则立即启动，启动时间轮之后会为startTime设置为当前时间；
     * 计算延迟时间deadline；
     * 将task任务封装到HashedWheelTimeout中，然后添加到timeouts队列中进行缓存。
     */
    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        long pendingTimeoutsCount = pendingTimeouts.incrementAndGet();

        // maxPendingTimeouts = -1，
        if (maxPendingTimeouts > 0 && pendingTimeoutsCount > maxPendingTimeouts) {
            pendingTimeouts.decrementAndGet();
            throw new RejectedExecutionException("Number of pending timeouts (" + pendingTimeoutsCount
                                                 + ") is greater than or equal to maximum allowed pending "
                                                 + "timeouts (" + maxPendingTimeouts + ")");
        }

        // 如果时间轮没有启动，则启动，并初始化startTime为当前纳秒
        // 启动ThreadWorker线程，并使用CountDownLatch闩锁，等待线程由: Runnable -> Running
        start();

        // Add the timeout to the timeout queue which will be processed on the next tick.
        // During processing all the queued HashedWheelTimeouts will be added to the correct HashedWheelBucket.
        long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;

        // Guard against overflow.
        // 在delay 为正数的情况下，deadline 不可能为负数，
        // 若为deadline为负数，那么一定是超出了Long类型的最大值
        if (delay > 0 && deadline < 0) {
            deadline = Long.MAX_VALUE;
        }

        // 将任务添加到并发容器队列，等待下一次tick(Long tick++, wheel & tick)，将从队列中获取数据放入到时间轮数组中
        // 每次最多取出 100000个任务 - 由 transferTimeoutsToBuckets() 实现/* for (int i = 0; i < 100000; i++) {
        //                HashedWheelTimeout timeout = timeouts.poll();*/
        // ThreadWorker 在run 中处理timeouts的数据（transferTimeoutsToBuckets）
        HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline);
        timeouts.add(timeout);
        return timeout;
    }

    /**
     * Returns the number of pending timeouts of this {@link Timer}.
     */
    public long pendingTimeouts() {
        return pendingTimeouts.get();
    }

    private static void reportTooManyInstances() {
        String resourceType = HashedWheelTimer.class.getSimpleName();
        LOG.error("You are creating too many {} instances.  {} is a shared resource that must be "
                  + "reused across the JVM, so that only a few instances are created.", resourceType, resourceType);
    }

    /**
     * 内部负责添加任务, 累加tick, 执行任务等
     */
    private final class Worker implements Runnable {
        private final Set<Timeout> unprocessedTimeouts = new HashSet<>();

        private long               tick;

        @Override
        public void run() {
            // Initialize the startTime.
            startTime = System.nanoTime();
            if (startTime == 0) {
                // We use 0 as an indicator for the uninitialized value here, so make sure it's not 0 when initialized.
                startTime = 1;
            }

            // Notify the other threads waiting for the initialization at start().
            // 在start()方法中，由线程启动之后，进入await等待，使用countDown将闩锁的计数器减一（初始化为1，所以可执行await之后内容）
            startTimeInitialized.countDown();

            do {
                // 休眠到下一个tick代表的时间到来，deadline = System.nanoTime() - startTime，
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    // 使用或运算算出slot，mask = 2^N - 1 = 01...111(二进制) & tick
                    int idx = (int) (tick & mask);
                    // 将cancelledTimeouts队列中的任务取出来，并将当前的任务从时间轮中移除
                    processCancelledTasks();
                    HashedWheelBucket bucket = wheel[idx];
                    // 将timeouts队列中缓存（newTimeout()方法中加入到待处理定时任务队列）的数据取出加入到时间轮里面
                    transferTimeoutsToBuckets();
                    bucket.expireTimeouts(deadline);
                    tick++;
                }
            // 若workerState 一直为 started 状态，就一直循环处理数据
            } while (workerStateUpdater.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);

            // Fill the unprocessedTimeouts so we can return them from stop() method.
            for (HashedWheelBucket bucket : wheel) {
                bucket.clearTimeouts(unprocessedTimeouts);
            }
            for (;;) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    break;
                }
                // 若存在未处理的timeout，则将它加入到unprocessedTimeouts队列中
                if (!timeout.isCancelled()) {
                    unprocessedTimeouts.add(timeout);
                }
            }
            // 处理被取消的任务
            processCancelledTasks();
        }

        private void transferTimeoutsToBuckets() {
            // transfer only max. 100000 timeouts per tick to prevent a thread to stale the workerThread when it just
            // adds new timeouts in a loop.
            // 每一次tick，最多获取100_000个Task，以防止在一个线程中添加过多而超时导致WorkerThread失效。
            for (int i = 0; i < 100000; i++) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    // all processed
                    break;
                }
                if (timeout.state() == HashedWheelTimeout.ST_CANCELLED) {
                    // Was cancelled in the meantime.
                    continue;
                }

                //
                long calculated = timeout.deadline / tickDuration;
                timeout.remainingRounds = (calculated - tick) / wheel.length;

                final long ticks = Math.max(calculated, tick); // Ensure we don't schedule for past.
                int stopIndex = (int) (ticks & mask);

                HashedWheelBucket bucket = wheel[stopIndex];
                bucket.addTimeout(timeout);
            }
        }

        private void processCancelledTasks() {
            for (;;) {
                HashedWheelTimeout timeout = cancelledTimeouts.poll();
                if (timeout == null) {
                    // all processed
                    break;
                }
                try {
                    timeout.remove();
                } catch (Throwable t) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("An exception was thrown while process a cancellation task", t);
                    }
                }
            }
        }

        /**
         * Calculate goal nanoTime from startTime and current tick number,
         * then wait until that goal has been reached.
         *
         * @return Long.MIN_VALUE if received a shutdown request,
         * current time otherwise (with Long.MIN_VALUE changed by +1)
         */
        private long waitForNextTick() {
            long deadline = tickDuration * (tick + 1);

            for (;;) {
                final long currentTime = System.nanoTime() - startTime;
                long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;

                if (sleepTimeMs <= 0) {
                    if (currentTime == Long.MIN_VALUE) {
                        return -Long.MAX_VALUE;
                    } else {
                        return currentTime;
                    }
                }

                // We decide to remove the original approach (as below) which used in netty for
                // windows platform.
                // See https://github.com/netty/netty/issues/356
                //
                // if (Platform.isWindows()) {
                //     sleepTimeMs = sleepTimeMs / 10 * 10;
                // }
                //
                // The above approach that make sleepTimes to be a multiple of 10ms will
                // lead to severe spin in this loop for several milliseconds, which
                // causes the high CPU usage.
                // See https://github.com/sofastack/sofa-jraft/issues/311
                //
                // According to the regression testing on windows, we haven't reproduced the
                // Thread.sleep() bug referenced in https://www.javamex.com/tutorials/threads/sleep_issues.shtml
                // yet.
                //
                // The regression testing environment:
                // - SOFAJRaft version: 1.2.6
                // - JVM version (e.g. java -version): JDK 1.8.0_191
                // - OS version: Windows 7 ultimate 64 bit
                // - CPU: Intel(R) Core(TM) i7-2670QM CPU @ 2.20GHz (4 cores)

                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException ignored) {
                    if (workerStateUpdater.get(HashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }

        public Set<Timeout> unprocessedTimeouts() {
            return Collections.unmodifiableSet(unprocessedTimeouts);
        }
    }

    /**
     * HashedWheelTimeout 任务的包装类, 链表结构；
     * 使用双向链表的结构，方便加入wheel，记录deadline、remainingRounds、state等信息。
     */
    private static final class HashedWheelTimeout implements Timeout {

        /*------ state start-----*/
        private static final int                                           ST_INIT       = 0;
        private static final int                                           ST_CANCELLED  = 1;
        private static final int                                           ST_EXPIRED    = 2;
        private static final AtomicIntegerFieldUpdater<HashedWheelTimeout> STATE_UPDATER = AtomicIntegerFieldUpdater
                                                                                             .newUpdater(
                                                                                                 HashedWheelTimeout.class,
                                                                                                 "state");
        /*------ state end-------*/
        private final HashedWheelTimer                                     timer;
        private final TimerTask                                            task;
        /** Timer启动时间 - 任务执行时间（任务加入时间 + 任务延迟时间），时间单位：纳秒 */
        private final long                                                 deadline;

        @SuppressWarnings({ "unused", "FieldMayBeFinal", "RedundantFieldInitialization" })
        private volatile int                                               state         = ST_INIT;

        // remainingRounds will be calculated and set by Worker.transferTimeoutsToBuckets() before the
        // HashedWheelTimeout will be added to the correct HashedWheelBucket.
        /**
         * 距离执行任务需要等待的轮数，在任务加入到wheel时计算该值，并在Worker中，每过一轮，该值减一;
         * timeout.remainingRounds = (timeout.deadline / tickDuration - tick) / wheel.length;
         */
        long                                                               remainingRounds;

        // This will be used to chain timeouts in HashedWheelTimerBucket via a double-linked-list.
        // As only the workerThread will act on it there is no need for synchronization / volatile.
        /** 双向链表，因为只有WorkerThread操作，因此不需要synchronization/volatile */
        HashedWheelTimeout                                                 next;
        HashedWheelTimeout                                                 prev;

        // The bucket to which the timeout was added
        /** HashedWheelTimeout 所在的 wheel */
        HashedWheelBucket                                                  bucket;

        HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;
        }

        @Override
        public Timer timer() {
            return timer;
        }

        @Override
        public TimerTask task() {
            return task;
        }

        @Override
        public boolean cancel() {
            // only update the state it will be removed from HashedWheelBucket on next tick.
            // 将TimerTask使用CAS将状态从ST_INIT到ST_CANCELLED，CAS=false（不是初始化状态或已经取消了） 则返回false
            if (!compareAndSetState(ST_INIT, ST_CANCELLED)) {
                return false;
            }
            // If a task should be canceled we put this to another queue which will be processed on each tick.
            // So this means that we will have a GC latency of max. 1 tick duration which is good enough. This way
            // we can make again use of our MpscLinkedQueue and so minimize the locking / overhead as much as possible.
            // 若 CAS=true，则表示第一次调用cancel方法，则添加到cancelledTimeouts队列中，等待下一次tick统一处理；
            // 因为在timeouts队列中会存在多线程操作（HashedWheelTimer.newTimeout()）,
            // 若是使用同一个队列则需要使用锁，将之与timeouts队列分开处理，则不需要关注锁的问题。
            timer.cancelledTimeouts.add(this);
            return true;
        }

        void remove() {
            HashedWheelBucket bucket = this.bucket;
            if (bucket != null) {
                bucket.remove(this);
            } else {
                timer.pendingTimeouts.decrementAndGet();
            }
        }

        public boolean compareAndSetState(int expected, int state) {
            return STATE_UPDATER.compareAndSet(this, expected, state);
        }

        public int state() {
            return state;
        }

        @Override
        public boolean isCancelled() {
            return state() == ST_CANCELLED;
        }

        @Override
        public boolean isExpired() {
            return state() == ST_EXPIRED;
        }

        public void expire() {
            // CAS=false，返回
            if (!compareAndSetState(ST_INIT, ST_EXPIRED)) {
                return;
            }

            // 到期执行Task
            try {
                task.run(this);
            } catch (Throwable t) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + '.', t);
                }
            }
        }

        @Override
        public String toString() {
            final long currentTime = System.nanoTime();
            long remaining = deadline - currentTime + timer.startTime;

            StringBuilder buf = new StringBuilder(192).append(getClass().getSimpleName()).append('(')
                .append("deadline: ");
            if (remaining > 0) {
                buf.append(remaining).append(" ns later");
            } else if (remaining < 0) {
                buf.append(-remaining).append(" ns ago");
            } else {
                buf.append("now");
            }

            if (isCancelled()) {
                buf.append(", cancelled");
            }

            return buf.append(", task: ").append(task()).append(')').toString();
        }
    }

    /**
     * wheel数组元素, 负责存储HashedWheelTimeout链表
     *
     * Bucket that stores HashedWheelTimeouts. These are stored in a linked-list like datastructure to allow easy
     * removal of HashedWheelTimeouts in the middle. Also the HashedWheelTimeout act as nodes themself and so no
     * extra object creation is needed.
     * 双向链表，用来存储HashedWheelTimeout
     */
    private static final class HashedWheelBucket {
        // Used for the linked-list datastructure
        /** 链表结构 */
        private HashedWheelTimeout head;
        private HashedWheelTimeout tail;

        /**
         * Add {@link HashedWheelTimeout} to this bucket.
         * 添加一个HashedWheelTimeout到链表中
         */
        public void addTimeout(HashedWheelTimeout timeout) {
            assert timeout.bucket == null;
            timeout.bucket = this;
            if (head == null) {
                head = tail = timeout;
            } else {
                tail.next = timeout;
                timeout.prev = tail;
                tail = timeout;
            }
        }

        /**
         * Expire all {@link HashedWheelTimeout}s for the given {@code deadline}.
         * 当tick到此wheel时，Worker调用expireTimeout()；根据deadline判断任务是否过期（remainingRounds<=0）;
         * 任务到期就执行，若没有到期，则remainingRounds--，
         * 因为调用expireTimeouts方法则表面在wheel数组中 slot（mask & tick++） 任务已经走过一轮了。
         */
        public void expireTimeouts(long deadline) {
            HashedWheelTimeout timeout = head;

            // process all timeouts
            while (timeout != null) {
                HashedWheelTimeout next = timeout.next;
                // 任务已经到达执行点（remainingRounds：任务执行等待的轮数）
                if (timeout.remainingRounds <= 0) {
                    next = remove(timeout);
                    // 执行任务
                    if (timeout.deadline <= deadline) {
                        timeout.expire();
                    } else {
                        // The timeout was placed into a wrong slot. This should never happen.
                        throw new IllegalStateException(String.format("timeout.deadline (%d) > deadline (%d)",
                            timeout.deadline, deadline));
                    }
                // 任务已经取消，从链表中移除
                } else if (timeout.isCancelled()) {
                    next = remove(timeout);
                } else {
                    // 任务没有到达执行轮数，剩余轮数减一
                    timeout.remainingRounds--;
                }
                timeout = next;
            }
        }

        public HashedWheelTimeout remove(HashedWheelTimeout timeout) {
            HashedWheelTimeout next = timeout.next;
            // remove timeout that was either processed or cancelled by updating the linked-list
            if (timeout.prev != null) {
                timeout.prev.next = next;
            }
            if (timeout.next != null) {
                timeout.next.prev = timeout.prev;
            }

            if (timeout == head) {
                // if timeout is also the tail we need to adjust the entry too
                if (timeout == tail) {
                    tail = null;
                    head = null;
                } else {
                    head = next;
                }
            } else if (timeout == tail) {
                // if the timeout is the tail modify the tail to be the prev node.
                tail = timeout.prev;
            }
            // null out prev, next and bucket to allow for GC.
            timeout.prev = null;
            timeout.next = null;
            timeout.bucket = null;
            timeout.timer.pendingTimeouts.decrementAndGet();
            return next;
        }

        /**
         * Clear this bucket and return all not expired / cancelled {@link Timeout}s.
         */
        public void clearTimeouts(Set<Timeout> set) {
            for (;;) {
                HashedWheelTimeout timeout = pollTimeout();
                if (timeout == null) {
                    return;
                }
                if (timeout.isExpired() || timeout.isCancelled()) {
                    continue;
                }
                set.add(timeout);
            }
        }

        private HashedWheelTimeout pollTimeout() {
            HashedWheelTimeout head = this.head;
            if (head == null) {
                return null;
            }
            HashedWheelTimeout next = head.next;
            if (next == null) {
                tail = this.head = null;
            } else {
                this.head = next;
                next.prev = null;
            }

            // null out prev and next to allow for GC.
            head.next = null;
            head.prev = null;
            head.bucket = null;
            return head;
        }
    }
}
