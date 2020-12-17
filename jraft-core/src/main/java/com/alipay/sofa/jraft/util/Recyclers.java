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
package com.alipay.sofa.jraft.util;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 本类基于Netty-轻量级对象池思想设计
 * 1) 每一个 Recyclers 对象包含一个 ThreadLocal<Stack<T>> threadLocal实例；
 *    每一个线程包含一个 Stack 对象，该 Stack 对象包含一个 DefaultHandle[]，而 DefaultHandle 中有一个属性 T value，用于存储真实对象。
 *    也就是说，每一个被回收的对象都会被包装成一个 DefaultHandle 对象。
 * 2) 每一个 Recyclers 对象包含一个ThreadLocal<Map<Stack<?>, WeakOrderQueue>> delayedRecycled实例；
 *    每一个线程对象包含一个 Map<Stack<?>, WeakOrderQueue>，存储着为其他线程创建的 WeakOrderQueue 对象，
 *    WeakOrderQueue 对象中存储一个以 Head 为首的 Link 数组，每个 Link 对象中存储一个 DefaultHandle[] 数组，用于存放回收对象。
 *
 *  https://www.jianshu.com/p/854b855bd198
 *
 * Light-weight object pool based on a thread-local stack.
 * <p/>
 * Forked from <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @param <T> the type of the pooled object
 */
public abstract class Recyclers<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Recyclers.class);

    /** 唯一ID生成器；<br>
     * 两处使用：1）当前的线程ID；2）WeakOrderQueue的ID； */
    private static final AtomicInteger idGenerator = new AtomicInteger(Integer.MIN_VALUE);

    /** 生成并获取一个唯一ID；用于pushNow()中的item.recycleId和item.lastRecycleId的设定 */
    private static final int OWN_THREAD_ID = idGenerator.getAndIncrement();
    private static final int DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD = 4 * 1024; // Use 4k instances as default.
    /**
     * 每个 Stack 默认的最大容量；默认是4 * 1024；
     * 注意：
     * 1）当maxCapacityPerThread==0时，禁用回收功能；（当maxCapacityPerThread < 0，则默认：4 * 1024）
     * 2）Recyclers 中有且仅有两个地方存储DefaultHandle对象（Stack和Link）；
     *
     * 在Jraft中，可通过属性设置最多存储容量值：
     * 1) JVM启动参数：-Djraft.recyclers.maxCapacityPerThread
     * 2）在JRaft中通过构造器{@link Recyclers#Recyclers(int)}传入Stack容量，
     *    但是，在构造器中设置的容量，不允许超过JVM启动参数或默认的值（4 * 1024）。
     */
    private static final int MAX_CAPACITY_PER_THREAD;
    /**
     * 每个 Stack 初始值的默认的最大容量：默认是256，并且最大上限为256；
     * 在使用中可通过扩容，INITIAL_CAPACITY <= MAX_CAPACITY_PER_THREAD；
     */
    private static final int INITIAL_CAPACITY;

    static {
        // 每个线程的最大对象容量池
        int maxCapacityPerThread = SystemPropertyUtil.getInt("jraft.recyclers.maxCapacityPerThread", DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD);
        if (maxCapacityPerThread < 0) {
            maxCapacityPerThread = DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD;
        }

        MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;

        LOG.info("-Djraft.recyclers.maxCapacityPerThread: {}.", MAX_CAPACITY_PER_THREAD == 0 ? "disabled" : MAX_CAPACITY_PER_THREAD);

        // 设置初始化容量信息（最大默认为256）
        INITIAL_CAPACITY = Math.min(MAX_CAPACITY_PER_THREAD, 256);
    }

    /**
     * 表示一个不需要回收的包装对象，用于在禁止使用Recycler功能时进行占位的功能；
     * 当 recycler.maxCapacityPerThread == 0 时使用。
     */
    public static final Handle NOOP_HANDLE = new Handle() {};
    /**
     * 在Stack中DefaultHandle数组长度最大值
     */
    private final int maxCapacityPerThread;
    /** 线程变量，保存每个线程的对象池信息，通过ThreadLocal使用，避免不同线程之间的竞争;
     *  当首次执行threadLocal.get() 时，会调用threadLocal#initialValue()来创建一个Stack对象。
     * */
    private final ThreadLocal<Stack<T>> threadLocal = new ThreadLocal<Stack<T>>() {
        @Override
        protected Stack<T> initialValue() {
            return new Stack<>(Recyclers.this, Thread.currentThread(), maxCapacityPerThread);
        }
    };

    protected Recyclers() {
        this(MAX_CAPACITY_PER_THREAD);
    }

    protected Recyclers(int maxCapacityPerThread) {
        this.maxCapacityPerThread = Math.min(MAX_CAPACITY_PER_THREAD, Math.max(0, maxCapacityPerThread));
    }

    @SuppressWarnings("unchecked")
    public final T get() {
        /* 最大容量为0，禁止使用回收功能，
           创建一个对象，其Recycler.Handle<T> handle属性为NOOP_HANDLE，
           在 Recyclers#recycle(T, Handle) 中，若 handle == NOOP_HANDLE 则直接返回 false；*/
        if (maxCapacityPerThread == 0) {
            return newObject(NOOP_HANDLE);
        }
        // 获取当前线程的Stack<T>对象，内部结构：DefaultHandle[]；
        Stack<T> stack = threadLocal.get();
        // 获取栈顶元素（DefaultHandle[this.size--]）
        DefaultHandle handle = stack.pop();
        // 若栈顶不存在元素（DefaultHandle[this.size--] == null），则实例化一个DefaultHandle
        if (handle == null) {
            // 新建一个DefaultHandle对象 -> 然后新建T对象 -> 存储到DefaultHandle对象
            // 一个DefaultHandle对象对应一个Object对象（子类实现newObject(handle)）,
            handle = stack.newHandle();
            handle.value = newObject(handle);
        }
        return (T) handle.value;
    }

    /**
     * 回收对象；
     */
    public final boolean recycle(T o, Handle handle) {
        // 不回收操作类型（maxCapacityPerThread == 0）
        if (handle == NOOP_HANDLE) {
            return false;
        }

        DefaultHandle h = (DefaultHandle) handle;

        final Stack<?> stack = h.stack;
        if (h.lastRecycledId != h.recycleId || stack == null) {
            throw new IllegalStateException("recycled already");
        }

        // 因在构建Stack实例时，使用Recyclers.this作为parent，若是不相等则表示不是同一个对象
        if (stack.parent != this) {
            return false;
        }
        if (o != h.value) {
            throw new IllegalArgumentException("o does not belong to handle");
        }
        // DefaultHandle的recycle方法进行回收
        h.recycle();
        return true;
    }

    /**
     * 1）钩子方法，创建一个对象，由子类实现；
     * 2）传入Handle对象，对创建出来的对象进行回收操作
     */
    protected abstract T newObject(Handle handle);

    public final int threadLocalCapacity() {
        return threadLocal.get().elements.length;
    }

    public final int threadLocalSize() {
        return threadLocal.get().size;
    }

    /**
     * 提供对象的回收功能
     * 目前该接口只有两个实现：NOOP_HANDLE和DefaultHandle；
     */
    public interface Handle {}

    static final class DefaultHandle implements Handle {
        //在WeakOrderQueue的add方法中会设置成ID
        //在push方法中设置成为OWN_THREAD_ID
        //在pop方法中设置为0
        private int lastRecycledId;
        //只有在push方法中才会设置OWN_THREAD_ID
        //在pop方法中设置为0
        private int recycleId;

        //当前的DefaultHandle对象所属的Stack
        private Stack<?> stack;
        private Object value;

        DefaultHandle(Stack<?> stack) {
            this.stack = stack;
        }

        /**
         * DefaultHandle在实例化的时候会传入一个stack实例，代表当前实例是属于这个stack的。
         *
         * 所以在调用recycle方法的时候，会判断一下，当前的线程是不是stack所属的线程，
         * 如果是那么直接push到stack里面就好了，不是则调用延迟队列delayedRecycled；
         *
         * 从delayedRecycled队列中获取Map<Stack<?>, WeakOrderQueue> delayedRecycled，
         * 根据stack作为key来获取WeakOrderQueue，然后将当前的DefaultHandle实例放入到WeakOrderQueue中。
         */
        public void recycle() {
            Thread thread = Thread.currentThread();

            final Stack<?> stack = this.stack;
            // Recycler对象做防护性检测
            if (lastRecycledId != recycleId || stack == null) {
                throw new IllegalStateException("recycled already");
            }

            // Stack检测当前线程是否是创建Stack的线程，若是则调用stack#push(this)；
            if (thread == stack.thread) {
                stack.push(this);
                return;
            }
            // we don't want to have a ref to the queue as the value in our weak map
            // so we null it out; to ensure there are no races with restoring it later
            // we impose a memory ordering here (no-op on x86)
            // 不是Stack的创建线程执行回收，执行异线程（延迟）回收逻辑
            // 获取当前线程存储的延迟回收WeakHashMap
            Map<Stack<?>, WeakOrderQueue> delayedRecycled = Recyclers.delayedRecycled.get();
            // 当前 handler 所在的 stack 是否已经在延迟回收的任务队列中
            // 并且 WeakOrderQueue是一个多线程间可以共享的Queue
            WeakOrderQueue queue = delayedRecycled.get(stack);
            if (queue == null) {
                delayedRecycled.put(stack, queue = new WeakOrderQueue(stack, thread));
            }
            queue.add(this);
        }
    }

    private static final ThreadLocal<Map<Stack<?>, WeakOrderQueue>> delayedRecycled = ThreadLocal.withInitial(WeakHashMap::new);

    // a queue that makes only moderate guarantees about visibility: items are seen in the correct order,
    // but we aren't absolutely guaranteed to ever see anything at all, thereby keeping the queue cheap to maintain
    /** WeakOrderQueue核心包含两个方法：
     *  add方法将元素添加到自身的“队列”中，
     *  transfer方法将自己拥有的元素“传输”到Stack中。
     * */
    private static final class WeakOrderQueue {
        private static final int LINK_CAPACITY = 16;

        // Let Link extend AtomicInteger for intrinsics. The Link itself will be used as writerIndex.
        @SuppressWarnings("serial")
        /** Link继承AtomicInteger，AtomicInteger的值存储elements的最新索引 */
        private static final class Link extends AtomicInteger {
            /** DefaultHandle数组用于存储实例 */
            private final DefaultHandle[] elements = new DefaultHandle[LINK_CAPACITY];
            /** 标记存储实例的读取位置索引 */
            private int readIndex;
            /** 当elements容量满了之后，指向下一个Link */
            private Link next;
        }

        // chain of data items
        /** 内部元素的指针（WeakOrderQueue内部存储的是一个Link的链表） */
        private Link head, tail;
        // pointer to another queue of delayed items for the same stack
        /** 指向下一个WeakOrderQueue的指针 */
        private WeakOrderQueue next;
        /** Stack的所属线程
         *  使用软引用 WeakReference，作用是在pop调用{@link Stack#scavengeSome()}的时候，
         *  若该线程对象在外界已经没有强引用了，即owner不存在了，
         *  那么则需要将该线程所包含的WeakOrderQueue的元素释放（回收），然后从链表中删除此Queue；
         *
         *  如果此处用的是强引用，那么虽然外界不再对该线程有强引用，
         *  但是该stack对象还持有强引用（假设开发人员存储了DefaultHandle对象，然后一直不释放，而DefaultHandle对象又持有stack引用），
         *  导致该线程对象无法释放。
         */
        private final WeakReference<Thread> owner;
        private final int id = idGenerator.getAndIncrement();

        WeakOrderQueue(Stack<?> stack, Thread thread) {
            head = tail = new Link();
            owner = new WeakReference<>(thread);
            // 假设线程B和线程C同时回收线程A的对象时，有可能会同时创建一个WeakOrderQueue，就坑同时设置head，所以需要加锁
            synchronized (stackLock(stack)) {
                next = stack.head;
                stack.head = this;
            }
        }

        private Object stackLock(Stack<?> stack) {
            return stack;
        }

        /** 将元素添加到tail指向的Link对象中，如果Link已满则创建一个新的Link实例。 */
        void add(DefaultHandle handle) {
            // 设置handler的最近一次回收的id信息，标记此时暂存的handler是被谁回收的
            handle.lastRecycledId = id;

            Link tail = this.tail;
            int writeIndex;
            // 检查Link对象是否已经满了；
            // 若没满（tail < 'LINK_CAPACITY=8'），则直接添加实例；
            // 若满了（tail = 'LINK_CAPACITY=8'），创建一个Link对象，并将 tail.next 指向新建对象，组成一个Link链表
            if ((writeIndex = tail.get()) == LINK_CAPACITY) {
                // Link链表实例已满，重新添加一个Link对象
                this.tail = tail = tail.next = new Link();
                writeIndex = tail.get();
            }
            tail.elements[writeIndex] = handle;
            // 如果使用者在将DefaultHandle对象压入队列后，将Stack设置为null
            // 但是此处的DefaultHandle是持有stack的强引用的，则Stack对象无法回收；
            // 而且由于此处DefaultHandle是持有stack的强引用，WeakHashMap中对应stack的WeakOrderQueue也无法被回收掉了，导致内存泄漏
            handle.stack = null;
            // we lazy set to ensure that setting stack to null appears before we unnull it in the owning thread;
            // this also means we guarantee visibility of an element in the queue if we see the index updated
            // 使用延迟设置，确保队列中实例的可见性，
            // Link tail，继承AtomicInteger，此处直接操作tail+1，即下一个存储elements的最新索引
            tail.lazySet(writeIndex + 1);
        }

        boolean hasFinalData() {
            return tail.readIndex != tail.get();
        }

        // transfer as many items as we can from this queue to the stack, returning true if any were transferred
        @SuppressWarnings("rawtypes")
        /** 根据stack的容量和自身拥有的实例数，计算出最终需要转移的实例数；之后就是数组的拷贝和指标的调整。 */
        boolean transfer(Stack<?> dst) {
            // 获取第一个Link
            Link head = this.head;
            // true：表示没有存储数据实例的节点，直接返回
            if (head == null) {
                return false;
            }
            // 读下标的索引位置已达到Link的DefaultHandle数组容量，若有下一个Link则指向下一个Link 进行节点转移；
            if (head.readIndex == LINK_CAPACITY) {
                // 已达到Link链表的尾部，直接返回；
                if (head.next == null) {
                    return false;
                }
                // 将当前的Link节点的下一个Link节点赋值给head和this.head.link，进而操作下一个Link节点进行
                this.head = head = head.next;
            }

            // 获取Link节点的 readIndex（读指针），即当前的Link节点的第一个有效元素的位置
            final int srcStart = head.readIndex;
            // 获取Link节点的writeIndex（写指针），即当前的Link节点的最后一个有效元素的位置
            int srcEnd = head.get();
            // 本次可转移的对象数量
            final int srcSize = srcEnd - srcStart;
            if (srcSize == 0) {
                return false;
            }

            // 获取转移元素的目的地Stack中当前的元素个数
            final int dstSize = dst.size;
            // 计算期盼的容量
            final int expectedCapacity = dstSize + srcSize;
            // 期望的容量大小与实际 Stack 所能承载的容量大小进行比对，取最小值
            if (expectedCapacity > dst.elements.length) {
                final int actualCapacity = dst.increaseCapacity(expectedCapacity);
                srcEnd = Math.min(srcStart + actualCapacity - dstSize, srcEnd);
            }

            if (srcStart != srcEnd) {
                // 获取Link节点的DefaultHandle[]
                final DefaultHandle[] srcElems = head.elements;
                // 获取目的地Stack的DefaultHandle[]
                final DefaultHandle[] dstElems = dst.elements;
                // dst数组的大小，会随着元素的迁入而增加，如果最后发现没有增加，那么表示没有迁移成功任何一个元素
                int newDstSize = dstSize;
                // 进行对象转移（读(head）指针 至 写(tail）指针）
                for (int i = srcStart; i < srcEnd; i++) {
                    DefaultHandle element = srcElems[i];
                    // 表明实例还没有被任何一个 Stack 所回收
                    if (element.recycleId == 0) {
                        element.recycleId = element.lastRecycledId;
                    }
                    //  避免实例重复回收
                    else if (element.recycleId != element.lastRecycledId) {
                        throw new IllegalStateException("recycled already");
                    }
                    // 将可转移成功的DefaultHandle元素的stack属性设置为目的地Stack
                    element.stack = dst;
                    // 将DefaultHandle元素转移到目的地Stack的DefaultHandle[newDstSize ++]中
                    dstElems[newDstSize++] = element;
                    // 设置为null，清除暂存的handler信息
                    srcElems[i] = null; // Help GC
                }
                // 将新的newDstSize赋值给目的地Stack的size
                dst.size = newDstSize;

                if (srcEnd == LINK_CAPACITY && head.next != null) {
                    // 将Head指向下一个Link，也就是将当前的Link给回收掉了
                    // 假设之前为Head -> Link1 -> Link2，回收之后为Head -> Link2
                    this.head = head.next;
                }

                // 设置读指针位置
                head.readIndex = srcEnd;
                return true;
            } else {
                // The destination stack is full already.
                return false;
            }
        }
    }

    /**
     * 负责管理缓存对象；
     * 主线程直接从Stack#elements中存取对象，而非主线程回收对象则存入WeakOrderQueue中；
     *
     * 源码解读 {@link DefaultHandle
     *      DefaultHandle对象的包装类，在Recycler中缓存的对象都会包装成DefaultHandle类。
     * }
     * */
    static final class Stack<T> {

        // we keep a queue of per-thread queues, which is appended to once only, each time a new thread other
        // than the stack owner recycles: when we run out of items in our stack we iterate this collection
        // to scavenge those that can be reused. this permits us to incur minimal thread synchronisation whilst
        // still recycling all items.
        /** 关联对应的Recycler */
        final Recyclers<T> parent;
        /** Stack所属主线程 */
        final Thread thread;
        /** Stack数据结构：使用DefaultHandle数组存储数据，主线程回收的对象 */
        private DefaultHandle[] elements;
        /** elements数组最大长度 */
        private final int maxCapacity;
        /** elements中的元素个数，同时也可作为操作数组的下标，
            数组只有elements.length来计算数组容量的函数，没有计算当前数组中的元素个数的函数，所以需要记录 */
        private int size;
        /** 指向WeakOrderQueue元素组成的链表的头部“指针”，用于存放其他线程的对象，非主线程回收的对象 */
        private volatile WeakOrderQueue head;
        /** 当前游标和前一元素的“指针” */
        private WeakOrderQueue cursor, prev;

        Stack(Recyclers<T> parent, Thread thread, int maxCapacity) {
            this.parent = parent;
            this.thread = thread;
            this.maxCapacity = maxCapacity;
            elements = new DefaultHandle[Math.min(INITIAL_CAPACITY, maxCapacity)];
        }

        /**
         * 将DefaultHandle数组长度（翻倍）扩容至期望的容量，扩容上限 MAX_CAPACITY_PER_THREAD；
         */
        int increaseCapacity(int expectedCapacity) {
            int newCapacity = elements.length;
            int maxCapacity = this.maxCapacity;
            do {
                newCapacity <<= 1;
            } while (newCapacity < expectedCapacity && newCapacity < maxCapacity);

            newCapacity = Math.min(newCapacity, maxCapacity);
            if (newCapacity != elements.length) {
                elements = Arrays.copyOf(elements, newCapacity);
            }

            return newCapacity;
        }

        /** 从stack中申请一个对象：
         * 1) 首先获取当前的 Stack 中的 DefaultHandle 对象中的元素个数。
         * 2) 如果为 0，则从其他线程的与当前的 Stack 对象关联的 WeakOrderQueue 中获取元素，
         *    并转移到 Stack 的 DefaultHandle[] 中（每一次 pop 只转移一个有元素的 Link），
         *    如果转移不成功，说明没有元素可用，直接返回 null；
         * 3) 如果转移成功，则重置 size属性 = 转移后的 Stack 的 DefaultHandle[] 的 size，
         *    之后直接获取 Stack 对象中 DefaultHandle[] 的最后一位元素，之后做防护性检测，
         *    最后重置当前的 stack 对象的 size 属性以及获取到的 DefaultHandle 对象的 recycledId 和 lastRecycledId 回收标记，
         *    返回 DefaultHandle 对象。
         *
         * 源码解读 {@link Stack#scavenge()
         *      调用scavengeSome扫描判断是否存在可转移的 Handler，如果没有，那么就返回false，表示没有可用对象；
         * }
         * 源码解读 {@link Stack#scavengeSome()}
         *      1) 首先设置当前操作的 WeakOrderQueue cursor，如果是 Null，则赋值为 stack.head 节点，
         *         如果 stack.head 是 Null，则表明外部线程没有回收过当前线程创建的实例，
         *         外部线程在回收实例的时候会创建一个WeakOrderQueue，并将stack.head 指向新创建的WeakOrderQueue对象，则直接返回 false；
         *         如果不为 null，则继续向下执行；
         *      2) 首先对当前的 cursor 进行元素的转移，如果转移成功，则跳出循环，设置 prev 和 cursor 属性；
         *      3) 如果转移不成功，获取下一个线程 Y 中的与当前线程的 Stack 实例关联的 WeakOrderQueue，
         *         如果该 queue 所属的线程 Y 还可达，则直接设置 cursor 为该 queue，进行下一轮循环；
         *         如果该 queue 所属的线程 Y 不可达了，则判断其内是否还有元素，
         *         如果有，全部转移到当前线程的 Stack 中，之后将线程 Y 的 queue 从查询 queue 链表中移除。
         * 源码解读 {@link WeakOrderQueue#transfer(Stack)
         *      1) 寻找 cursor 节点中的第一个 Link如果为 null，则表示没有数据，直接返回；
         *      2) 如果第一个 Link 节点的 readIndex 索引已经到达该 Link 对象的 DefaultHandle[] 的尾部，
         *         则判断当前的 Link 节点的下一个节点是否为 null，如果为 null，说明已经达到了 Link 链表尾部，直接返回，
         *         否则，将当前的 Link 节点的下一个 Link 节点赋值给 head ，进而对下一个 Link 节点进行操作；
         *      3) 获取 Link 节点的 readIndex，即当前的 Link 节点的第一个有效元素的位置
         *      4) 获取 Link 节点的 writeIndex，即当前的 Link 节点的最后一个有效元素的位置
         *      5) 计算 Link 节点中可以被转移的元素个数，如果为 0，表示没有可转移的元素，直接返回
         *      6) 获取转移元素的目标 Stack 中当前的元素个数（dstSize）并计算期盼的容量 expectedCapacity，
         *         如果 expectedCapacity 大于目标Stack 的长度（dst.elements.length），
         *         则先对目的地 Stack 进行扩容，计算 Link 中最终的可转移的最后一个元素的下标；
         *      7) 如果发现目的地 Stack 已经满了（ srcStart ！= srcEnd为false），则直接返回 false
         *      8) 获取 Link 节点的 DefaultHandle[] （srcElems）和目标 Stack 的 DefaultHandle[]（dstElems）
         *      9) 根据可转移的起始位置和结束位置对 Link 节点的 DefaultHandle[] 进行循环操作
         *     10) 将可转移成功的 DefaultHandle 元素的stack属性设置为目标 Stack（element.stack = dst），
         *         将 DefaultHandle 元素转移到目的地 Stack 的 DefaultHandle[newDstSize++] 中，最后置空 Link 节点的 DefaultHandle[i]
         * `   11) 如果当前被遍历的 Link 节点的 DefaultHandle[] 已经被掏空了（srcEnd == LINK_CAPACITY），
         *         并且该 Link 节点还有下一个 Link 节点
         *     12) 重置当前 Link 的 readIndex
         * }
         * */
        DefaultHandle pop() {
            // size 表示整个stack的数据大小即可用实例
            int size = this.size;
            // true: 当前线程的Stack没有可用的实例，需从其它线程中获取
            if (size == 0) {
                // 遍历Stack的WeakOrderQueue获取可用的实例，若Queue中没有可用实例，则遍历下一个Queue；
                if (!scavenge()) {
                    return null;
                }
                //由于在transfer(Stack<?> dst)的过程中，可能会将其他线程的WeakOrderQueue中的DefaultHandle对象传递到当前的Stack,
                //所以size发生了变化，需要重新赋值
                size = this.size;
            }
            size--;
            // 获取数组最后一个数据，即栈顶元素
            DefaultHandle ret = elements[size];
            if (ret.lastRecycledId != ret.recycleId) {
                throw new IllegalStateException("recycled multiple times");
            }
            // 清空回收信息，以便判断是否重复回收
            ret.recycleId = 0;
            ret.lastRecycledId = 0;
            this.size = size;
            return ret;
        }

        boolean scavenge() {
            // continue an existing scavenge, if any
            // 扫描判断是否存在可转移的 Handler
            if (scavengeSome()) {
                return true;
            }

            // reset our scavenge cursor
            prev = null;
            cursor = head;
            return false;
        }

        boolean scavengeSome() {
            WeakOrderQueue cursor = this.cursor;
            if (cursor == null) {
                cursor = head;
                // 如果head==null，表示当前的Stack对象没有WeakOrderQueue，直接返回
                if (cursor == null) {
                    return false;
                }
            }

            boolean success = false;
            WeakOrderQueue prev = this.prev;
            do {
                // 从当前的WeakOrderQueue节点进行 handler 的转移
                if (cursor.transfer(this)) {
                    success = true;
                    break;
                }

                // 遍历下一个WeakOrderQueue
                WeakOrderQueue next = cursor.next;
                // 如果 WeakOrderQueue 的实际持有线程因GC回收了
                if (cursor.owner.get() == null) {
                    // If the thread associated with the queue is gone, unlink it, after
                    // performing a volatile read to confirm there is no data left to collect.
                    // We never unlink the first queue, as we don't want to synchronize on updating the head.
                    // 如果当前的WeakOrderQueue的线程已经不可达了
                    // 如果该WeakOrderQueue中有数据，则将其中的数据全部转移到当前Stack中
                    if (cursor.hasFinalData()) {
                        for (;;) {
                            if (cursor.transfer(this)) {
                                success = true;
                            } else {
                                break;
                            }
                        }
                    }
                    // 将当前的WeakOrderQueue的前一个节点prev指向当前的WeakOrderQueue的下一个节点，
                    // 即将当前的WeakOrderQueue从Queue链表中移除。Help GC
                    if (prev != null) {
                        prev.next = next;
                    }
                } else {
                    prev = cursor;
                }

                cursor = next;

            } while (cursor != null && !success);

            this.prev = prev;
            this.cursor = cursor;
            return success;
        }

        /**
         * 同线程回收对象 DefaultHandle#recycle 步骤（Stack 主线程执行回收逻辑）：
         *
         * 1) 首先判断是否重复回收，然后判断 stack 的 DefaultHandle[] 中的元素个数是否已经超过最大容量（4k），如果是，直接返回；
         * 2) 如果不是，则计算当前元素是否需要回收，如果不需要回收，直接返回；
         *    如果需要回收，则判断当前的 DefaultHandle[] 是否还有空位，如果没有，以 maxCapacity 为最大边界扩容 2 倍，
         *    之后拷贝旧数组的元素到新数组，然后将当前的 DefaultHandle 对象放置到 DefaultHandle[] 中；
         * 3) 最后重置 stack.size 属性；
         */
        void push(DefaultHandle item) {
            // (item.recycleId | item.lastRecycleId) != 0 等价于 item.recycleId!=0 && item.lastRecycleId!=0
            // 当item开始创建时item.recycleId==0 && item.lastRecycleId==0
            // 当item被recycle时，item.recycleId==x，item.lastRecycleId==y 进行赋值
            // 当item被pop之后， item.recycleId = item.lastRecycleId = 0
            // 所以当item.recycleId 和 item.lastRecycleId 任何一个不为0，则表示回收过
            if ((item.recycleId | item.lastRecycledId) != 0) {
                throw new IllegalStateException("recycled already");
            }
            // 设置对象的回收id为线程id信息，标记自己的被回收的线程信息
            item.recycleId = item.lastRecycledId = OWN_THREAD_ID;

            int size = this.size;
            if (size >= maxCapacity) {
                // Hit the maximum capacity - drop the possibly youngest object.
                return;
            }
            // stack中的elements扩容两倍，复制元素，将新数组赋值给stack.elements
            if (size == elements.length) {
                elements = Arrays.copyOf(elements, Math.min(size << 1, maxCapacity));
            }

            elements[size] = item;
            this.size = size + 1;
        }

        DefaultHandle newHandle() {
            return new DefaultHandle(this);
        }
    }
}
