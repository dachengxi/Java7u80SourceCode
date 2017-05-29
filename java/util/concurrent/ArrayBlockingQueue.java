/*
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

/*
 *
 *
 *
 *
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package java.util.concurrent;
import java.util.concurrent.locks.*;
import java.util.*;

/**
 * A bounded {@linkplain BlockingQueue blocking queue} backed by an
 * array.  This queue orders elements FIFO (first-in-first-out).  The
 * <em>head</em> of the queue is that element that has been on the
 * queue the longest time.  The <em>tail</em> of the queue is that
 * element that has been on the queue the shortest time. New elements
 * are inserted at the tail of the queue, and the queue retrieval
 * operations obtain elements at the head of the queue.
 *
 * <p>This is a classic &quot;bounded buffer&quot;, in which a
 * fixed-sized array holds elements inserted by producers and
 * extracted by consumers.  Once created, the capacity cannot be
 * changed.  Attempts to {@code put} an element into a full queue
 * will result in the operation blocking; attempts to {@code take} an
 * element from an empty queue will similarly block.
 *
 * <p>This class supports an optional fairness policy for ordering
 * waiting producer and consumer threads.  By default, this ordering
 * is not guaranteed. However, a queue constructed with fairness set
 * to {@code true} grants threads access in FIFO order. Fairness
 * generally decreases throughput but reduces variability and avoids
 * starvation.
 *
 * <p>This class and its iterator implement all of the
 * <em>optional</em> methods of the {@link Collection} and {@link
 * Iterator} interfaces.
 *
 * <p>This class is a member of the
 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
 * Java Collections Framework</a>.
 *
 * @since 1.5
 * @author Doug Lea
 * @param <E> the type of elements held in this collection
 */
public class ArrayBlockingQueue<E> extends AbstractQueue<E>
        implements BlockingQueue<E>, java.io.Serializable {

    /**
     * Serialization ID. This class relies on default serialization
     * even for the items array, which is default-serialized, even if
     * it is empty. Otherwise it could not be declared final, which is
     * necessary here.
     */
    private static final long serialVersionUID = -817911632652898426L;

    /** The queued items */
    //存放元素的数组
    final Object[] items;

    /** items index for next take, poll, peek or remove */
    //获取的索引
    int takeIndex;

    /** items index for next put, offer, or add */
    //添加的索引
    int putIndex;

    /** Number of elements in the queue */
    //Queue中元素的数量
    int count;

    /*
     * Concurrency control uses the classic two-condition algorithm
     * found in any textbook.
     */

    /** Main lock guarding all access */
    //锁
    final ReentrantLock lock;
    /** Condition for waiting takes */
    //队列非空的条件，如果队列为空，获取元素的线程会调用notEmpty.await来等待队列变成非空的情况
    private final Condition notEmpty;
    /** Condition for waiting puts */
    //队列非满的条件，如果队列满了，放入元素的线程会调用notFull.await来等待队列变成有空位的情况
    private final Condition notFull;

    // Internal helper methods

    /**
     * Circularly increment i.
     */
    final int inc(int i) {
        //如果增长1之后，变成了队列的长度，需要从0开始
        return (++i == items.length) ? 0 : i;
    }

    /**
     * Circularly decrement i.
     */
    final int dec(int i) {
        return ((i == 0) ? items.length : i) - 1;
    }

    @SuppressWarnings("unchecked")
    static <E> E cast(Object item) {
        return (E) item;
    }

    /**
     * Returns item at index i.
     * 返回指定索引位置的元素
     */
    final E itemAt(int i) {
        return this.<E>cast(items[i]);
    }

    /**
     * Throws NullPointerException if argument is null.
     *
     * @param v the element
     */
    private static void checkNotNull(Object v) {
        if (v == null)
            throw new NullPointerException();
    }

    /**
     * Inserts element at current put position, advances, and signals.
     * Call only when holding lock.
     * 插入元素
     */
    private void insert(E x) {
        //在putInde处插入元素
        items[putIndex] = x;
        //punIndex加1
        putIndex = inc(putIndex);
        //元素个数加1
        ++count;
        //唤醒等待不为空条件的线程
        notEmpty.signal();
    }

    /**
     * Extracts element at current take position, advances, and signals.
     * Call only when holding lock.
     * 提取元素
     */
    private E extract() {
        //存储队列元素的数组
        final Object[] items = this.items;
        //takeIndex处的元素
        E x = this.<E>cast(items[takeIndex]);
        //提取后删除
        items[takeIndex] = null;
        //takeIndex加1
        takeIndex = inc(takeIndex);
        //总数减1
        --count;
        //唤醒等待不满条件的线程
        notFull.signal();
        //返回提取的元素
        return x;
    }

    /**
     * Deletes item at position i.
     * Utility for remove and iterator.remove.
     * Call only when holding lock.
     * 删除指定位置的元素
     */
    void removeAt(int i) {
        //存放元素的数组
        final Object[] items = this.items;
        // if removing front item, just advance
        //i==takeIndex，最前面的元素
        if (i == takeIndex) {
            //设为null
            items[takeIndex] = null;
            //takeIndex加1
            takeIndex = inc(takeIndex);
        } else {//在中间
            // slide over all others up through putIndex.
            for (;;) {
                int nexti = inc(i);
                //将i元素后面的元素往前移动一个位置
                if (nexti != putIndex) {
                    items[i] = items[nexti];
                    i = nexti;
                } else {//要移除的元素是putIndex的前一个元素，也就是队尾，此时可以直接移除
                    items[i] = null;
                    putIndex = i;
                    break;
                }
            }
        }
        //移除后总数减1
        --count;
        //移除后唤醒等待队列不满的线程
        notFull.signal();
    }

    /**
     * Creates an {@code ArrayBlockingQueue} with the given (fixed)
     * capacity and default access policy.
     *
     * @param capacity the capacity of this queue
     * @throws IllegalArgumentException if {@code capacity < 1}
     * 指定容量构造，默认为非公平
     */
    public ArrayBlockingQueue(int capacity) {
        this(capacity, false);
    }

    /**
     * Creates an {@code ArrayBlockingQueue} with the given (fixed)
     * capacity and the specified access policy.
     *
     * @param capacity the capacity of this queue
     * @param fair if {@code true} then queue accesses for threads blocked
     *        on insertion or removal, are processed in FIFO order;
     *        if {@code false} the access order is unspecified.
     * @throws IllegalArgumentException if {@code capacity < 1}
     * 构造
     */
    public ArrayBlockingQueue(int capacity, boolean fair) {
        if (capacity <= 0)
            throw new IllegalArgumentException();
        //存放元素的数组
        this.items = new Object[capacity];
        //锁
        lock = new ReentrantLock(fair);
        //不为空的条件
        notEmpty = lock.newCondition();
        //非满的条件
        notFull =  lock.newCondition();
    }

    /**
     * Creates an {@code ArrayBlockingQueue} with the given (fixed)
     * capacity, the specified access policy and initially containing the
     * elements of the given collection,
     * added in traversal order of the collection's iterator.
     *
     * @param capacity the capacity of this queue
     * @param fair if {@code true} then queue accesses for threads blocked
     *        on insertion or removal, are processed in FIFO order;
     *        if {@code false} the access order is unspecified.
     * @param c the collection of elements to initially contain
     * @throws IllegalArgumentException if {@code capacity} is less than
     *         {@code c.size()}, or less than 1.
     * @throws NullPointerException if the specified collection or any
     *         of its elements are null
     * 指定集合构造
     */
    public ArrayBlockingQueue(int capacity, boolean fair,
                              Collection<? extends E> c) {
        this(capacity, fair);
        //将集合中的元素放到队列中，需要加锁
        final ReentrantLock lock = this.lock;
        lock.lock(); // Lock only for visibility, not mutual exclusion
        try {
            int i = 0;
            try {
                for (E e : c) {
                    checkNotNull(e);
                    items[i++] = e;
                }
            } catch (ArrayIndexOutOfBoundsException ex) {
                throw new IllegalArgumentException();
            }
            count = i;
            putIndex = (i == capacity) ? 0 : i;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Inserts the specified element at the tail of this queue if it is
     * possible to do so immediately without exceeding the queue's capacity,
     * returning {@code true} upon success and throwing an
     * {@code IllegalStateException} if this queue is full.
     *
     * @param e the element to add
     * @return {@code true} (as specified by {@link Collection#add})
     * @throws IllegalStateException if this queue is full
     * @throws NullPointerException if the specified element is null
     * 队尾添加元素，队列满的话抛出异常
     */
    public boolean add(E e) {
        return super.add(e);
    }

    /**
     * Inserts the specified element at the tail of this queue if it is
     * possible to do so immediately without exceeding the queue's capacity,
     * returning {@code true} upon success and {@code false} if this queue
     * is full.  This method is generally preferable to method {@link #add},
     * which can fail to insert an element only by throwing an exception.
     *
     * @throws NullPointerException if the specified element is null
     * 队尾添加元素，队列满的话返回false
     */
    public boolean offer(E e) {
        //队列中元素不能为null
        checkNotNull(e);
        //加锁
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            //队列已满，返回false
            if (count == items.length)
                return false;
            else {
                //在putIndex处插入元素
                insert(e);
                return true;
            }
        } finally {
            //解锁
            lock.unlock();
        }
    }

    /**
     * Inserts the specified element at the tail of this queue, waiting
     * for space to become available if the queue is full.
     *
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     * 队尾插入元素，队列满了的话，会阻塞等待，直到队列不满
     */
    public void put(E e) throws InterruptedException {
        //先检查null
        checkNotNull(e);
        //加锁
        final ReentrantLock lock = this.lock;
        //锁可中断
        lock.lockInterruptibly();
        try {
            //队列满
            while (count == items.length)
                //需要等待队列变成不为满
                notFull.await();
            //队列此时可以插入元素
            insert(e);
        } finally {
            //解锁
            lock.unlock();
        }
    }

    /**
     * Inserts the specified element at the tail of this queue, waiting
     * up to the specified wait time for space to become available if
     * the queue is full.
     *
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     * 队尾插入元素，可以指定阻塞的超时时间，超过了指定的时间，返回false
     */
    public boolean offer(E e, long timeout, TimeUnit unit)
        throws InterruptedException {

        checkNotNull(e);
        //超时时间
        long nanos = unit.toNanos(timeout);
        //加锁
        final ReentrantLock lock = this.lock;
        //锁可中断
        lock.lockInterruptibly();
        try {
            //队列满
            while (count == items.length) {
                //超时了
                if (nanos <= 0)
                    return false;
                //awaitNanos在AQS中实现，时间会减少
                nanos = notFull.awaitNanos(nanos);
            }
            //队列没有满，插入
            insert(e);
            return true;
        } finally {
            //解锁
            lock.unlock();
        }
    }

    /**
     * 获取元素，队列为空的话，返回null
     * @return
     */
    public E poll() {
        //加锁
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            //队列为空返回null。不为空使用提取方法提取元素
            return (count == 0) ? null : extract();
        } finally {
            //解锁
            lock.unlock();
        }
    }

    /**
     * 获取元素，如果队列为空会阻塞
     * @return
     * @throws InterruptedException
     */
    public E take() throws InterruptedException {
        //加锁
        final ReentrantLock lock = this.lock;
        //锁可中断
        lock.lockInterruptibly();
        try {
            //队列为空
            while (count == 0)
                //当前线程会等待队列变成不为空
                notEmpty.await();
            //队列不为空，直接提取
            return extract();
        } finally {
            //解锁
            lock.unlock();
        }
    }

    /**
     * 获取元素，可以指定超时时间，超时之后返回null
     * @param timeout
     * @param unit
     * @return
     * @throws InterruptedException
     */
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        //超时时间
        long nanos = unit.toNanos(timeout);
        //加锁
        final ReentrantLock lock = this.lock;
        //锁可中断
        lock.lockInterruptibly();
        try {
            //队列为空
            while (count == 0) {
                //超时时间已到，返回null
                if (nanos <= 0)
                    return null;
                //在AQS中实现
                nanos = notEmpty.awaitNanos(nanos);
            }
            //队列不为空，提取元素
            return extract();
        } finally {
            //解锁
            lock.unlock();
        }
    }

    /**
     * 返回队头元素，但是不删除
     * @return
     */
    public E peek() {
        //加锁
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            //队列为空返回null，不为空返回takeIndex的元素
            return (count == 0) ? null : itemAt(takeIndex);
        } finally {
            //解锁
            lock.unlock();
        }
    }

    // this doc comment is overridden to remove the reference to collections
    // greater in size than Integer.MAX_VALUE
    /**
     * Returns the number of elements in this queue.
     *
     * @return the number of elements in this queue
     * Queue的大小
     */
    public int size() {
        //获取大小之前需要加锁
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    // this doc comment is a modified copy of the inherited doc comment,
    // without the reference to unlimited queues.
    /**
     * Returns the number of additional elements that this queue can ideally
     * (in the absence of memory or resource constraints) accept without
     * blocking. This is always equal to the initial capacity of this queue
     * less the current {@code size} of this queue.
     *
     * <p>Note that you <em>cannot</em> always tell if an attempt to insert
     * an element will succeed by inspecting {@code remainingCapacity}
     * because it may be the case that another thread is about to
     * insert or remove an element.
     * 剩余容量
     */
    public int remainingCapacity() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            //数组长度减去元素数量
            return items.length - count;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes a single instance of the specified element from this queue,
     * if it is present.  More formally, removes an element {@code e} such
     * that {@code o.equals(e)}, if this queue contains one or more such
     * elements.
     * Returns {@code true} if this queue contained the specified element
     * (or equivalently, if this queue changed as a result of the call).
     *
     * <p>Removal of interior elements in circular array based queues
     * is an intrinsically slow and disruptive operation, so should
     * be undertaken only in exceptional circumstances, ideally
     * only when the queue is known not to be accessible by other
     * threads.
     *
     * @param o element to be removed from this queue, if present
     * @return {@code true} if this queue changed as a result of the call
     * 删除元素
     */
    public boolean remove(Object o) {
        //不为null
        if (o == null) return false;
        //队列数组
        final Object[] items = this.items;
        //加锁
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            //遍历存在的元素
            for (int i = takeIndex, k = count; k > 0; i = inc(i), k--) {
                if (o.equals(items[i])) {
                    //删除指定位置的元素
                    removeAt(i);
                    return true;
                }
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns {@code true} if this queue contains the specified element.
     * More formally, returns {@code true} if and only if this queue contains
     * at least one element {@code e} such that {@code o.equals(e)}.
     *
     * @param o object to be checked for containment in this queue
     * @return {@code true} if this queue contains the specified element
     * 是否包含元素
     */
    public boolean contains(Object o) {
        if (o == null) return false;
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            for (int i = takeIndex, k = count; k > 0; i = inc(i), k--)
                if (o.equals(items[i]))
                    return true;
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns an array containing all of the elements in this queue, in
     * proper sequence.
     *
     * <p>The returned array will be "safe" in that no references to it are
     * maintained by this queue.  (In other words, this method must allocate
     * a new array).  The caller is thus free to modify the returned array.
     *
     * <p>This method acts as bridge between array-based and collection-based
     * APIs.
     *
     * @return an array containing all of the elements in this queue
     * toArray方法
     */
    public Object[] toArray() {
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final int count = this.count;
            //要返回的数组
            Object[] a = new Object[count];
            //遍历添加到数组中
            for (int i = takeIndex, k = 0; k < count; i = inc(i), k++)
                a[k] = items[i];
            return a;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns an array containing all of the elements in this queue, in
     * proper sequence; the runtime type of the returned array is that of
     * the specified array.  If the queue fits in the specified array, it
     * is returned therein.  Otherwise, a new array is allocated with the
     * runtime type of the specified array and the size of this queue.
     *
     * <p>If this queue fits in the specified array with room to spare
     * (i.e., the array has more elements than this queue), the element in
     * the array immediately following the end of the queue is set to
     * {@code null}.
     *
     * <p>Like the {@link #toArray()} method, this method acts as bridge between
     * array-based and collection-based APIs.  Further, this method allows
     * precise control over the runtime type of the output array, and may,
     * under certain circumstances, be used to save allocation costs.
     *
     * <p>Suppose {@code x} is a queue known to contain only strings.
     * The following code can be used to dump the queue into a newly
     * allocated array of {@code String}:
     *
     * <pre>
     *     String[] y = x.toArray(new String[0]);</pre>
     *
     * Note that {@code toArray(new Object[0])} is identical in function to
     * {@code toArray()}.
     *
     * @param a the array into which the elements of the queue are to
     *          be stored, if it is big enough; otherwise, a new array of the
     *          same runtime type is allocated for this purpose
     * @return an array containing all of the elements in this queue
     * @throws ArrayStoreException if the runtime type of the specified array
     *         is not a supertype of the runtime type of every element in
     *         this queue
     * @throws NullPointerException if the specified array is null
     */
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final int count = this.count;
            final int len = a.length;
            if (len < count)
                a = (T[])java.lang.reflect.Array.newInstance(
                    a.getClass().getComponentType(), count);
            for (int i = takeIndex, k = 0; k < count; i = inc(i), k++)
                a[k] = (T) items[i];
            if (len > count)
                a[count] = null;
            return a;
        } finally {
            lock.unlock();
        }
    }

    public String toString() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int k = count;
            if (k == 0)
                return "[]";

            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (int i = takeIndex; ; i = inc(i)) {
                Object e = items[i];
                sb.append(e == this ? "(this Collection)" : e);
                if (--k == 0)
                    return sb.append(']').toString();
                sb.append(',').append(' ');
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Atomically removes all of the elements from this queue.
     * The queue will be empty after this call returns.
     * 清空，需要把总数和putIndex，takeIndex都归0
     */
    public void clear() {
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            for (int i = takeIndex, k = count; k > 0; i = inc(i), k--)
                items[i] = null;
            count = 0;
            putIndex = 0;
            takeIndex = 0;
            //通知等待队列不满的线程
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @throws IllegalArgumentException      {@inheritDoc}
     * 将队列中的元素输出到集合中
     */
    public int drainTo(Collection<? super E> c) {
        checkNotNull(c);
        if (c == this)
            throw new IllegalArgumentException();
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int i = takeIndex;
            int n = 0;
            int max = count;
            while (n < max) {
                c.add(this.<E>cast(items[i]));
                items[i] = null;
                i = inc(i);
                ++n;
            }
            if (n > 0) {
                count = 0;
                putIndex = 0;
                takeIndex = 0;
                notFull.signalAll();
            }
            return n;
        } finally {
            lock.unlock();
        }
    }

    /**
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @throws IllegalArgumentException      {@inheritDoc}
     * 将队列中的元素输出到集合中去，可以指定输出的最大元素数
     */
    public int drainTo(Collection<? super E> c, int maxElements) {
        checkNotNull(c);
        if (c == this)
            throw new IllegalArgumentException();
        if (maxElements <= 0)
            return 0;
        final Object[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int i = takeIndex;
            int n = 0;
            int max = (maxElements < count) ? maxElements : count;
            while (n < max) {
                c.add(this.<E>cast(items[i]));
                items[i] = null;
                i = inc(i);
                ++n;
            }
            if (n > 0) {
                count -= n;
                takeIndex = i;
                notFull.signalAll();
            }
            return n;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns an iterator over the elements in this queue in proper sequence.
     * The elements will be returned in order from first (head) to last (tail).
     *
     * <p>The returned {@code Iterator} is a "weakly consistent" iterator that
     * will never throw {@link java.util.ConcurrentModificationException
     * ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     *
     * @return an iterator over the elements in this queue in proper sequence
     */
    public Iterator<E> iterator() {
        return new Itr();
    }

    /**
     * Iterator for ArrayBlockingQueue. To maintain weak consistency
     * with respect to puts and takes, we (1) read ahead one slot, so
     * as to not report hasNext true but then not have an element to
     * return -- however we later recheck this slot to use the most
     * current value; (2) ensure that each array slot is traversed at
     * most once (by tracking "remaining" elements); (3) skip over
     * null slots, which can occur if takes race ahead of iterators.
     * However, for circular array-based queues, we cannot rely on any
     * well established definition of what it means to be weakly
     * consistent with respect to interior removes since these may
     * require slot overwrites in the process of sliding elements to
     * cover gaps. So we settle for resiliency, operating on
     * established apparent nexts, which may miss some elements that
     * have moved between calls to next.
     */
    private class Itr implements Iterator<E> {
        private int remaining; // Number of elements yet to be returned
        private int nextIndex; // Index of element to be returned by next
        private E nextItem;    // Element to be returned by next call to next
        private E lastItem;    // Element returned by last call to next
        private int lastRet;   // Index of last element returned, or -1 if none

        Itr() {
            final ReentrantLock lock = ArrayBlockingQueue.this.lock;
            lock.lock();
            try {
                lastRet = -1;
                if ((remaining = count) > 0)
                    nextItem = itemAt(nextIndex = takeIndex);
            } finally {
                lock.unlock();
            }
        }

        public boolean hasNext() {
            return remaining > 0;
        }

        public E next() {
            final ReentrantLock lock = ArrayBlockingQueue.this.lock;
            lock.lock();
            try {
                if (remaining <= 0)
                    throw new NoSuchElementException();
                lastRet = nextIndex;
                E x = itemAt(nextIndex);  // check for fresher value
                if (x == null) {
                    x = nextItem;         // we are forced to report old value
                    lastItem = null;      // but ensure remove fails
                }
                else
                    lastItem = x;
                while (--remaining > 0 && // skip over nulls
                       (nextItem = itemAt(nextIndex = inc(nextIndex))) == null)
                    ;
                return x;
            } finally {
                lock.unlock();
            }
        }

        public void remove() {
            final ReentrantLock lock = ArrayBlockingQueue.this.lock;
            lock.lock();
            try {
                int i = lastRet;
                if (i == -1)
                    throw new IllegalStateException();
                lastRet = -1;
                E x = lastItem;
                lastItem = null;
                // only remove if item still at index
                if (x != null && x == items[i]) {
                    boolean removingHead = (i == takeIndex);
                    removeAt(i);
                    if (!removingHead)
                        nextIndex = dec(nextIndex);
                }
            } finally {
                lock.unlock();
            }
        }
    }

}
