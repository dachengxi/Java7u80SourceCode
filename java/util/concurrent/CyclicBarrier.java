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

/**
 * A synchronization aid that allows a set of threads to all wait for
 * each other to reach a common barrier point.  CyclicBarriers are
 * useful in programs involving a fixed sized party of threads that
 * must occasionally wait for each other. The barrier is called
 * <em>cyclic</em> because it can be re-used after the waiting threads
 * are released.
 * 允许线程相互等待到达一个点，所有线程都释放之后，barrier可以重用
 *
 * <p>A <tt>CyclicBarrier</tt> supports an optional {@link Runnable} command
 * that is run once per barrier point, after the last thread in the party
 * arrives, but before any threads are released.
 * This <em>barrier action</em> is useful
 * for updating shared-state before any of the parties continue.
 * 最后一个线程到达的时候可以触发一个Runnable来完成一些工作
 *
 * <p><b>Sample usage:</b> Here is an example of
 *  using a barrier in a parallel decomposition design:
 * <pre>
 * class Solver {
 *   final int N;
 *   final float[][] data;
 *   final CyclicBarrier barrier;
 *
 *   class Worker implements Runnable {
 *     int myRow;
 *     Worker(int row) { myRow = row; }
 *     public void run() {
 *       while (!done()) {
 *         processRow(myRow);
 *
 *         try {
 *           barrier.await();
 *         } catch (InterruptedException ex) {
 *           return;
 *         } catch (BrokenBarrierException ex) {
 *           return;
 *         }
 *       }
 *     }
 *   }
 *
 *   public Solver(float[][] matrix) {
 *     data = matrix;
 *     N = matrix.length;
 *     barrier = new CyclicBarrier(N,
 *                                 new Runnable() {
 *                                   public void run() {
 *                                     mergeRows(...);
 *                                   }
 *                                 });
 *     for (int i = 0; i < N; ++i)
 *       new Thread(new Worker(i)).start();
 *
 *     waitUntilDone();
 *   }
 * }
 * </pre>
 * Here, each worker thread processes a row of the matrix then waits at the
 * barrier until all rows have been processed. When all rows are processed
 * the supplied {@link Runnable} barrier action is executed and merges the
 * rows. If the merger
 * determines that a solution has been found then <tt>done()</tt> will return
 * <tt>true</tt> and each worker will terminate.
 *
 * <p>If the barrier action does not rely on the parties being suspended when
 * it is executed, then any of the threads in the party could execute that
 * action when it is released. To facilitate this, each invocation of
 * {@link #await} returns the arrival index of that thread at the barrier.
 * You can then choose which thread should execute the barrier action, for
 * example:
 * <pre>  if (barrier.await() == 0) {
 *     // log the completion of this iteration
 *   }</pre>
 *
 * <p>The <tt>CyclicBarrier</tt> uses an all-or-none breakage model
 * for failed synchronization attempts: If a thread leaves a barrier
 * point prematurely because of interruption, failure, or timeout, all
 * other threads waiting at that barrier point will also leave
 * abnormally via {@link BrokenBarrierException} (or
 * {@link InterruptedException} if they too were interrupted at about
 * the same time).
 * CyclicBarrier要么都成功要么都失败，一个线程被中断，失败或者超市，所有的其他线程也都失败
 *
 * <p>Memory consistency effects: Actions in a thread prior to calling
 * {@code await()}
 * <a href="package-summary.html#MemoryVisibility"><i>happen-before</i></a>
 * actions that are part of the barrier action, which in turn
 * <i>happen-before</i> actions following a successful return from the
 * corresponding {@code await()} in other threads.
 *
 * @since 1.5
 * @see CountDownLatch
 *
 * @author Doug Lea
 */
public class CyclicBarrier {
    /**
     * Each use of the barrier is represented as a generation instance.
     * The generation changes whenever the barrier is tripped, or
     * is reset. There can be many generations associated with threads
     * using the barrier - due to the non-deterministic way the lock
     * may be allocated to waiting threads - but only one of these
     * can be active at a time (the one to which <tt>count</tt> applies)
     * and all the rest are either broken or tripped.
     * There need not be an active generation if there has been a break
     * but no subsequent reset.
     * CyclicBarrier是可重用的
     * 每次从开始使用到穿过栅栏当做 一代
     */
    private static class Generation {
        /**
         * broken表示当前屏障是否损坏
         */
        boolean broken = false;
    }

    /** The lock for guarding barrier entry */
    private final ReentrantLock lock = new ReentrantLock();
    /** Condition to wait on until tripped */
    /**
     * 等待条件
     */
    private final Condition trip = lock.newCondition();
    /** The number of parties */
    /**
     * 参与的线程数量
     */
    private final int parties;
    /* The command to run when tripped */
    /**
     * 最后一个线程到达时候要运行的命令
     */
    private final Runnable barrierCommand;
    /** The current generation */
    /**
     * 当前所处的代
     */
    private Generation generation = new Generation();

    /**
     * Number of parties still waiting. Counts down from parties to 0
     * on each generation.  It is reset to parties on each new
     * generation or when broken.
     * 正在等待的线程的数量。每一个Generation都会从parties开始倒数到0。
     * 新的Generation或者是屏障损坏，都会被重置。
     *
     * 还没有到栅栏的线程数
     */
    private int count;

    /**
     * Updates state on barrier trip and wakes up everyone.
     * Called only while holding lock.
     * 开启下一代
     * 当最后一个线程到达栅栏的时候，调用这个方法来唤醒其他线程，同时初始化下一代
     */
    private void nextGeneration() {
        // signal completion of last generation
        // 唤醒在栅栏上等待的线程
        trip.signalAll();
        // set up next generation
        // 重置count
        count = parties;
        // 产生一个新的Generation实例
        generation = new Generation();
    }

    /**
     * Sets current barrier generation as broken and wakes up everyone.
     * Called only while holding lock.
     * 设置当前屏障generation已经损坏，唤醒每个线程
     */
    private void breakBarrier() {
        // 当前线程已经损坏
        generation.broken = true;
        // 重置count
        count = parties;
        // 唤醒所有已经在等待的线程
        trip.signalAll();
    }

    /**
     * Main barrier code, covering the various policies.
     */
    private int dowait(boolean timed, long nanos)
        throws InterruptedException, BrokenBarrierException,
               TimeoutException {
        // 加锁
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            // 当前Generation
            final Generation g = generation;

            // 如果当前Generation已经损坏，抛异常
            if (g.broken)
                throw new BrokenBarrierException();

            // 当前线程被中断
            if (Thread.interrupted()) {
                // 设置当前屏障损坏，并抛异常
                breakBarrier();
                throw new InterruptedException();
            }

            /**
             * count减一后再赋值给index
             * index也是该方法的返回值
             */
           int index = --count;
            // 等于0，说明所有的线程都到栅栏了，需要准备通过
           if (index == 0) {  // tripped
               boolean ranAction = false;
               try {
                   //最后一个线程到达的时候需要执行的命令
                   final Runnable command = barrierCommand;
                   if (command != null)
                       command.run();
                   /**
                    * 如果ranAction为true说明执行command.run的时候没有发生异常
                    */
                   ranAction = true;
                   /**
                    * 唤醒等待线程
                    * 开启新的一代
                    */
                   nextGeneration();
                   return 0;
               } finally {
                   if (!ranAction)
                   /**
                    * 能进入到这里，说明执行command.run操作时发生了异常，需要打破栅栏
                    * 打破栅栏会唤醒所有等待的线程，设置broken为true，重置count为parties
                    */
                       breakBarrier();
               }
           }

            // loop until tripped, broken, interrupted, or timed out
            /**
             * 如果是最后一个线程调用的await方法，那么在上面的操作后就返回了，不会走到这里
             * 走到这里，说明调用await方法的线程不是最后一个线程
             */
            for (;;) {
                try {
                    // 不是带超时时间的
                    if (!timed)
                        // AQS中实现await，可中断条件的等待
                        trip.await();
                    else if (nanos > 0L)
                        // 带超时时间的可中断条件的等待
                        nanos = trip.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    // 走到这里说明等待的线程在Condition.await的时候被中断了
                    if (g == generation && ! g.broken) {
                        // 打破栅栏
                        breakBarrier();
                        throw ie;
                    } else {
                        // We're about to finish waiting even if we had not
                        // been interrupted, so this interrupt is deemed to
                        // "belong" to subsequent execution.
                        /**
                         * 走到这里有两种情况：
                         * 1. g != generation 说明新的一代已经产生，即最后一个线程await执行完成
                         *    此时没有必要抛异常，记录下这个中断信息即可
                         *
                         * 2. g == generation 但是g.broken 栅栏被打破了，不应该抛出InterruptedException，
                         *    而是下面的抛出BrokenBarrierException异常
                         */
                        Thread.currentThread().interrupt();
                    }
                }

                if (g.broken)
                    throw new BrokenBarrierException();

                /**
                 * 最后一个线程在执行完指定任务，会调用nextGeneration开启新的一代，然后释放掉锁，
                 * 其他线程从Condition.await方法中得到锁并返回，到这里的时候就会满足g!=generation。
                 *
                 * 如果command执行过程除了异常，会打破栅栏操作，设置broken为true，然后唤醒这些线程，
                 * 这些线程会从上面一个if分支异常返回。
                 *
                 * 如果await超时，不会从这个if分支返回，而是从再下面的if分支返回
                 */
                if (g != generation)
                    return index;

                /**
                 * 唤醒后，超时，打破栅栏，抛异常
                 */
                if (timed && nanos <= 0L) {
                    breakBarrier();
                    throw new TimeoutException();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Creates a new <tt>CyclicBarrier</tt> that will trip when the
     * given number of parties (threads) are waiting upon it, and which
     * will execute the given barrier action when the barrier is tripped,
     * performed by the last thread entering the barrier.
     *
     * @param parties the number of threads that must invoke {@link #await}
     *        before the barrier is tripped
     * @param barrierAction the command to execute when the barrier is
     *        tripped, or {@code null} if there is no action
     * @throws IllegalArgumentException if {@code parties} is less than 1
     */
    public CyclicBarrier(int parties, Runnable barrierAction) {
        if (parties <= 0) throw new IllegalArgumentException();
        this.parties = parties;
        this.count = parties;
        this.barrierCommand = barrierAction;
    }

    /**
     * Creates a new <tt>CyclicBarrier</tt> that will trip when the
     * given number of parties (threads) are waiting upon it, and
     * does not perform a predefined action when the barrier is tripped.
     *
     * @param parties the number of threads that must invoke {@link #await}
     *        before the barrier is tripped
     * @throws IllegalArgumentException if {@code parties} is less than 1
     */
    public CyclicBarrier(int parties) {
        this(parties, null);
    }

    /**
     * Returns the number of parties required to trip this barrier.
     *
     * @return the number of parties required to trip this barrier
     */
    public int getParties() {
        return parties;
    }

    /**
     * Waits until all {@linkplain #getParties parties} have invoked
     * <tt>await</tt> on this barrier.
     *
     * <p>If the current thread is not the last to arrive then it is
     * disabled for thread scheduling purposes and lies dormant until
     * one of the following things happens:
     * <ul>
     * <li>The last thread arrives; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * one of the other waiting threads; or
     * <li>Some other thread times out while waiting for barrier; or
     * <li>Some other thread invokes {@link #reset} on this barrier.
     * </ul>
     *
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * <p>If the barrier is {@link #reset} while any thread is waiting,
     * or if the barrier {@linkplain #isBroken is broken} when
     * <tt>await</tt> is invoked, or while any thread is waiting, then
     * {@link BrokenBarrierException} is thrown.
     *
     * <p>If any thread is {@linkplain Thread#interrupt interrupted} while waiting,
     * then all other waiting threads will throw
     * {@link BrokenBarrierException} and the barrier is placed in the broken
     * state.
     *
     * <p>If the current thread is the last thread to arrive, and a
     * non-null barrier action was supplied in the constructor, then the
     * current thread runs the action before allowing the other threads to
     * continue.
     * If an exception occurs during the barrier action then that exception
     * will be propagated in the current thread and the barrier is placed in
     * the broken state.
     *
     * @return the arrival index of the current thread, where index
     *         <tt>{@link #getParties()} - 1</tt> indicates the first
     *         to arrive and zero indicates the last to arrive
     * @throws InterruptedException if the current thread was interrupted
     *         while waiting
     * @throws BrokenBarrierException if <em>another</em> thread was
     *         interrupted or timed out while the current thread was
     *         waiting, or the barrier was reset, or the barrier was
     *         broken when {@code await} was called, or the barrier
     *         action (if present) failed due an exception.
     * 等待
     */
    public int await() throws InterruptedException, BrokenBarrierException {
        try {
            return dowait(false, 0L);
        } catch (TimeoutException toe) {
            throw new Error(toe); // cannot happen;
        }
    }

    /**
     * Waits until all {@linkplain #getParties parties} have invoked
     * <tt>await</tt> on this barrier, or the specified waiting time elapses.
     *
     * <p>If the current thread is not the last to arrive then it is
     * disabled for thread scheduling purposes and lies dormant until
     * one of the following things happens:
     * <ul>
     * <li>The last thread arrives; or
     * <li>The specified timeout elapses; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * one of the other waiting threads; or
     * <li>Some other thread times out while waiting for barrier; or
     * <li>Some other thread invokes {@link #reset} on this barrier.
     * </ul>
     *
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * <p>If the specified waiting time elapses then {@link TimeoutException}
     * is thrown. If the time is less than or equal to zero, the
     * method will not wait at all.
     *
     * <p>If the barrier is {@link #reset} while any thread is waiting,
     * or if the barrier {@linkplain #isBroken is broken} when
     * <tt>await</tt> is invoked, or while any thread is waiting, then
     * {@link BrokenBarrierException} is thrown.
     *
     * <p>If any thread is {@linkplain Thread#interrupt interrupted} while
     * waiting, then all other waiting threads will throw {@link
     * BrokenBarrierException} and the barrier is placed in the broken
     * state.
     *
     * <p>If the current thread is the last thread to arrive, and a
     * non-null barrier action was supplied in the constructor, then the
     * current thread runs the action before allowing the other threads to
     * continue.
     * If an exception occurs during the barrier action then that exception
     * will be propagated in the current thread and the barrier is placed in
     * the broken state.
     *
     * @param timeout the time to wait for the barrier
     * @param unit the time unit of the timeout parameter
     * @return the arrival index of the current thread, where index
     *         <tt>{@link #getParties()} - 1</tt> indicates the first
     *         to arrive and zero indicates the last to arrive
     * @throws InterruptedException if the current thread was interrupted
     *         while waiting
     * @throws TimeoutException if the specified timeout elapses
     * @throws BrokenBarrierException if <em>another</em> thread was
     *         interrupted or timed out while the current thread was
     *         waiting, or the barrier was reset, or the barrier was broken
     *         when {@code await} was called, or the barrier action (if
     *         present) failed due an exception
     * 带超时的等待
     */
    public int await(long timeout, TimeUnit unit)
        throws InterruptedException,
               BrokenBarrierException,
               TimeoutException {
        return dowait(true, unit.toNanos(timeout));
    }

    /**
     * Queries if this barrier is in a broken state.
     *
     * @return {@code true} if one or more parties broke out of this
     *         barrier due to interruption or timeout since
     *         construction or the last reset, or a barrier action
     *         failed due to an exception; {@code false} otherwise.
     * 当前屏障是否是损坏状态
     */
    public boolean isBroken() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return generation.broken;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resets the barrier to its initial state.  If any parties are
     * currently waiting at the barrier, they will return with a
     * {@link BrokenBarrierException}. Note that resets <em>after</em>
     * a breakage has occurred for other reasons can be complicated to
     * carry out; threads need to re-synchronize in some other way,
     * and choose one to perform the reset.  It may be preferable to
     * instead create a new barrier for subsequent use.
     * 重置屏障为初始化状态
     */
    public void reset() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            breakBarrier();   // break the current generation
            nextGeneration(); // start a new generation
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of parties currently waiting at the barrier.
     * This method is primarily useful for debugging and assertions.
     *
     * @return the number of parties currently blocked in {@link #await}
     */
    public int getNumberWaiting() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return parties - count;
        } finally {
            lock.unlock();
        }
    }
}
