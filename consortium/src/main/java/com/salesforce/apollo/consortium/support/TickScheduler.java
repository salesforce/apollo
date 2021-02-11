/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.consortium.Consortium.Timers;

/**
 * @author hal.hildebrand
 *
 */
public class TickScheduler {
    public class Timer implements Comparable<Timer> {
        private final Runnable   action;
        private volatile boolean cancelled = false;
        private final Instant    deadline;
        private volatile int     heapIndex;
        private final Timers     label;

        public Timer(Timers label, Duration duration, Runnable action) {
            this.label = label;
            this.deadline = clock.instant().plus(duration);
            this.action = action;
        }

        public boolean cancel() {
            cancelled = true;
            return scheduled.remove(this);
        }

        @Override
        public int compareTo(Timer o) {
            if (o == null) {
                return -1;
            }
            return deadline.compareTo(o.deadline);
        }

        public void fire() {
            final boolean isCancelled = cancelled;
            cancelled = true;
            if (isCancelled) {
                return;
            }
            try {
                action.run();
            } catch (Throwable t) {
                log.error("Error executing action {}", label, t);
            }
        }

        public Instant getDeadline() {
            return deadline;
        }

        public long getDelay() {
            return clock.instant().until(deadline, ChronoUnit.MILLIS);
        }

        public Timers getLabel() {
            return label;
        }
    }

    /**
     * Specialized delay queue.
     */
    static class DelayedWorkQueue extends AbstractQueue<Timer> implements BlockingQueue<Timer> {

        /*
         * A DelayedWorkQueue is based on a heap-based data structure like those in
         * DelayQueue and PriorityQueue, except that every Timer also records its index
         * into the heap array. This eliminates the need to find a task upon
         * cancellation, greatly speeding up removal (down from O(n) to O(log n)), and
         * reducing garbage retention that would otherwise occur by waiting for the
         * element to rise to top before clearing. But because the queue may also hold
         * TimerScheduledFutures that are not Timers, we are not guaranteed to have such
         * indices available, in which case we fall back to linear search. (We expect
         * that most tasks will not be decorated, and that the faster cases will be much
         * more common.)
         *
         * All heap operations must record index changes -- mainly within siftUp and
         * siftDown. Upon removal, a task's heapIndex is set to -1. Note that Timers can
         * appear at most once in the queue (this need not be true for other kinds of
         * tasks or work queues), so are uniquely identified by heapIndex.
         */

        /**
         * Snapshot iterator that works off copy of underlying q array.
         */
        private class Itr implements Iterator<Timer> {
            final Timer[] array;
            int           cursor;       // index of next element to return; initially 0
            int           lastRet = -1; // index of last element returned; -1 if no such

            Itr(Timer[] array) {
                this.array = array;
            }

            @Override
            public boolean hasNext() {
                return cursor < array.length;
            }

            @Override
            public Timer next() {
                if (cursor >= array.length)
                    throw new NoSuchElementException();
                return array[lastRet = cursor++];
            }

            @Override
            public void remove() {
                if (lastRet < 0)
                    throw new IllegalStateException();
                DelayedWorkQueue.this.remove(array[lastRet]);
                lastRet = -1;
            }
        }

        private static final int INITIAL_CAPACITY = 16;

        /**
         * Sets f's heapIndex if it is a Timer.
         */
        private static void setIndex(Timer f, int idx) {
            f.heapIndex = idx;
        }

        /**
         * Condition signalled when a newer task becomes available at the head of the
         * queue or a new thread may need to become leader.
         */
        private final Condition available;

        /**
         * Thread designated to wait for the task at the head of the queue. This variant
         * of the Leader-Follower pattern (http://www.cs.wustl.edu/~schmidt/POSA/POSA2/)
         * serves to minimize unnecessary timed waiting. When a thread becomes the
         * leader, it waits only for the next delay to elapse, but other threads await
         * indefinitely. The leader thread must signal some other thread before
         * returning from take() or poll(...), unless some other thread becomes leader
         * in the interim. Whenever the head of the queue is replaced with a task with
         * an earlier expiration time, the leader field is invalidated by being reset to
         * null, and some waiting thread, but not necessarily the current leader, is
         * signalled. So waiting threads must be prepared to acquire and lose leadership
         * while waiting.
         */
        private Thread              leader;
        private final ReentrantLock lock  = new ReentrantLock();
        private Timer[]             queue = new Timer[INITIAL_CAPACITY];
        private int                 size;

        DelayedWorkQueue() {
            available = lock.newCondition();
        }

        @Override
        public boolean add(Timer e) {
            return offer(e);
        }

        @Override
        public void clear() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                List<Timer> cancelled = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    Timer t = queue[i];
                    if (t != null) {
                        cancelled.add(t);
                        queue[i] = null;
                        setIndex(t, -1);
                    }
                }
                size = 0;
                cancelled.forEach(t -> t.cancel());
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean contains(Object x) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return indexOf(x) != -1;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public int drainTo(Collection<? super Timer> c) {
            return drainTo(c, Integer.MAX_VALUE);
        }

        @Override
        public int drainTo(Collection<? super Timer> c, int maxElements) {
            Objects.requireNonNull(c);
            if (c == this)
                throw new IllegalArgumentException();
            if (maxElements <= 0)
                return 0;
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                int n = 0;
                for (Timer first; n < maxElements && (first = queue[0]) != null && first.getDelay() <= 0;) {
                    c.add(first); // In this order, in case add() throws.
                    finishPoll(first);
                    ++n;
                }
                return n;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public Iterator<Timer> iterator() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return new Itr(Arrays.copyOf(queue, size));
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean offer(Timer x) {
            if (x == null)
                throw new NullPointerException();
            Timer e = x;
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                int i = size;
                if (i >= queue.length)
                    grow();
                size = i + 1;
                if (i == 0) {
                    queue[0] = e;
                    setIndex(e, 0);
                } else {
                    siftUp(i, e);
                }
                if (queue[0] == e) {
                    leader = null;
                    available.signal();
                }
            } finally {
                lock.unlock();
            }
            return true;
        }

        @Override
        public boolean offer(Timer e, long timeout, TimeUnit unit) {
            return offer(e);
        }

        @Override
        public Timer peek() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return queue[0];
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Timer poll() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                Timer first = queue[0];
                return (first == null || first.getDelay() > 0) ? null : finishPoll(first);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Timer poll(long timeout, TimeUnit unit) throws InterruptedException {
            long nanos = unit.toNanos(timeout);
            final ReentrantLock lock = this.lock;
            lock.lockInterruptibly();
            try {
                for (;;) {
                    Timer first = queue[0];
                    if (first == null) {
                        if (nanos <= 0L)
                            return null;
                        else
                            nanos = available.awaitNanos(nanos);
                    } else {
                        long delay = first.getDelay();
                        if (delay <= 0L)
                            return finishPoll(first);
                        if (nanos <= 0L)
                            return null;
                        first = null; // don't retain ref while waiting
                        if (nanos < delay || leader != null)
                            nanos = available.awaitNanos(nanos);
                        else {
                            Thread thisThread = Thread.currentThread();
                            leader = thisThread;
                            try {
                                long timeLeft = available.awaitNanos(delay);
                                nanos -= delay - timeLeft;
                            } finally {
                                if (leader == thisThread)
                                    leader = null;
                            }
                        }
                    }
                }
            } finally {
                if (leader == null && queue[0] != null)
                    available.signal();
                lock.unlock();
            }
        }

        @Override
        public void put(Timer e) {
            offer(e);
        }

        @Override
        public int remainingCapacity() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean remove(Object x) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                int i = indexOf(x);
                if (i < 0)
                    return false;

                setIndex(queue[i], -1);
                int s = --size;
                Timer replacement = queue[s];
                queue[s] = null;
                if (s != i) {
                    siftDown(i, replacement);
                    if (queue[i] == replacement)
                        siftUp(i, replacement);
                }
                return true;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public int size() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return size;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public Timer take() throws InterruptedException {
            final ReentrantLock lock = this.lock;
            lock.lockInterruptibly();
            try {
                for (;;) {
                    Timer first = queue[0];
                    if (first == null)
                        available.await();
                    else {
                        long delay = first.getDelay();
                        if (delay <= 0L)
                            return finishPoll(first);
                        first = null; // don't retain ref while waiting
                        if (leader != null)
                            available.await();
                        else {
                            Thread thisThread = Thread.currentThread();
                            leader = thisThread;
                            try {
                                available.awaitNanos(delay);
                            } finally {
                                if (leader == thisThread)
                                    leader = null;
                            }
                        }
                    }
                }
            } finally {
                if (leader == null && queue[0] != null)
                    available.signal();
                lock.unlock();
            }
        }

        @Override
        public Object[] toArray() {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                return Arrays.copyOf(queue, size, Object[].class);
            } finally {
                lock.unlock();
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T[] toArray(T[] a) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                if (a.length < size)
                    return (T[]) Arrays.copyOf(queue, size, a.getClass());
                System.arraycopy(queue, 0, a, 0, size);
                if (a.length > size)
                    a[size] = null;
                return a;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Performs common bookkeeping for poll and take: Replaces first element with
         * last and sifts it down. Call only when holding lock.
         *
         * @param f the task to remove and return
         */
        private Timer finishPoll(Timer f) {
            int s = --size;
            Timer x = queue[s];
            queue[s] = null;
            if (s != 0)
                siftDown(0, x);
            setIndex(f, -1);
            return f;
        }

        /**
         * Resizes the heap array. Call only when holding lock.
         */
        private void grow() {
            int oldCapacity = queue.length;
            int newCapacity = oldCapacity + (oldCapacity >> 1); // grow 50%
            if (newCapacity < 0) // overflow
                newCapacity = Integer.MAX_VALUE;
            queue = Arrays.copyOf(queue, newCapacity);
        }

        /**
         * Finds index of given object, or -1 if absent.
         */
        private int indexOf(Object x) {
            if (x != null) {
                if (x instanceof Timer) {
                    int i = ((Timer) x).heapIndex;
                    // Sanity check; x could conceivably be a
                    // Timer from some other pool.
                    if (i >= 0 && i < size && queue[i] == x)
                        return i;
                } else {
                    for (int i = 0; i < size; i++)
                        if (x.equals(queue[i]))
                            return i;
                }
            }
            return -1;
        }

        /**
         * Sifts element added at top down to its heap-ordered spot. Call only when
         * holding lock.
         */
        private void siftDown(int k, Timer key) {
            int half = size >>> 1;
            while (k < half) {
                int child = (k << 1) + 1;
                Timer c = queue[child];
                int right = child + 1;
                if (right < size && c.compareTo(queue[right]) > 0)
                    c = queue[child = right];
                if (key.compareTo(c) <= 0)
                    break;
                queue[k] = c;
                setIndex(c, k);
                k = child;
            }
            queue[k] = key;
            setIndex(key, k);
        }

        /**
         * Sifts element added at bottom up to its heap-ordered spot. Call only when
         * holding lock.
         */
        private void siftUp(int k, Timer key) {
            while (k > 0) {
                int parent = (k - 1) >>> 1;
                Timer e = queue[parent];
                if (key.compareTo(e) >= 0)
                    break;
                queue[k] = e;
                setIndex(e, k);
                k = parent;
            }
            queue[k] = key;
            setIndex(key, k);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TickScheduler.class);

    private final Clock            clock;
    private final DelayedWorkQueue scheduled = new DelayedWorkQueue();

    public TickScheduler() {
        this(Clock.systemUTC());
    }

    public TickScheduler(Clock clock) {
        this.clock = clock;
    }

    public void cancelAll() {
        scheduled.clear();
    }

    public Timer schedule(Timers t, Runnable action, Duration duration) {
        Timer timer = new Timer(t, duration, action);
        scheduled.add(timer);
        return timer;
    }

    public void tick() {
        List<Timer> drained = new ArrayList<>();
        Timer head = scheduled.poll();
        while (head != null) {
            drained.add(head);
            head = scheduled.poll();
        }
        drained.forEach(e -> {
            try {
                e.fire();
            } catch (Throwable ex) {
                log.error("Exception in timer action", ex);
            }
        });
    }
}
