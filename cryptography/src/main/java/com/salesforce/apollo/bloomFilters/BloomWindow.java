/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.bloomFilters;

import com.salesfoce.apollo.cryptography.proto.Biff;
import com.salesforce.apollo.utils.Entropy;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author hal.hildebrand
 */
public class BloomWindow<T> {

    private final    ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final    int           capacity;
    private volatile Active<T>     active1;
    private volatile Active<T>     active2;

    private BloomWindow(BloomFilter<T> active1, int capacity, BloomFilter<T> active2) {
        this.active1 = new Active<>(active1, new AtomicInteger());
        this.active2 = new Active<>(active2, new AtomicInteger());
        this.capacity = capacity;
    }

    public static <Q> BloomWindow<Q> create(int n, double p, Biff.Type type) {
        return create(Entropy.nextBitsStreamLong(), Entropy.nextBitsStreamLong(), n, p, type);
    }

    public static <Q> BloomWindow<Q> create(long seed1, long seed2, int n, double p, Biff.Type type) {
        return new BloomWindow<>(BloomFilter.create(seed1, n, p, type), n, BloomFilter.create(seed2, n, p, type));
    }

    /**
     * @param element
     * @return true if the element is new and has been added to the window, false if element was already present
     */
    public boolean add(T element) {
        var lock = rwLock.writeLock();
        lock.lock();
        try {
            if (active1.contains(element)) {
                return false;
            }
            active1.add(element);
            if (active1.count.get() == capacity) {
                active2.clear();
                // Switch buffers
                var t = active1;
                active1 = active2;
                active2 = t;

                active1.add(element);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean contains(T element) {
        var lock = rwLock.readLock();
        lock.lock();
        try {
            if (active1.contains(element)) {
                return true;
            }
            return active2.contains(element);
        } finally {
            lock.unlock();
        }
    }

    private record Active<T>(BloomFilter<T> bff, AtomicInteger count) {
        public boolean add(T element) {
            return bff.add(element);
        }

        public boolean contains(T element) {
            return bff.contains(element);
        }

        void clear() {
            bff.clear();
            count.set(0);
        }
    }
}
