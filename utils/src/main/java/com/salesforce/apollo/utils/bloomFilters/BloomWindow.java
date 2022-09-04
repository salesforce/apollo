/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author hal.hildebrand
 *
 */
public class BloomWindow<T> {
    private final AtomicInteger                 count    = new AtomicInteger(0);
    private final Supplier<BloomFilter<T>>      factory;
    private final ReadWriteLock                 rwLock   = new ReentrantReadWriteLock();
    private final BlockingDeque<BloomFilter<T>> segments = new LinkedBlockingDeque<>();
    private final int                           windowSize;

    public BloomWindow(int windowSize, Supplier<BloomFilter<T>> factory, int segments) {
        this.windowSize = windowSize;
        this.factory = factory;
        for (var i = 0; i < segments; i++) {
            this.segments.add(factory.get());
        }
    }

    public void add(T element) {
        if (count.incrementAndGet() % windowSize == 0) {
            segments.removeLast();
            segments.addFirst(factory.get());
        }
        segments.getFirst().add(element);
    }

    public boolean add(T element, Consumer<T> ifAbsent) {
        if (count.incrementAndGet() % windowSize == 0) {
            segments.removeLast();
            segments.addFirst(factory.get());
        }
        AtomicBoolean added = new AtomicBoolean();
        Consumer<T> wrap = t -> {
            if (ifAbsent != null) {
                ifAbsent.accept(t);
            }
            added.set(true);
        };
        final var l = rwLock.writeLock();
        l.lock();
        try {
            segments.getFirst().add(element, wrap);
        } finally {
            l.unlock();
        }
        return added.get();
    }

    public boolean contains(T element) {
        final var l = rwLock.readLock();
        l.lock();
        try {
            for (var biff : segments) {
                if (biff.contains(element)) {
                    return true;
                }
            }
        } finally {
            l.unlock();
        }
        return false;
    }
}
