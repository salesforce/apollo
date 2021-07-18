/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * A container for storing slices of units and accessing them via their creator
 * id
 * 
 * @author hal.hildebrand
 *
 */
public interface SlottedUnits {

    record slottedUnits(List<List<Unit>> contents, List<ReadWriteLock> mxs) implements SlottedUnits {

        @Override
        public List<Unit> get(short pid) {
            if (pid > mxs.size()) {
                return Collections.emptyList();
            }
            Lock lock = mxs.get(pid).readLock();
            lock.lock();
            try {
                return contents.get(pid);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void set(short pid, List<Unit> units) {
            if (pid >= mxs.size()) {
                return;
            }
            Lock lock = mxs.get(pid).writeLock();
            lock.lock();
            try {
                contents.set(pid, units);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void iterate(Function<List<Unit>, Boolean> work) {
            for (short id = 0; id < mxs.size(); id++) {
                if (!work.apply(get(id))) {
                    return;
                }
            }
        }

    }

    static SlottedUnits newSlottedUnits(short n) {
        List<List<Unit>> contents = new ArrayList<>();
        List<ReadWriteLock> locks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            contents.add(new ArrayList<>());
            locks.add(new ReentrantReadWriteLock());
        }
        return new slottedUnits(contents, locks);
    }

    /**
     * Return all units in this container created by the process identified by the
     * pid
     */
    List<Unit> get(short pid);

    void iterate(Function<List<Unit>, Boolean> work);

    /**
     * Replace all units created by the process specified by the pid with the
     * specified units
     */
    void set(short pid, List<Unit> units);
}
