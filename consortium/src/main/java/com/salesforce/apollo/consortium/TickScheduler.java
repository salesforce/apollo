/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.consortium.Consortium.Timers;

/**
 * @author hal.hildebrand
 *
 */
public class TickScheduler {
    public class Timer implements Comparable<Timer> {
        private final Runnable action;
        private final int      deadline;
        private final Timers   label;

        public Timer(Timers label, int deadline, Runnable action) {
            this.label = label;
            this.deadline = deadline;
            this.action = action;
        }

        public boolean cancel() {
            return scheduled.remove(this);
        }

        @Override
        public int compareTo(Timer o) {
            return Integer.compare(deadline, o.deadline);
        }

        public void fire() {
            try {
                action.run();
            } catch (Throwable t) {
                log.error("Error executing action {}", t);
            }
        }

        public int getDeadline() {
            return deadline;
        }

        public Timers getLabel() {
            return label;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TickScheduler.class);

    private final ReadWriteLock        rwLock    = new ReentrantReadWriteLock();
    private final PriorityQueue<Timer> scheduled = new PriorityQueue<>();

    public void cancelAll() {
        locked(() -> {
            scheduled.clear();
            return null;
        });
    }

    public Timer schedule(Timers t, Runnable action, int target) {
        return locked(() -> {
            Timer timer = new Timer(t, target, action);
            scheduled.add(timer);
            return timer;
        });
    }

    public void tick(int current) {
        locked(() -> {
            List<Timer> drained = new ArrayList<>();
            Timer head = scheduled.peek();
            while (head != null) {
                if (current >= head.deadline) {
                    drained.add(scheduled.remove());
                    head = scheduled.peek();
                } else {
                    head = null;
                }
            }
            drained.forEach(e -> {
                try {
                    e.fire();
                } catch (Throwable ex) {
                    log.error("Exception in timer action", ex);
                }
            });
            return null;
        });
    }

    private <T> T locked(Callable<T> call) {
        final Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            return call.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            lock.unlock();
        }
    }
}
