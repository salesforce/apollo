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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hal.hildebrand
 *
 */
public class TickScheduler {
    public class Timer implements Comparable<Timer> {
        public final Runnable action;
        public final int      deadline;

        public Timer(int deadline, Runnable action) {
            this.deadline = deadline;
            this.action = action;
        }

        public void cancel() {
            scheduled.remove(this);
        }

        @Override
        public int compareTo(Timer o) {
            return Integer.compare(deadline, o.deadline);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TickScheduler.class);

    private final AtomicInteger        clock     = new AtomicInteger();
    private final PriorityQueue<Timer> scheduled = new PriorityQueue<>();

    public void cancelAll() {
        scheduled.clear();
    }

    public Timer schedule(Runnable action, int delta) {
        int current = clock.get();
        Timer timer = new Timer(current + delta, action);
        scheduled.add(timer);
        return timer;
    }

    public void tick() {
        List<Timer> drained = new ArrayList<>();
        int current = clock.incrementAndGet();
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
                e.action.run();
            } catch (Throwable ex) {
                log.error("Exception in timer action", ex);
            }
        });
    }
}
