/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedules cancellable actions based on rounds that are modulo some tick count
 * 
 * @author hal.hildebrand
 *
 */
public class RoundScheduler extends AtomicInteger {
    public class Timer implements Comparable<Timer> {
        private final Runnable   action;
        private volatile boolean cancelled = false;
        private final int        deadline;
        private final String     label;

        public Timer(String label, int target, Runnable action) {
            this.label = label;
            this.deadline = target;
            this.action = action;
        }

        public boolean cancel() {
            cancelled = true;
            boolean remove = scheduled.remove(this);
            if (label != null) {
                timers.remove(label);
            }
            log.info("Cancelling: {} target: {} on: {}", label, deadline, RoundScheduler.this.label);
            return remove;
        }

        @Override
        public int compareTo(Timer o) {
            if (o == null) {
                return -1;
            }
            return Integer.compare(deadline, o.deadline);
        }

        public void fire() {
            final boolean isCancelled = cancelled;
            cancelled = true;
            if (isCancelled) {
                return;
            }
            timers.remove(label);
            try {
                action.run();
            } catch (Throwable t) {
                log.error("Error executing action {}", label, t);
            }
        }

        public int getDeadline() {
            return deadline;
        }

        public String getLabel() {
            return label;
        }
    }

    private static final Logger log              = LoggerFactory.getLogger(RoundScheduler.class);
    private static final long   serialVersionUID = 1L;

    private final int                          roundDuration;
    private final PriorityBlockingQueue<Timer> scheduled = new PriorityBlockingQueue<>();
    private final AtomicInteger                tick      = new AtomicInteger();
    private final Map<String, Timer>           timers    = new HashMap<>();
    private final String                       label;

    public RoundScheduler(String label, int roundDuration) {
        this.roundDuration = roundDuration;
        this.label = label;
    }

    public void cancel(String label) {
        var t = timers.remove(label);
        if (t != null) {
            t.cancel();
        }
    }

    public void cancelAll() {
        new ArrayList<>(timers.values()).forEach(e -> e.cancel());
    }

    public Timer schedule(Runnable action, int delayRounds) {
        return schedule(null, action, delayRounds);
    }

    public Timer schedule(String timerLabel, Runnable action, int delayRounds) {
        final var current = get();
        final var target = current + delayRounds;
        Timer timer = new Timer(timerLabel, target, action);
        if (delayRounds == 0) {
            return timer;
        }
        if (timerLabel != null) {
            Timer prev = timers.put(timerLabel, timer);
            if (prev != null) {
                prev.cancel();
            }
        }
        scheduled.add(timer);
        log.info("Scheduling: {} target: {} current: {} on: {}", timerLabel, target, current, label);
        return timer;
    }

    public void tick(int r) {
        var t = tick.incrementAndGet();
        if (t % roundDuration != 0) {
            return;
        }
        int current = incrementAndGet();
//        log.info("Round: {} on: {}", current, label);
        List<Timer> drained = new ArrayList<>();
        while (!scheduled.isEmpty() && scheduled.peek() != null && scheduled.peek().deadline <= current) {
            drained.add(scheduled.poll());
        }
        drained.forEach(e -> {
            log.info("Firing: {} target: {} current: {} on: {}", e.label, e.deadline, current, label);
            try {
                e.fire();
            } catch (Throwable ex) {
                log.error("Exception in timer: {} action on: {}", e.label, label, ex);
            }
        });
    }
}
