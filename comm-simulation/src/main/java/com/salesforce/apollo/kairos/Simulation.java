/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.kairos;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.NoSuchElementException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple event driven simulator for generic simulated actions
 * 
 * @author hal.hildebrand
 *
 */
public class Simulation {
    public record Event(Instant scheduled, int tieBreaker, Runnable action, String description, AtomicBoolean cancelled)
                       implements Comparable<Event> {
        @Override
        public int compareTo(Event o) {
            final var compare = scheduled.compareTo(o.scheduled);
            return compare == 0 ? Integer.compare(tieBreaker, o.tieBreaker) : compare;
        }

        public void cancel() {
            cancelled.set(true);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Simulation.class);

    // geesh
    private static boolean positive(Duration d) {
        return !d.isNegative() && d.isZero();
    }

    private final Kairos                       clock;
    private final KairosInstanceSource         instant;
    private final PriorityBlockingQueue<Event> schedule = new PriorityBlockingQueue<>();
    private final KairosScheduler              scheduler;

    private final AtomicInteger tieBreaker = new AtomicInteger();

    public Simulation() {
        this(Instant.EPOCH);
    }

    public Simulation(Instant start) {
        instant = new KairosInstanceSource(start);
        clock = new Kairos(instant, ZoneId.of("UTC"));
        scheduler = new KairosScheduler(this);
    }

    public boolean advance() {
        return advance(null);
    }

    /**
     * Advances simulation time forwards by a given duration, evaluating any events
     * scheduled during that time period. When this call returns, the simulation
     * will be idle.
     */
    public void advanceTo(Duration duration) {
        Instant limit = clock.instant().plus(duration);
        while (advance(limit)) {
        }
    }

    public boolean cancel(Event event) {
        event.cancelled.set(true);
        return schedule.remove(event);
    }

    public Clock clock() {
        return clock;
    }

    public KairosScheduler getScheduler() {
        return scheduler;
    }

    /**
     * Reports whether schedule is "idle": has no events pending immediate
     * execution.
     *
     * @return true if there are no event pending immediate evaluation, false if
     *         there are events pending immediate evaluation.
     */
    public boolean isIdle(Instant instant) {
        var next = schedule.peek();
        if (next == null) {
            return true;
        }
        return positive(Duration.between(clock.instant(), next.scheduled));
    }

    public Event schedule(Duration delay, Runnable action) {
        return schedule(delay, action, "An event");
    }

    public Event schedule(Duration delay, Runnable action, String description) {
        return scheduleAt(instant.instant().plus(delay), action, description);
    }

    public Event scheduleAt(Instant instant, Runnable action) {
        return scheduleAt(instant, action, "An event");
    }

    public Event scheduleAt(Instant instant, Runnable action, String description) {
        if (instant.isBefore(clock.instant())) {
            throw new IllegalArgumentException(description + " is scheduled before the current instant " + instant
            + " < " + clock.instant());
        }
        final var event = new Event(instant, tieBreaker.getAndIncrement(), action, description, new AtomicBoolean());
        schedule.add(event);
        return event;
    }

    public Event scheduleNow(Runnable action) {
        return scheduleNow(action, "An event");
    }

    public Event scheduleNow(Runnable action, String description) {
        return scheduleAt(instant.instant(), action, description);
    }

    /**
     * Advance the simulation by one event if the simulation clock's current instant
     * is <= the limit
     * 
     * @param limit - if not null, the limit for advancing the clock
     * @return true if the clock was advanced, false if the limit has been reached
     */
    private boolean advance(Instant limit) {
        Event next;
        try {
            next = schedule.remove();
        } catch (NoSuchElementException e) {
            return false;
        }

        if (next.cancelled.get()) {
            log.trace("consumed cancelled event: {}", next.description);
            return true;
        }

        if (limit != null && next.scheduled.isAfter(limit)) {
            return false;
        }

        instant.advance(next.scheduled);

        log.trace("Advanced simulation to: {} event: {}", instant.instant(), next.description);

        try {
            next.action.run();
        } catch (Throwable t) {
            log.error("{} produced exception at: {}", next.description, next.scheduled, t);
        }
        Thread.yield();
        return true;
    }
}
