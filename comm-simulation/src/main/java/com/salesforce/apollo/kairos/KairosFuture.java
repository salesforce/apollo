/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.kairos;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.salesforce.apollo.kairos.Simulation.Event;

/**
 * A ScheduledFuture evaluated in Kairos time
 * 
 * @author hal.hildebrand
 *
 * @param <T>
 */
public class KairosFuture<T> implements ScheduledFuture<T>, Runnable {
    static final class CallableRunnableAdapter<T> implements Callable<T> {
        private final T        result;
        private final Runnable runnable;

        CallableRunnableAdapter(Runnable runnable, T result) {
            this.runnable = runnable;
            this.result = result;
        }

        @Override
        public T call() {
            runnable.run();
            return result;
        }

        @Override
        public String toString() {
            return runnable.toString();
        }
    }

    private volatile Thread            captured;
    private final Callable<T>          command;
    private volatile Event             event;
    private final CompletableFuture<T> futureSailor = new CompletableFuture<>();
    private final Duration             repeatDelay;
    private final Simulation           simulation;

    public KairosFuture(Callable<T> command, Simulation simulation) {
        this.repeatDelay = null;
        this.simulation = simulation;
        this.command = command;
    }

    public KairosFuture(Duration repeatDelay, Runnable command, Simulation simulation) {
        this(new CallableRunnableAdapter<T>(command, null), simulation);
    }

    public KairosFuture(Runnable command, Simulation simulation) {
        this(null, command, simulation);
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean cancel(boolean interrupt) {
        var removed = simulation.cancel(event);
        var cancelled = futureSailor.cancel(interrupt);
        final var suspended = captured;
        if (suspended != null) {
            captured = null;
            suspended.stop();
        }
        return removed || cancelled;
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
    }

    @SuppressWarnings("removal")
    @Override
    public T get() throws ExecutionException {
        try {
            if (futureSailor.isDone()) {
                return futureSailor.get();
            }
            captured = Thread.currentThread();
            captured.suspend();
            return futureSailor.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw e;
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(Duration.between(simulation.clock().instant(), event.scheduled()));
    }

    @Override
    public boolean isCancelled() {
        return futureSailor.isCancelled();
    }

    @Override
    public boolean isDone() {
        return futureSailor.isDone();
    }

    @SuppressWarnings("removal")
    @Override
    public void run() {
        if (futureSailor.isDone()) {
            return;
        }
        final var suspended = captured;
        try {
            if (suspended != null) {
                captured = null;
                suspended.resume();
            }
            futureSailor.complete(command.call());
        } catch (Throwable e) {
            if (suspended != null) {
                captured = null;
                suspended.resume();
            }
            futureSailor.completeExceptionally(e);
        }
    }

    @Override
    public String toString() {
        return command.toString() + " repeatDelay=" + repeatDelay;
    }

    KairosFuture<T> setEvent(Event event) {
        this.event = event;
        return this;
    }
}
