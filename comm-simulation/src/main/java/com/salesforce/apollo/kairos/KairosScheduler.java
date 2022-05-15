/*
* Copyright (c) 2022, salesforce.com, inc.
* All rights reserved.
* SPDX-License-Identifier: BSD-3-Clause
* For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
*/

package com.salesforce.apollo.kairos;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.jmock.lib.concurrent.UnsupportedSynchronousOperationException;

import com.salesforce.apollo.kairos.KairosFuture.CallableRunnableAdapter;

/**
 * A ScheduledExecutorService for evaluating scheduled and runnable actions in
 * Kairos time
 * 
 * @author hal.hildebrand
 */
public final class KairosScheduler implements ScheduledExecutorService {

    public static Duration duration(long amount, TimeUnit unit) {
        switch (unit) {
        case NANOSECONDS:
            return Duration.ofNanos(amount);
        case MICROSECONDS:
            return Duration.ofNanos(unit.convert(amount, TimeUnit.NANOSECONDS));
        case MILLISECONDS:
            return Duration.ofMillis(amount);
        case SECONDS:
            return Duration.ofSeconds(amount);
        case MINUTES:
            return Duration.ofMinutes(amount);
        case HOURS:
            return Duration.ofHours(amount);
        case DAYS:
            return Duration.ofDays(amount);
        default:
            throw new IllegalStateException("Unreachable");
        }
    }

    private final Simulation simulation;

    public KairosScheduler(Simulation simulation) {
        this.simulation = simulation;
    }

    @Override
    public boolean awaitTermination(long _timeout, TimeUnit _unit) {
        throw blockingOperationsNotSupported();
    }

    @Override
    public void execute(Runnable command) {
        schedule(command, 0, TimeUnit.SECONDS);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> _tasks) {
        throw blockingOperationsNotSupported();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> _tasks, long _timeout,
                                         TimeUnit _unit) throws InterruptedException {
        throw blockingOperationsNotSupported();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> _tasks) {
        throw blockingOperationsNotSupported();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> _tasks, long _timeout, TimeUnit _unit) {
        throw blockingOperationsNotSupported();
    }

    @Override
    public boolean isShutdown() {
        throw shutdownNotSupported();
    }

    @Override
    public boolean isTerminated() {
        throw shutdownNotSupported();
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        KairosFuture<V> task = new KairosFuture<V>(callable, simulation);
        return task.setEvent(simulation.schedule(duration(delay, unit), task));
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        KairosFuture<Void> task = new KairosFuture<>(command, simulation);
        return task.setEvent(simulation.schedule(duration(delay, unit), task));
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduleWithFixedDelay(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        KairosFuture<Object> task = new KairosFuture<>(duration(delay, unit), command, simulation);
        return task.setEvent(simulation.schedule(duration(delay, unit), task));
    }

    @Override
    public void shutdown() {
        throw shutdownNotSupported();
    }

    @Override
    public List<Runnable> shutdownNow() {
        throw shutdownNotSupported();
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        return schedule(callable, 0, TimeUnit.SECONDS);
    }

    @Override
    public Future<?> submit(Runnable command) {
        return submit(command, null);
    }

    @Override
    public <T> Future<T> submit(Runnable command, T result) {
        return submit(new CallableRunnableAdapter<T>(command, result));
    }

    UnsupportedSynchronousOperationException blockingOperationsNotSupported() {
        return new UnsupportedSynchronousOperationException("cannot perform blocking wait on a task scheduled on a "
        + KairosScheduler.class.getName());
    }

    private UnsupportedOperationException shutdownNotSupported() {
        return new UnsupportedOperationException("shutdown not supported");
    }
}
