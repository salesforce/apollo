/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author hal.hildebrand
 *
 */
public class ExponentialBackoff<T> {

    public static final class Builder<T> {
        private long                base             = DEFAULT_WAIT_BASE_MILLIS;
        private long                cap              = DEFAULT_WAIT_CAP_MILLIS;
        private Consumer<Throwable> exceptionHandler = t -> {
                                                     };
        private boolean             infinite         = false;
        private boolean             jitter           = false;
        private int                 maxAttempts      = DEFAULT_MAX_ATTEMPTS;
        private Predicate<T>        retryIf          = t -> false;

        private Builder() {
        }

        public ExponentialBackoff<T> build() {
            return new ExponentialBackoff<>(cap, base, maxAttempts, jitter, infinite, retryIf, exceptionHandler);
        }

        public void execute(Callable<T> task, CompletableFuture<T> futureSailor, ScheduledExecutorService scheduler) {
            build().execute(task, futureSailor, scheduler);
        }

        public void executeAsync(Callable<ListenableFuture<T>> task, Executor executor,
                                 CompletableFuture<T> futureSailor, ScheduledExecutorService scheduler) {
            build().executeAsync(task, futureSailor, scheduler);
        }

        public Builder<T> retryIf(final Predicate<T> retryIf) {
            this.retryIf = Objects.requireNonNull(retryIf);
            return this;
        }

        public Builder<T> setBase(final long base) {
            this.base = base;
            return this;
        }

        public Builder<T> setCap(final long cap) {
            this.cap = cap;
            return this;
        }

        public Builder<T> setExceptionHandler(final Consumer<Throwable> exceptionHandler) {
            this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
            return this;
        }

        public Builder<T> setInfiniteAttempts() {
            this.infinite = true;
            return this;
        }

        public Builder<T> setJitter() {
            this.jitter = true;
            return this;
        }

        public Builder<T> setMaxAttempts(final int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }
    }

    public static class RetryFailed extends Exception {
        private static final long serialVersionUID = 1L;

        public RetryFailed() {
        }

        public RetryFailed(String message) {
            super(message);
        }

    }

    static final int  DEFAULT_MAX_ATTEMPTS     = 10;
    static final long DEFAULT_WAIT_BASE_MILLIS = 100;
    static final long DEFAULT_WAIT_CAP_MILLIS  = 60000;

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    static long getWaitTime(final long cap, final long base, final long n) {
        final long expWait = ((long) Math.pow(2, n)) * base;
        return expWait <= 0 ? cap : Math.min(cap, expWait);
    }

    static long getWaitTimeWithJitter(final long cap, final long base, final long n) {
        return ThreadLocalRandom.current().nextLong(0, getWaitTime(cap, base, n));
    }

    private final long                base;
    private final long                cap;
    private final Consumer<Throwable> exceptionHandler;
    private final boolean             infinite;
    private final boolean             jitter;
    private final int                 maxAttempts;
    private final Predicate<T>        retryIf;

    public ExponentialBackoff(final long cap, final long base, final int maxAttempts, final boolean jitter,
                              final boolean infinite, final Predicate<T> retryIf,
                              Consumer<Throwable> exceptionHandler) {
        this.cap = cap;
        this.base = base;
        this.maxAttempts = maxAttempts;
        this.jitter = jitter;
        this.infinite = infinite;
        this.exceptionHandler = exceptionHandler;
        this.retryIf = Objects.requireNonNull(retryIf);
    }

    public void execute(Callable<T> task, CompletableFuture<T> futureSailor, ScheduledExecutorService scheduler) {
        if (infinite) {
            execute(attempt -> true, 0, futureSailor, task, scheduler);
        } else {
            execute(attempt -> attempt < maxAttempts, 0, futureSailor, task, scheduler);
        }
    }

    public void executeAsync(Callable<ListenableFuture<T>> task, CompletableFuture<T> futureSailor,
                             ScheduledExecutorService scheduler) {
        if (infinite) {
            executeAsync(0, task, futureSailor, attempt -> true, scheduler);
        } else {
            executeAsync(0, task, futureSailor, attempt -> attempt < maxAttempts, scheduler);
        }
    }

    private void execute(final Predicate<Long> predicate, final long attempt, final CompletableFuture<T> futureSailor,
                         Callable<T> task, final ScheduledExecutorService scheduler) {
        if (!predicate.test(attempt)) {
            futureSailor.completeExceptionally(new RetryFailed("Exceeded maximum attempts"));
            return;
        }

        try {
            final T result = task.call();
            if (!retryIf.test(result)) {
                futureSailor.complete(result);
                return;
            }
        } catch (final Exception e) {
            exceptionHandler.accept(e);
        }
        final var nextAttempt = attempt + 1;
        final long waitTime = jitter ? getWaitTimeWithJitter(cap, base, nextAttempt)
                                     : getWaitTime(cap, base, nextAttempt);
        scheduler.schedule(() -> execute(predicate, nextAttempt, futureSailor, task, scheduler), waitTime,
                           TimeUnit.MILLISECONDS);
    }

    private void executeAsync(final long attempt, Callable<ListenableFuture<T>> task,
                              final CompletableFuture<T> futureSailor, final Predicate<Long> predicate,
                              final ScheduledExecutorService scheduler) {
        if (!predicate.test(attempt)) {
            futureSailor.completeExceptionally(new RetryFailed("Exceeded maximum attempts"));
            return;
        }
        final var nextAttempt = attempt + 1;
        final long waitTime = jitter ? getWaitTimeWithJitter(cap, base, nextAttempt)
                                     : getWaitTime(cap, base, nextAttempt);

        final ListenableFuture<T> r;

        try {
            r = task.call();
        } catch (Exception e) {
            exceptionHandler.accept(e);
            scheduler.schedule(() -> executeAsync(nextAttempt, task, futureSailor, predicate, scheduler), waitTime,
                               TimeUnit.MILLISECONDS);
            return;
        }

        if (r == null) {
            if (!retryIf.test(null)) {
                futureSailor.complete(null);
                return;
            }
        }

        r.addListener(() -> {
            try {
                T result = r.get();
                if (!retryIf.test(result)) {
                    futureSailor.complete(result);
                    return;
                }
            } catch (final Exception e) {
                exceptionHandler.accept(e);
            }
            scheduler.schedule(() -> executeAsync(nextAttempt, task, futureSailor, predicate, scheduler), waitTime,
                               TimeUnit.MILLISECONDS);
        }, run -> run.run());
    }
}
