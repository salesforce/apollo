/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.salesforce.apollo.utils.ExponentialBackoff.RetryFailed;

/**
 * @author hal.hildebrand
 *
 */
public class ExponentialBackoffTest {

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Test
    public void asyncSuccessfulAfterThreeAttempts() throws Exception {
        final AtomicInteger attempts = new AtomicInteger(0);
        final AtomicInteger exceptions = new AtomicInteger(0);
        CompletableFuture<String> futureSailor = new CompletableFuture<>();

        ExponentialBackoff.<String>newBuilder().setBase(1).setCap(5000).setMaxAttempts(5)
                          .setExceptionHandler(e -> exceptions.incrementAndGet()).executeAsync(() -> {
                              attempts.incrementAndGet();
                              final boolean shouldThrow = attempts.get() < 3;
                              SettableFuture<String> f = SettableFuture.create();
                              if (shouldThrow) {
                                  System.out.println("Throwing exception");
                                  f.setException(new RuntimeException("Fake exception"));
                              }
                              f.set("Fake result");
                              return f;
                          }, futureSailor, executor);

        String result = futureSailor.get(1, TimeUnit.SECONDS);

        assertThat(attempts.get(), is(equalTo(3)));
        assertThat(exceptions.get(), is(equalTo(2)));
        assertThat(result, is(equalTo("Fake result")));
    }

    @Test
    public void asyncTestInfiniteAttempts() throws Exception {
        final AtomicInteger attempts = new AtomicInteger(0);
        final AtomicInteger exceptions = new AtomicInteger(0);
        final Callable<ListenableFuture<String>> task = () -> {
            attempts.incrementAndGet();
            // Just to ensure that we exceed the default max
            // attempts
            final boolean shouldThrow = attempts.get() < ExponentialBackoff.DEFAULT_MAX_ATTEMPTS + 1;
            SettableFuture<String> f = SettableFuture.create();
            if (shouldThrow) {
                System.out.println("Throwing exception");
                f.setException(new RuntimeException("Fake exception"));
            } else {
                f.set("Fake result");
            }
            return f;
        };
        CompletableFuture<String> futureSailor = new CompletableFuture<>();

        ExponentialBackoff.<String>newBuilder().setBase(1).setCap(10).setInfiniteAttempts()
                          .setExceptionHandler(e -> exceptions.incrementAndGet())
                          .executeAsync(task, futureSailor, executor);

        String result = futureSailor.get(1, TimeUnit.SECONDS);

        assertThat(attempts.get(), is(equalTo(ExponentialBackoff.DEFAULT_MAX_ATTEMPTS + 1)));
        assertThat(exceptions.get(), is(equalTo(ExponentialBackoff.DEFAULT_MAX_ATTEMPTS)));
        assertThat(result, is(equalTo("Fake result")));
    }

    @Test
    public void asyncTestNullFuture() throws Exception {
        CompletableFuture<String> futureSailor = new CompletableFuture<>();
        ExponentialBackoff.<String>newBuilder().setBase(100).setCap(5000).setMaxAttempts(5).setJitter()
                          .setExceptionHandler(Throwable::printStackTrace).executeAsync(() -> {
                              return null;
                          }, futureSailor, executor);

        String result = futureSailor.get(1, TimeUnit.SECONDS);

        assertThat(result, is(equalTo(null)));
    }

    @Test
    public void asyncTestSuccessfulExecution() throws Exception {
        CompletableFuture<String> futureSailor = new CompletableFuture<>();
        ExponentialBackoff.<String>newBuilder().setBase(100).setCap(5000).setMaxAttempts(5).setJitter()
                          .setExceptionHandler(Throwable::printStackTrace).executeAsync(() -> {
                              SettableFuture<String> f = SettableFuture.create();
                              f.set("Do something");
                              return f;
                          }, futureSailor, executor);

        String result = futureSailor.get(1, TimeUnit.SECONDS);

        assertThat(result, is(equalTo("Do something")));
    }

    @Test
    public void asyncTestThatMaxAttemptsAreExceeded() throws Throwable {
        CompletableFuture<Long> futureSailor = new CompletableFuture<>();
        ExponentialBackoff.<Long>newBuilder().setMaxAttempts(3).setBase(1)
                          .setExceptionHandler(e -> System.out.println(e.getMessage())).executeAsync(() -> {
                              SettableFuture<Long> f = SettableFuture.create();
                              f.setException(new RuntimeException("Fake exception"));
                              return f;
                          }, futureSailor, executor);

        try {
            futureSailor.get(1, TimeUnit.SECONDS);
            fail("should have completed exceptionally");
        } catch (InterruptedException e) {
            throw e;
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof RetryFailed)) {
                throw e.getCause();
            }
        } catch (TimeoutException e) {
            throw e;
        }
    }

    @Test
    public void overflowIsHandledCorrectly() {
        final long cap = 60000L;
        final long base = 100L;

        final long wait1 = ExponentialBackoff.getWaitTime(cap, base, Long.MAX_VALUE);
        assertThat(wait1, is(equalTo(cap)));

        // MIN_VALUE will be capped
        final long wait2 = ExponentialBackoff.getWaitTime(cap, base, Long.MIN_VALUE);
        assertThat(wait2, is(equalTo(cap)));

        final long wait3 = ExponentialBackoff.getWaitTime(cap, base, Long.MIN_VALUE + 1);
        assertThat(wait3, is(equalTo(cap)));

        // -1 will be capped; Should result in min(60000, 2^(MAX_VALUE -1) * 100) =
        // 60000
        final long wait4 = ExponentialBackoff.getWaitTime(cap, base, -1);
        assertThat(wait4, is(equalTo(cap)));
    }

    @Test
    public void successfulAfterThreeAttempts() throws Exception {
        final AtomicInteger attempts = new AtomicInteger(0);
        final AtomicInteger exceptions = new AtomicInteger(0);
        CompletableFuture<String> futureSailor = new CompletableFuture<>();

        ExponentialBackoff.<String>newBuilder().setBase(1).setCap(5000).setMaxAttempts(5)
                          .setExceptionHandler(e -> exceptions.incrementAndGet()).execute(() -> {
                              attempts.incrementAndGet();
                              final boolean shouldThrow = attempts.get() < 3;
                              if (shouldThrow) {
                                  System.out.println("Throwing exception");
                                  throw new RuntimeException("Fake exception");
                              }
                              return "Fake result";
                          }, futureSailor, executor);

        String result = futureSailor.get(1, TimeUnit.SECONDS);

        assertThat(attempts.get(), is(equalTo(3)));
        assertThat(exceptions.get(), is(equalTo(2)));
        assertThat(result, is(equalTo("Fake result")));
    }

    @Test
    public void testInfiniteAttempts() throws Exception {
        final AtomicInteger attempts = new AtomicInteger(0);
        final AtomicInteger exceptions = new AtomicInteger(0);
        final Callable<String> task = () -> {
            attempts.incrementAndGet();
            // Just to ensure that we exceed the default max
            // attempts
            final boolean shouldThrow = attempts.get() < ExponentialBackoff.DEFAULT_MAX_ATTEMPTS + 1;
            if (shouldThrow) {
                System.out.println("Throwing exception");
                throw new RuntimeException("Fake exception");
            }
            return "Fake result";
        };
        CompletableFuture<String> futureSailor = new CompletableFuture<>();

        ExponentialBackoff.<String>newBuilder().setBase(1).setCap(10).setInfiniteAttempts()
                          .setExceptionHandler(e -> exceptions.incrementAndGet()).execute(task, futureSailor, executor);

        String result = futureSailor.get(1, TimeUnit.SECONDS);

        assertThat(attempts.get(), is(equalTo(ExponentialBackoff.DEFAULT_MAX_ATTEMPTS + 1)));
        assertThat(exceptions.get(), is(equalTo(ExponentialBackoff.DEFAULT_MAX_ATTEMPTS)));
        assertThat(result, is(equalTo("Fake result")));
    }

    @Test
    public void testSuccessfulExecution() throws Exception {
        CompletableFuture<String> futureSailor = new CompletableFuture<>();
        ExponentialBackoff.<String>newBuilder().setBase(100).setCap(5000).setMaxAttempts(5).setJitter()
                          .setExceptionHandler(Throwable::printStackTrace)
                          .execute(() -> "Do something", futureSailor, executor);

        String result = futureSailor.get(1, TimeUnit.SECONDS);

        assertThat(result, is(equalTo("Do something")));
    }

    @Test
    public void testThatMaxAttemptsAreExceeded() throws Throwable {
        CompletableFuture<Long> futureSailor = new CompletableFuture<>();
        ExponentialBackoff.<Long>newBuilder().setMaxAttempts(3).setBase(1)
                          .setExceptionHandler(e -> System.out.println(e.getMessage())).execute(() -> {
                              throw new RuntimeException("Fake exception");
                          }, futureSailor, executor);

        try {
            futureSailor.get(1, TimeUnit.SECONDS);
            fail("should have completed exceptionally");
        } catch (InterruptedException e) {
            throw e;
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof RetryFailed)) {
                throw e.getCause();
            }
        } catch (TimeoutException e) {
            throw e;
        }
    }
}
