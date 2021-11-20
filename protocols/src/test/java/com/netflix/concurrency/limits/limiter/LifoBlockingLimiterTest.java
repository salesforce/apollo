package com.netflix.concurrency.limits.limiter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.SettableLimit;

public class LifoBlockingLimiterTest { 

    final Executor executor = Executors.newCachedThreadPool();

    final SettableLimit limit = SettableLimit.startingAt(4);

    final SimpleLimiter<Void> simpleLimiter = SimpleLimiter.newBuilder().limit(limit).build();

    final LifoBlockingLimiter<Void> blockingLimiter = LifoBlockingLimiter.newBuilder(simpleLimiter).backlogSize(10)
                                                                         .backlogTimeout(1, TimeUnit.SECONDS).build();

    @Test
    public void blockWhenFullAndTimeout() {
        // Acquire all 4 available tokens
        for (int i = 0; i < 4; i++) {
            Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
            assertTrue(listener.isPresent());
        }

        // Next acquire will block for 1 second
        long start = System.nanoTime();
        Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
        long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        assertTrue(duration >= 1);
        assertFalse(listener.isPresent());
    }

    @Test
    public void unblockWhenFullBeforeTimeout() {
        // Acquire all 4 available tokens
        List<Optional<Limiter.Listener>> listeners = acquireN(blockingLimiter, 4);

        // Schedule one to release in 250 msec
        Executors.newSingleThreadScheduledExecutor().schedule(() -> listeners.get(0).get().onSuccess(), 250,
                                                              TimeUnit.MILLISECONDS);

        // Next acquire will block for 1 second
        long start = System.nanoTime();
        Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(duration >= 250);
        assertTrue(listener.isPresent());
    }

    @Test
    public void rejectWhenBacklogSizeReached() throws InterruptedException {
        acquireNAsync(blockingLimiter, 14);

        // Small delay to make sure all acquire() calls have been made
        TimeUnit.MILLISECONDS.sleep(250);

        // Next acquire will reject with no delay
        long start = System.nanoTime();
        Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(duration < 2000, "Duration = " + duration);
        assertFalse(listener.isPresent());
    }

    @Test
    public void adaptWhenLimitIncreases() {
        acquireN(blockingLimiter, 4);

        limit.setLimit(5);

        // Next acquire will succeed with no delay
        long start = System.nanoTime();
        Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(duration < 100, "Duration = " + duration);
        assertTrue(listener.isPresent());
    }

    @Test
    public void adaptWhenLimitDecreases() {
        List<Optional<Limiter.Listener>> listeners = acquireN(blockingLimiter, 4);

        limit.setLimit(3);

        listeners.get(0).get().onSuccess();

        // Next acquire will reject and block
        long start = System.nanoTime();
        Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
        long duration = TimeUnit.SECONDS.toMillis(System.nanoTime() - start);
        assertTrue(duration >= 1, "Duration = " + duration);
        assertFalse(listener.isPresent());
    }

//    @Test TODO - HSH figure out why this flaps ;)
    public void verifyFifoOrder() {
        // Make sure all tokens are acquired
        List<Optional<Limiter.Listener>> firstBatch = acquireN(blockingLimiter, 4);

        // Kick off 5 requests with a small delay to ensure futures are created in the
        // correct order
        List<Integer> values = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Integer>> futures = IntStream.range(0, 5).peek(i -> {
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException e) {
            }
        }).mapToObj(i -> CompletableFuture.supplyAsync(() -> {
            Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
            if (!listener.isPresent()) {
                return -1;
            }
            try {
                return i;
            } finally {
                listener.get().onSuccess();
            }
        }, executor)).peek(future -> future.whenComplete((value, error) -> values.add(value)))
                                                            .collect(Collectors.toList());

        // Release the first batch of tokens
        firstBatch.forEach(listener -> {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
            }
            listener.get().onSuccess();
        });

        // Make sure all requests finished
        futures.forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
            }
        });

        // Verify that results are in reverse order
        assertEquals(Arrays.asList(4, 3, 2, 1, 0), values);
    }

    private List<Optional<Limiter.Listener>> acquireN(Limiter<Void> limiter, int N) {
        return IntStream.range(0, N).mapToObj(i -> limiter.acquire(null))
                        .peek(listener -> assertTrue(listener.isPresent())).collect(Collectors.toList());
    }

    private List<CompletableFuture<Optional<Limiter.Listener>>> acquireNAsync(Limiter<Void> limiter, int N) {
        return IntStream.range(0, N).mapToObj(i -> CompletableFuture.supplyAsync(() -> limiter.acquire(null), executor))
                        .collect(Collectors.toList());
    }
}
