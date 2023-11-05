package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.SettableLimit;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

public class BlockingLimiterTest {
    @Test
    public void failOnHighTimeout() {
        SettableLimit limit = SettableLimit.startingAt(1);
        try {
            @SuppressWarnings("unused")
            BlockingLimiter<Void> limiter = BlockingLimiter.wrap(SimpleLimiter.newBuilder().limit(limit).build(),
                                                                 Duration.ofDays(1));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void test() {
        SettableLimit limit = SettableLimit.startingAt(10);
        BlockingLimiter<Void> limiter = BlockingLimiter.wrap(SimpleLimiter.newBuilder().limit(limit).build());

        LinkedList<Limiter.Listener> listeners = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            limiter.acquire(null).ifPresent(listeners::add);
        }

        limit.setLimit(1);

        while (!listeners.isEmpty()) {
            listeners.remove().onSuccess();
        }

        limiter.acquire(null);
    }

    @Test
    public void testMultipleBlockedThreads() throws InterruptedException, ExecutionException, TimeoutException {
        int numThreads = 80;
        SettableLimit limit = SettableLimit.startingAt(1);
        BlockingLimiter<Void> limiter = BlockingLimiter.wrap(SimpleLimiter.newBuilder().limit(limit).build());
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads, Thread.ofVirtual().factory());
        try {
            for (Future<?> future : IntStream.range(0, numThreads)
                                             .mapToObj(
                                             x -> executorService.submit(() -> limiter.acquire(null).get().onSuccess()))
                                             .collect(Collectors.toList())) {
                future.get(1, TimeUnit.SECONDS);
            }
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testNoTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        SettableLimit limit = SettableLimit.startingAt(1);
        BlockingLimiter<Void> limiter = BlockingLimiter.wrap(SimpleLimiter.newBuilder().limit(limit).build());
        limiter.acquire(null);
        try {
            CompletableFuture<Optional<Limiter.Listener>> future = CompletableFuture.supplyAsync(
            () -> limiter.acquire(null));
            future.get(1, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
    }

    @Test
    public void testTimeout() {
        Duration timeout = Duration.ofMillis(50);
        SettableLimit limit = SettableLimit.startingAt(1);
        BlockingLimiter<Void> limiter = BlockingLimiter.wrap(SimpleLimiter.newBuilder().limit(limit).build(), timeout);

        // Acquire first, will succeeed an not block
        limiter.acquire(null);

        // Second acquire should time out after at least 50 millis
        Instant before = Instant.now();
        assertFalse(limiter.acquire(null).isPresent());
        Instant after = Instant.now();

        Duration delay = Duration.between(before, after);
        var delta = timeout.minus(delay).abs().toMillis();
        System.out.println("Delay was " + delay.toMillis() + " millis, expected: " + timeout.toMillis() + " millis");
    }
}
