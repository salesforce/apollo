package com.netflix.concurrency.limits.limit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class AIMDLimitTest {
    @Test
    public void testDefault() {
        AIMDLimit limiter = AIMDLimit.newBuilder().initialLimit(10).build();
        assertEquals(10, limiter.getLimit());
    }

    @Test
    public void increaseOnSuccess() {
        AIMDLimit limiter = AIMDLimit.newBuilder().initialLimit(20).build();
        limiter.onSample(0, TimeUnit.MILLISECONDS.toNanos(1), 10, false);
        assertEquals(21, limiter.getLimit());
    }

    @Test
    public void decreaseOnDrops() {
        AIMDLimit limiter = AIMDLimit.newBuilder().initialLimit(30).build();
        limiter.onSample(0, 0, 0, true);
        assertEquals(27, limiter.getLimit());
    }
}
