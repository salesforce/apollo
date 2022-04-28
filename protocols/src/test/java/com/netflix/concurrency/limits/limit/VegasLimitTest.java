package com.netflix.concurrency.limits.limit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class VegasLimitTest {
    public static VegasLimit create() {
        return VegasLimit.newBuilder().alpha(3).beta(6).smoothing(1.0).initialLimit(10).maxConcurrency(20).build();
    }

    @Test
    public void largeLimitIncrease() {
        VegasLimit limit = VegasLimit.newBuilder().initialLimit(10000).maxConcurrency(20000).build();
        limit.onSample(0, TimeUnit.SECONDS.toNanos(10), 5000, false);
        assertEquals(10000, limit.getLimit());
        limit.onSample(0, TimeUnit.SECONDS.toNanos(10), 6000, false);
        assertEquals(10024, limit.getLimit());
    }

    @Test
    public void increaseLimit() {
        VegasLimit limit = create();
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(10), 10, false);
        assertEquals(10, limit.getLimit());
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(10), 11, false);
        assertEquals(16, limit.getLimit());
    }

    @Test
    public void decreaseLimit() {
        VegasLimit limit = create();
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(10), 10, false);
        assertEquals(10, limit.getLimit());
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(50), 11, false);
        assertEquals(9, limit.getLimit());
    }

    @Test
    public void noChangeIfWithinThresholds() {
        VegasLimit limit = create();
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(10), 10, false);
        assertEquals(10, limit.getLimit());
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(14), 14, false);
        assertEquals(10, limit.getLimit());
    }

    @Test
    public void decreaseSmoothing() {
        VegasLimit limit = VegasLimit.newBuilder()
                                     .decrease(current -> current / 2)
                                     .smoothing(0.5)
                                     .initialLimit(100)
                                     .maxConcurrency(200)
                                     .build();

        // Pick up first min-rtt
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(10), 100, false);
        assertEquals(100, limit.getLimit());

        // First decrease
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(20), 100, false);
        assertEquals(75, limit.getLimit());

        // Second decrease
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(20), 100, false);
        assertEquals(56, limit.getLimit());
    }

    @Test
    public void decreaseWithoutSmoothing() {
        VegasLimit limit = VegasLimit.newBuilder()
                                     .decrease(current -> current / 2)
                                     .initialLimit(100)
                                     .maxConcurrency(200)
                                     .build();

        // Pick up first min-rtt
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(10), 100, false);
        assertEquals(100, limit.getLimit());

        // First decrease
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(20), 100, false);
        assertEquals(50, limit.getLimit());

        // Second decrease
        limit.onSample(0, TimeUnit.MILLISECONDS.toNanos(20), 100, false);
        assertEquals(25, limit.getLimit());
    }
}
