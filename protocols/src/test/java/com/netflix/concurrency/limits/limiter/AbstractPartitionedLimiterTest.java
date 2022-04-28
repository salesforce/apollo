package com.netflix.concurrency.limits.limiter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limit.SettableLimit;

public class AbstractPartitionedLimiterTest {
    public static class TestPartitionedLimiter extends AbstractPartitionedLimiter<String> {
        public static class Builder extends AbstractPartitionedLimiter.Builder<Builder, String> {
            @Override
            protected Builder self() {
                return this;
            }
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public TestPartitionedLimiter(Builder builder) {
            super(builder);
        }
    }

    @Test
    public void limitAllocatedToBins() {
        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                                                                                                                .partitionResolver(Function.identity())
                                                                                                                .partition("batch",
                                                                                                                           0.3)
                                                                                                                .partition("live",
                                                                                                                           0.7)
                                                                                                                .limit(FixedLimit.of(10))
                                                                                                                .build();

        assertEquals(3, limiter.getPartition("batch").getLimit());
        assertEquals(7, limiter.getPartition("live").getLimit());
    }

    @Test
    public void useExcessCapacityUntilTotalLimit() {
        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                                                                                                                .partitionResolver(Function.identity())
                                                                                                                .partition("batch",
                                                                                                                           0.3)
                                                                                                                .partition("live",
                                                                                                                           0.7)
                                                                                                                .limit(FixedLimit.of(10))
                                                                                                                .build();

        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.acquire("batch").isPresent());
            assertEquals(i + 1, limiter.getPartition("batch").getInflight());
        }

        assertFalse(limiter.acquire("batch").isPresent());
    }

    @Test
    public void exceedTotalLimitForUnusedBin() {
        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                                                                                                                .partitionResolver(Function.identity())
                                                                                                                .partition("batch",
                                                                                                                           0.3)
                                                                                                                .partition("live",
                                                                                                                           0.7)
                                                                                                                .limit(FixedLimit.of(10))
                                                                                                                .build();

        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.acquire("batch").isPresent());
            assertEquals(i + 1, limiter.getPartition("batch").getInflight());
        }

        assertFalse(limiter.acquire("batch").isPresent());

        for (int i = 0; i < 7; i++) {
            assertTrue(limiter.acquire("live").isPresent());
            assertEquals(i + 1, limiter.getPartition("live").getInflight());
        }

        assertFalse(limiter.acquire("live").isPresent());
    }

    @Test
    public void rejectOnceAllLimitsReached() {
        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                                                                                                                .partitionResolver(Function.identity())
                                                                                                                .partition("batch",
                                                                                                                           0.3)
                                                                                                                .partition("live",
                                                                                                                           0.7)
                                                                                                                .limit(FixedLimit.of(10))
                                                                                                                .build();

        for (int i = 0; i < 3; i++) {
            assertTrue(limiter.acquire("batch").isPresent());
            assertEquals(i + 1, limiter.getPartition("batch").getInflight());
            assertEquals(i + 1, limiter.getInflight());
        }

        for (int i = 0; i < 7; i++) {
            assertTrue(limiter.acquire("live").isPresent());
            assertEquals(i + 1, limiter.getPartition("live").getInflight());
            assertEquals(i + 4, limiter.getInflight());
        }

        assertFalse(limiter.acquire("batch").isPresent());
        assertFalse(limiter.acquire("live").isPresent());
    }

    @Test
    public void releaseLimit() {
        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                                                                                                                .partitionResolver(Function.identity())
                                                                                                                .partition("batch",
                                                                                                                           0.3)
                                                                                                                .partition("live",
                                                                                                                           0.7)
                                                                                                                .limit(FixedLimit.of(10))
                                                                                                                .build();

        Optional<Limiter.Listener> completion = limiter.acquire("batch");
        for (int i = 1; i < 10; i++) {
            assertTrue(limiter.acquire("batch").isPresent());
            assertEquals(i + 1, limiter.getPartition("batch").getInflight());
        }

        assertEquals(10, limiter.getInflight());

        assertFalse(limiter.acquire("batch").isPresent());

        completion.get().onSuccess();
        assertEquals(9, limiter.getPartition("batch").getInflight());
        assertEquals(9, limiter.getInflight());

        assertTrue(limiter.acquire("batch").isPresent());
        assertEquals(10, limiter.getPartition("batch").getInflight());
        assertEquals(10, limiter.getInflight());
    }

    @Test
    public void setLimitReservesBusy() {
        SettableLimit limit = SettableLimit.startingAt(10);

        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                                                                                                                .partitionResolver(Function.identity())
                                                                                                                .partition("batch",
                                                                                                                           0.3)
                                                                                                                .partition("live",
                                                                                                                           0.7)
                                                                                                                .limit(limit)
                                                                                                                .build();

        limit.setLimit(10);
        assertEquals(3, limiter.getPartition("batch").getLimit());
        assertTrue(limiter.acquire("batch").isPresent());
        assertEquals(1, limiter.getPartition("batch").getInflight());
        assertEquals(1, limiter.getInflight());

        limit.setLimit(20);
        assertEquals(6, limiter.getPartition("batch").getLimit());
        assertEquals(1, limiter.getPartition("batch").getInflight());
        assertEquals(1, limiter.getInflight());
    }
}
