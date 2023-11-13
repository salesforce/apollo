package com.salesforce.apollo.choam.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import com.salesforce.apollo.choam.support.BatchingQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Nitesh Kant (nkant@netflix.com)
 */
public class BatchingQueueTest {

    @Test
    public void testBatch() throws Exception {
        var q = newQ();
        String event = "Event1";
        assertTrue(q.offer(event), "batch queue offer failed.");

        var currentBatch = q.getCurrentBatch();
        assertTrue(currentBatch.getEvents().contains(event), "Offered event not in current batch");

        var b = q.take(Duration.ofMillis(100));
        assertNotNull(b, "Batch not available");
    }

    @Test
    public void testQueueFull() throws Exception {
        var q = newQ();
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 3; i++) {
                assertTrue(q.offer("event" + j + i), "Batch queue offer failed: " + j + " : " + i);
            }
        }
        assertEquals(6, q.size());
        assertFalse(q.offer("EventToFail"), "Offer not failing when queue is full");
        assertNotNull(q.take(Duration.ofMillis(100)), "No batch available.");
        assertNotNull(q.take(Duration.ofMillis(100)), "No batch available.");
        assertFalse(q.offer("EventToGoThrough"), "Offer succeeding after total met");
        assertNull(q.take(Duration.ofMillis(100)), "No batch available.");

    }

    private BatchingQueue<String> newQ() {
        return new BatchingQueue<>(2, 3, s -> s.length(), 200);
    }
}
