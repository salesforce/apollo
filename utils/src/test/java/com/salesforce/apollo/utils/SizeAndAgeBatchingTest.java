package com.salesforce.apollo.utils;
 
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * @author Nitesh Kant (nkant@netflix.com)
 */
public class SizeAndAgeBatchingTest {

    @Test
    public void testAgeBatchAged() throws Exception { 

        AgeBatchingQueue<String> q = newQ();
        assertTrue( q.offer("Event1"), "Age batch queue offer failed.");
        q.invokeReaping("Testing");
        AgeBatchingQueue<String>.AgeBatch agedBatch = q.blockingTakeWithTimeout(6000);
        assertNotNull( agedBatch, "No batch available after batch age expired.");
    }

    @Test
    public void testAgeBatchYoung() throws Exception { 
        AgeBatchingQueue<String> q = newQ();
        String event = "Event1";
        assertTrue(q.offer(event), "Age batch queue offer failed.");

        AgeBatchingQueue<String>.AgeBatch currentBatch = q.getCurrentBatch();
        assertTrue( currentBatch.events.contains(event), "Offered event not in current batch");

        Object shdBeNull = q.nonBlockingTake();
        assertNull( shdBeNull, "Batch available before batch age expiry.");
    }

    @Test
    public void testQueueFull() throws Exception { 
        AgeBatchingQueue<String> q = newQ();
        for (int j=0; j < 2;j++) {
            for (int i = 0; i < 3; i++) {
                assertTrue( q.offer("event" + j +i), "Age batch queue offer failed.");
            }
        }

        while(q.getCurrentBatch().events.size() < 2) {
            q.offer("EventToFill");
        }

        assertFalse( q.offer("EventToFail"),"Offer not failing when queue is full");
        assertNotNull( q.blockingTake(), "No batch available after age expiry.");
        assertNotNull( q.blockingTake(), "No batch available after age expiry.");
        assertTrue( q.offer("EventToGoThrough"), "Offer failing when queue is not full");

    }

    private AgeBatchingQueue<String> newQ() {
        return new SizeAndAgeBatchingQueue<>(2, "something", 2);
    } 
}