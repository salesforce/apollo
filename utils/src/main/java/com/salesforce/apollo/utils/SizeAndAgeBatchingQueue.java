package com.salesforce.apollo.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extends the {@link AgeBatchingQueue} to add one more reaping point based on
 * the current batch size.
 *
 * @author Nitesh Kant (nkant@netflix.com)
 */
public class SizeAndAgeBatchingQueue<T> extends AgeBatchingQueue<T> {

    class AgeAndSizeBatch extends AgeBatch {
 
        private final AtomicInteger currentBatchSize;

        protected AgeAndSizeBatch( ) {
            super(); 
            currentBatchSize = new AtomicInteger();
        }

        @Override
        protected boolean addEvent(T event) {
            if (currentBatchSize.get() >= batchSize) {
                if (!reapCurrentBatch("Batch size exceeded")) {
                    return false;
                }
            }
            if (super.addEvent(event)) {
                currentBatchSize.incrementAndGet();
                return true;
            }
            return false;
        }

        @Override
        protected void clear() {
            super.clear();
            currentBatchSize.set(0);
        }
    }

    private final int batchSize;

    public SizeAndAgeBatchingQueue(int batchSize, String label, int queueSize, int limit) {
        super(label, queueSize, limit);
        this.batchSize = batchSize;
    }

    @Override
    AgeBatch createNewBatch() {
        return new AgeAndSizeBatch();
    }
}
