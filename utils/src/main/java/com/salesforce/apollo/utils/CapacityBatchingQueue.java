/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @author hal.hildebrand
 *
 */
public class CapacityBatchingQueue<T> extends SizeAndAgeBatchingQueue<T> {
    class LimitedBatch extends AgeAndSizeBatch {
        private final AtomicInteger byteSize;

        protected LimitedBatch() {
            super();
            byteSize = new AtomicInteger();
        }

        @Override
        protected boolean addEvent(T event) {
            if (byteSize.get() >= maxByteSize) {
                if (!reapCurrentBatch("Batch size exceeded")) {
                    return false;
                }
            }
            if (super.addEvent(event)) {
                byteSize.addAndGet(sizer.apply(event));
                return true;
            }
            return false;
        }

        @Override
        protected void clear() {
            super.clear();
            byteSize.set(0);
        }
    }

    private final int                  maxByteSize;
    private final Function<T, Integer> sizer;

    public CapacityBatchingQueue(int limit, String label, int batchSize, int maxByteSize, Function<T, Integer> sizer,
                                 int queueSize) {
        super(batchSize, label, queueSize, limit);
        this.maxByteSize = maxByteSize;
        this.sizer = sizer;
    }

    @Override
    AgeBatch createNewBatch() {
        return new LimitedBatch();
    }
}
