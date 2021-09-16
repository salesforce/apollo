/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.ethereal.DataSource;

/**
 * 
 * The data source for CHOAM. Provides back pressure to the caller when the
 * capacity of the receiver is exceeded. This data source has a fixed capacity
 * and produces a data packet up to the maximum byte size allowed, if the
 * receiver has available data. Each time the data is pulled from the receiver,
 * the remaining capacity is reduced by the max buffer size. The receiver will
 * not accept any more data after the capacity has been used, regardless of
 * whether there is space available.
 * 
 * @author hal.hildebrand
 *
 */
public class TxDataSource implements DataSource {
    private final static Logger log = LoggerFactory.getLogger(TxDataSource.class);

    private final AtomicInteger              buffered   = new AtomicInteger();
    private final Parameters                 parameters;
    private final BlockingDeque<Transaction> processing = new LinkedBlockingDeque<>();
    private final AtomicInteger              remaining  = new AtomicInteger();

    public TxDataSource(Parameters parameters, int maxBufferSize) {
        this.parameters = parameters;
        remaining.set(maxBufferSize);
    }

    public int getBuffered() {
        return buffered.get();
    }

    @Override
    public ByteString getData() {
        Executions.Builder builder = Executions.newBuilder();
        int batchSize = 0;
        int bytesRemaining = parameters.producer().maxBatchByteSize();
        while (processing.peek() != null && bytesRemaining >= processing.peek().getSerializedSize()) {
            Transaction next = processing.poll();
            bytesRemaining -= next.getSerializedSize();
            batchSize++;
            builder.addExecutions(next);
        }
        int byteSize = parameters.producer().maxBatchByteSize() - bytesRemaining;
        buffered.addAndGet(-byteSize);
        if (parameters.metrics() != null) {
            parameters.metrics().publishedBatch(batchSize, byteSize);
        }
        if (batchSize > 0) {
            remaining.addAndGet(-parameters.producer().maxBatchByteSize());
        }
        log.trace("Processed: {} txns totalling: {} bytes  on: {}", batchSize, byteSize, parameters.member());
        return builder.build().toByteString();
    }

    public int getProcessing() {
        return processing.size();
    }

    public int getRemaining() {
        return remaining.get();
    }

    public boolean offer(Transaction txn) {
        if (remaining.addAndGet(-txn.getSerializedSize()) > 0) {
            buffered.addAndGet(txn.getSerializedSize());
            processing.add(txn);
            return true;
        } else {
            remaining.addAndGet(txn.getSerializedSize());
            return false;
        }
    }
}
