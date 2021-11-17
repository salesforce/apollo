/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.UnitData;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.utils.CapacityBatchingQueue;

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

    private final Duration                           batchInterval;
    private volatile Thread                          blockingThread;
    private final AtomicBoolean                      closed          = new AtomicBoolean();
    private final Member                             member;
    private final CapacityBatchingQueue<Transaction> processing;
    private final BlockingQueue<Validate>            validations     = new LinkedBlockingQueue<>();
    private final AtomicBoolean                      validationsOnly = new AtomicBoolean(false);

    public TxDataSource(Member member, int maxElements, ChoamMetrics metrics, int maxBatchByteSize,
                        Duration batchInterval, int maxBatchCount) {
        this.member = member;
        this.batchInterval = batchInterval;
        processing = new CapacityBatchingQueue<Transaction>(maxElements, String.format("Tx DS[%s]", member.getId()),
                                                            maxBatchCount, maxBatchByteSize,
                                                            tx -> tx.toByteString().size(), 5);
    }

    public void close() {
        closed.set(true);
        validationsOnly.set(false);
        final var current = blockingThread;
        if (current != null) {
            current.interrupt();
        }
        blockingThread = null;
        log.trace("Closed with remaining txns: {} validations: {} on: {}", processing.size(), validations.size());
    }

    @Override
    public ByteString getData() {
        if (closed.get()) {
            return ByteString.EMPTY;
        }
//        log.info("Requesting Unit data on: {}",  member);
        var builder = UnitData.newBuilder();

        if (validationsOnly.get()) {
            blockingThread = Thread.currentThread();
            try {
                Validate validation = validations.poll(1, TimeUnit.SECONDS);
                while (!closed.get() && validation == null) {
                    validation = validations.poll(1, TimeUnit.SECONDS);
                }
                if (closed.get()) {
                    return ByteString.EMPTY;
                }
                builder.addValidations(validation);
            } catch (InterruptedException e) {
                return ByteString.EMPTY;
            } finally {
                blockingThread = null;
            }
        } else {
            Queue<Transaction> batch;
            try {
                batch = processing.blockingTakeWithTimeout(batchInterval);
            } catch (InterruptedException e) {
                return ByteString.EMPTY;
            }
            if (batch != null) {
                builder.addAllTransactions(batch);
            }
        }

        var vdx = new ArrayList<Validate>();
        validations.drainTo(vdx);
        builder.addAllValidations(vdx);

        final var data = builder.build();
        final var bs = data.toByteString();

        log.trace("Unit data txns: {} validations: {} totalling: {} bytes  on: {}", data.getTransactionsCount(),
                  data.getValidationsCount(), bs.size(), member);
        return bs;
    }

    public int getProcessing() {
        return processing.size();
    }

    public int getRemaining() {
        return processing.size();
    }

    public int getRemainingValidations() {
        return validations.size();
    }

    public boolean offer(Transaction txn) {
        if (closed.get() || validationsOnly.get()) {
            return false;
        }
        return processing.offer(txn);
    }

    public void offer(Validate generateValidation) {
        validations.offer(generateValidation);
    }

    public void start(Duration batchInterval, ScheduledExecutorService scheduler) {
        processing.start(batchInterval, scheduler);
    }

    public void validationsOnly(boolean only) {
        validationsOnly.set(only);
    }
}
