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
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.Reassemble;
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

    private enum Mode {
        CLOSED, DRAIN, UNIT, VALIDATIONS;
    }

    private final static Logger log = LoggerFactory.getLogger(TxDataSource.class);

    private final Duration                           batchInterval;
    private volatile Thread                          blockingThread;
    private final Member                             member;
    private final ChoamMetrics                       metrics;
    private final AtomicReference<Mode>              mode         = new AtomicReference<>(Mode.UNIT);
    private final CapacityBatchingQueue<Transaction> processing;
    private final BlockingQueue<Reassemble>          reassemblies = new LinkedBlockingQueue<>();
    private final BlockingQueue<Validate>            validations  = new LinkedBlockingQueue<>();

    public TxDataSource(Member member, int maxElements, ChoamMetrics metrics, int maxBatchByteSize,
                        Duration batchInterval, int maxBatchCount) {
        this.member = member;
        this.batchInterval = batchInterval;
        processing = new CapacityBatchingQueue<Transaction>(maxElements, String.format("Tx DS[%s]", member.getId()),
                                                            maxBatchCount, maxBatchByteSize,
                                                            tx -> tx.toByteString().size(), 5);
        this.metrics = metrics;
    }

    public void close() {
        mode.set(Mode.CLOSED);
        final var current = blockingThread;
        if (current != null) {
            current.interrupt();
        }
        blockingThread = null;
        if (metrics != null) {
            metrics.dropped(processing.size(), validations.size());
        }
        log.trace("Closed with remaining txns: {} validations: {} on: {}", processing.size(), validations.size(),
                  member);
    }

    public void drain() {
        log.trace("Setting data source mode to DRAIN on: {}", member);
        mode.set(Mode.DRAIN);
    }

    @Override
    public ByteString getData() {
        var builder = UnitData.newBuilder();
        switch (mode.get()) {
        case CLOSED:
            return ByteString.EMPTY;
        case DRAIN:
        case UNIT:
            log.trace("Requesting unit data on: {}", member);
            Queue<Transaction> batch;
            try {
                batch = processing.blockingTakeWithTimeout(batchInterval, true);
            } catch (InterruptedException e) {
                return ByteString.EMPTY;
            }
            if (batch != null) {
                builder.addAllTransactions(batch);
            }
            break;
        case VALIDATIONS:
            log.trace("Requesting validations only on: {}", member);
            mode.set(Mode.CLOSED);
            blockingThread = Thread.currentThread();
            try {
                Validate validation = validations.take();
                if (validation != null) {
                    builder.addValidations(validation);
                } else {
                    System.out.println("No waiting validations on: " + member.getId());
                }
            } catch (InterruptedException e) {
                return ByteString.EMPTY;
            } finally {
                blockingThread = null;
            }
            break;
        default:
            break;
        }

        var vdx = new ArrayList<Validate>();
        validations.drainTo(vdx);
        builder.addAllValidations(vdx);

        var reass = new ArrayList<Reassemble>();
        reassemblies.drainTo(reass);
        builder.addAllReassemblies(reass);

        final var data = builder.build();
        final var bs = data.toByteString();
        if (metrics != null) {
            metrics.publishedBatch(data.getTransactionsCount(), bs.size(), data.getValidationsCount());
        }
        log.trace("Unit data: {} txns, {} validations {} reassemblies totalling: {} bytes  on: {}",
                  data.getTransactionsCount(), data.getValidationsCount(), data.getReassembliesCount(), bs.size(),
                  member.getId());
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

    public void offer(Reassemble reassembly) {
        reassemblies.offer(reassembly);
    }

    public boolean offer(Transaction txn) {
        switch (mode.get()) {
        case UNIT:
            return processing.offer(txn);
        default:
            return false;
        }
    }

    public void offer(Validate generateValidation) {
        validations.offer(generateValidation);
    }

    public void start(Duration batchInterval, ScheduledExecutorService scheduler) {
        processing.start(batchInterval, scheduler);
    }

    public void validationsOnly() {
        log.trace("Setting data source mode to VALIDATIONS on: {}", member);
        mode.set(Mode.VALIDATIONS);
    }
}
