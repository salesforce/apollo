/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private final static Logger log = LoggerFactory.getLogger(TxDataSource.class);

    private final Duration                           batchInterval;
    private volatile Thread                          blockingThread;
    private AtomicBoolean                            draining     = new AtomicBoolean();
    private final Duration                           maditoryLatency;
    private final Member                             member;
    private final ChoamMetrics                       metrics;
    private final CapacityBatchingQueue<Transaction> processing;
    private final BlockingQueue<Reassemble>          reassemblies = new LinkedBlockingQueue<>();
    private final BlockingQueue<Validate>            validations  = new LinkedBlockingQueue<>();

    public TxDataSource(Member member, int maxElements, ChoamMetrics metrics, int maxBatchByteSize,
                        Duration batchInterval, int maxBatchCount, Duration maditoryLatency) {
        this.member = member;
        this.batchInterval = batchInterval;
        this.maditoryLatency = maditoryLatency;
        processing = new CapacityBatchingQueue<Transaction>(maxElements, String.format("Tx DS[%s]", member.getId()),
                                                            maxBatchCount, maxBatchByteSize,
                                                            tx -> tx.toByteString().size(), 5);
        this.metrics = metrics;
    }

    public void close() {
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
        draining.set(true);
    }

    @Override
    public ByteString getData() {
        var builder = UnitData.newBuilder();
        log.trace("Requesting unit data on: {}", member);
        blockingThread = Thread.currentThread();
        final var then = Instant.now();
        var target = then.plus(batchInterval);
        var minimum = then.plus(maditoryLatency);
        try {
            while (true) {
                var batch = processing.nonBlockingTake();
                if (batch != null) {
                    builder.addAllTransactions(batch);
                }
                var r = new ArrayList<Reassemble>();
                reassemblies.drainTo(r);
                builder.addAllReassemblies(r);

                var v = new ArrayList<Validate>();
                validations.drainTo(v);
                builder.addAllValidations(v);

                if (builder.getTransactionsCount() > 0 || builder.getReassembliesCount() > 0 ||
                    builder.getValidationsCount() > 0) {
                    ByteString bs = builder.build().toByteString();
                    if (metrics != null) {
                        metrics.publishedBatch(builder.getTransactionsCount(), bs.size(),
                                               builder.getValidationsCount());
                    }
                    log.trace("Unit data: {} txns, {} validations {} reassemblies totalling: {} bytes  on: {}",
                              builder.getTransactionsCount(), builder.getValidationsCount(),
                              builder.getReassembliesCount(), bs.size(), member.getId());
                    var now = Instant.now();
                    if (now.isAfter(minimum)) {
                        return bs;
                    }
                    try {
                        Thread.sleep(maditoryLatency.minus(Duration.between(then, now)).toMillis());
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        return ByteString.EMPTY;
                    }
                    return bs;
                }
                if (Instant.now().isAfter(target)) {
                    log.trace("No data available on: {}", member.getId());
                    return ByteString.EMPTY;
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return ByteString.EMPTY;
                }
            }
        } finally {
            blockingThread = null;
        }
    }

    public int getProcessing() {
        return processing.size();
    }

    public int getRemaining() {
        return processing.size();
    }

    public int getRemainingReassemblies() {
        return reassemblies.size();
    }

    public int getRemainingValidations() {
        return validations.size();
    }

    public void offer(Reassemble reassembly) {
        reassemblies.offer(reassembly);
    }

    public boolean offer(Transaction txn) {
        if (!draining.get()) {
            return processing.offer(txn);
        } else {
            return false;
        }
    }

    public void offer(Validate generateValidation) {
        validations.offer(generateValidation);
    }

    public void start(Duration batchInterval, ScheduledExecutorService scheduler) {
        processing.start(batchInterval, scheduler);
    }
}
