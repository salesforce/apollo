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
import com.salesforce.apollo.utils.BatchingQueue;

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

    private final Duration                   batchInterval;
    private volatile Thread                  blockingThread;
    private final AtomicBoolean              draining     = new AtomicBoolean();
    private final ExponentialBackoffPolicy   drainPolicy;
    private final Member                     member;
    private final ChoamMetrics               metrics;
    private final BatchingQueue<Transaction> processing;
    private final BlockingQueue<Reassemble>  reassemblies = new LinkedBlockingQueue<>();
    private final BlockingQueue<Validate>    validations  = new LinkedBlockingQueue<>();

    public TxDataSource(Member member, int maxElements, ChoamMetrics metrics, int maxBatchByteSize,
                        Duration batchInterval, int maxBatchCount, ExponentialBackoffPolicy drainPolicy) {
        this.member = member;
        this.batchInterval = batchInterval;
        this.drainPolicy = drainPolicy;
        processing = new BatchingQueue<Transaction>(maxElements, maxBatchCount, tx -> tx.toByteString().size(),
                                                    maxBatchByteSize);
        this.metrics = metrics;
    }

    public void close() {
        final var current = blockingThread;
        if (current != null) {
            current.interrupt();
        }
        blockingThread = null;
        log.trace("Closing with remaining txns: {}({}:{}) validations: {} reassemblies: {} on: {}", processing.size(),
                  processing.added(), processing.taken(), validations.size(), reassemblies.size(), member);
    }

    public void drain() {
        draining.set(true);
        if (metrics != null) {
            metrics.dropped(processing.size(), validations.size());
        }
        log.trace("Draining with remaining txns: {}({}:{}) on: {}", processing.size(), processing.added(),
                  processing.taken(), member);
    }

    @Override
    public ByteString getData() {
        var builder = UnitData.newBuilder();
        log.trace("Requesting unit data on: {}", member);
        blockingThread = Thread.currentThread();
        try {
            var r = new ArrayList<Reassemble>();
            var v = new ArrayList<Validate>();

            if (draining.get()) {
                var target = Instant.now().plus(drainPolicy.nextBackoff());
                while (target.isAfter(Instant.now()) && builder.getReassembliesCount() == 0 &&
                       builder.getValidationsCount() == 0) {
                    // rinse and repeat
                    r = new ArrayList<Reassemble>();
                    reassemblies.drainTo(r);
                    builder.addAllReassemblies(r);

                    v = new ArrayList<Validate>();
                    validations.drainTo(v);
                    builder.addAllValidations(v);

                    if (builder.getReassembliesCount() != 0 || builder.getValidationsCount() != 0) {
                        break;
                    }

                    // sleep waiting for input
                    try {
                        Thread.sleep(drainPolicy.getInitialBackoff().dividedBy(2).toMillis());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return ByteString.EMPTY;
                    }
                }
            } else {
                try {
                    var batch = processing.take(batchInterval);
                    if (batch != null) {
                        builder.addAllTransactions(batch);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return ByteString.EMPTY;
                }
            }

            // One more time into ye breech
            r = new ArrayList<Reassemble>();
            reassemblies.drainTo(r);
            builder.addAllReassemblies(r);

            v = new ArrayList<Validate>();
            validations.drainTo(v);
            builder.addAllValidations(v);

            ByteString bs = builder.build().toByteString();
            if (metrics != null) {
                metrics.publishedBatch(builder.getTransactionsCount(), bs.size(), builder.getValidationsCount());
            }
            log.trace("Unit data: {} txns, {} validations, {} reassemblies totalling: {} bytes  on: {}",
                      builder.getTransactionsCount(), builder.getValidationsCount(), builder.getReassembliesCount(),
                      bs.size(), member.getId());
            return bs;
        } finally {
            blockingThread = null;
        }
    }

    public int getRemainingReassemblies() {
        return reassemblies.size();
    }

    public int getRemainingTransactions() {
        return processing.size();
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
}
