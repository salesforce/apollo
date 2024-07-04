/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.choam.proto.Assemblies;
import com.salesforce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.proto.UnitData;
import com.salesforce.apollo.choam.proto.Validate;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.membership.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The data source for CHOAM. Provides back pressure to the caller when the capacity of the receiver is exceeded. This
 * data source has a fixed capacity and produces a data packet up to the maximum byte size allowed, if the receiver has
 * available data. Each time the data is pulled from the receiver, the remaining capacity is reduced by the max buffer
 * size. The receiver will not accept any more data after the capacity has been used, regardless of whether there is
 * space available.
 *
 * @author hal.hildebrand
 */
public class TxDataSource implements DataSource {

    private final static Logger log = LoggerFactory.getLogger(TxDataSource.class);

    private final Duration                   batchInterval;
    private final AtomicBoolean              draining    = new AtomicBoolean();
    private final Member                     member;
    private final ChoamMetrics               metrics;
    private final BatchingQueue<Transaction> processing;
    private final BlockingQueue<Assemblies>  assemblies  = new LinkedBlockingQueue<>();
    private final BlockingQueue<Validate>    validations = new LinkedBlockingQueue<>();

    public TxDataSource(Member member, int maxElements, ChoamMetrics metrics, int maxBatchByteSize,
                        Duration batchInterval, int maxBatchCount) {
        this.member = member;
        this.batchInterval = batchInterval;
        processing = new BatchingQueue<Transaction>(maxElements, maxBatchCount, tx -> tx.toByteString().size(),
                                                    maxBatchByteSize);
        this.metrics = metrics;
    }

    public void close() {
        if (metrics != null) {
            metrics.dropped(processing.size(), validations.size(), assemblies.size());
        }
        log.debug("Closing with remaining txns: {}({}:{}) validations: {} assemblies: {} on: {}", processing.size(),
                  processing.added(), processing.taken(), validations.size(), assemblies.size(), member.getId());
    }

    public void drain() {
        draining.set(true);
        log.debug("Draining with remaining txns: {}({}:{}) on: {}", processing.size(), processing.added(),
                  processing.taken(), member.getId());
    }

    @Override
    public ByteString getData() {
        var builder = UnitData.newBuilder();
        var r = new ArrayList<Assemblies>();
        assemblies.drainTo(r);
        builder.addAllAssemblies(r);

        var v = new ArrayList<Validate>();
        validations.drainTo(v);
        builder.addAllValidations(v);
        if (!draining.get()) {
            if (processing.size() > 0 || (validations.isEmpty() || assemblies.isEmpty())) {
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
        }

        ByteString bs = builder.build().toByteString();
        if (metrics != null) {
            metrics.publishedBatch(builder.getTransactionsCount(), bs.size(), builder.getValidationsCount(),
                                   builder.getAssembliesCount());
        }
        log.trace("Unit data: {} txns, {} validations, {} assemblies totalling: {} bytes  on: {}",
                  builder.getTransactionsCount(), builder.getValidationsCount(), builder.getAssembliesCount(),
                  bs.size(), member.getId());
        return bs;
    }

    public int getRemainingReassemblies() {
        return assemblies.size();
    }

    public int getRemainingTransactions() {
        return processing.size();
    }

    public int getRemainingValidations() {
        return validations.size();
    }

    public boolean offer(Assemblies assemblies) {
        return this.assemblies.offer(assemblies);
    }

    public boolean offer(Transaction txn) {
        if (!draining.get()) {
            return processing.offer(txn);
        } else {
            return false;
        }
    }

    public boolean offer(Validate generateValidation) {
        return validations.offer(generateValidation);
    }

    public void reset() {
        log.debug("Clearing with remaining txns: {}({}:{}) validations: {} assemblies: {} on: {}", processing.size(),
                  processing.added(), processing.taken(), validations.size(), assemblies.size(), member.getId());
        processing.clear();
    }
}
