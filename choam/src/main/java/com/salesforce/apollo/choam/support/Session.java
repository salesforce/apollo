/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public class Session {

    private final Logger log = LoggerFactory.getLogger(Session.class);

    private final Parameters                          parameters;
    private final BlockingDeque<SubmittedTransaction> pending           = new LinkedBlockingDeque<>();
    private final AtomicInteger                       pendingByteSize   = new AtomicInteger();
    private final Map<Digest, SubmittedTransaction>   submitted         = new ConcurrentHashMap<>();
    private final AtomicInteger                       submittedByteSize = new AtomicInteger(); 

    public Session(Parameters parameters) {
        this.parameters = parameters;
    }

    /**
     * Offer a transaction to be submitted by the Session.
     * 
     * @param onSubmit     - accepted when the transaction is submitted
     * @param onCompletion - accepted when the transaction is completed
     * @param transaction  - the Transaction to submit
     * @param timeout      - the duration of the timeout allowed, if null the
     *                     default timeout is applied
     * @return Digest of the submitted transaction, or null if the session cannot
     *         accept the transaction due to rate limiting
     */
    public <T> Digest offer(BiConsumer<Digest, Throwable> onSubmit, BiConsumer<T, Throwable> onCompletion,
                            Transaction transaction, Duration timeout) {
        return offer(onSubmit, false, onCompletion, transaction, timeout);
    }

    /**
     * Complete the transaction indicated by the supplied hash.
     * 
     * @return the SubmittedTransaction corresponding to the hash, null if not found
     */
    public SubmittedTransaction complete(Digest hash) {
        final SubmittedTransaction txn = submitted.remove(hash);
        if (txn == null) {
            return null;
        }
        if (txn.latency() != null) {
            txn.latency().close();
            parameters.metrics().transactionComplete();
        }
        return txn;
    }

    private Digest offer(BiConsumer<Digest, Throwable> onSubmit, boolean join, BiConsumer<?, Throwable> onCompletion,
                         Transaction transaction, Duration timeout) {
        final Digest hash = parameters.digestAlgorithm().digest(transaction.toByteString());
        var duration = timeout == null ? parameters.submitTimeout() : timeout;
        Future<?> futureTimeout = parameters.scheduler().schedule(() -> timeout(hash), duration.toMillis(),
                                                                  TimeUnit.MILLISECONDS);
        Timer.Context latency = parameters.metrics() == null ? null : parameters.metrics().sessionLatency().time();
        SubmittedTransaction s = new SubmittedTransaction(hash, onCompletion, transaction, futureTimeout, latency);
        pending.add(s);
        pendingByteSize.addAndGet(transaction.getSerializedSize());
        return s.hash();
    }

    private void timeout(Digest hash) {
        SubmittedTransaction txn = submitted.remove(hash);
        if (txn == null) {
            txn = pending.stream().filter(s -> hash.equals(s.hash())).findFirst().orElse(null);
            if (txn == null) {
                return;
            }
            pending.remove(txn);
            pendingByteSize.addAndGet(-txn.submitted().getSerializedSize());
            log.trace("Timed out pending txn: {} totalling: {} bytes  on: {}", hash,
                      txn.submitted().getSerializedSize(), parameters.member());
        } else {
            submittedByteSize.addAndGet(-txn.submitted().getSerializedSize());
            log.trace("Timed out submitted txn: {} totalling: {} bytes  on: {}", hash,
                      txn.submitted().getSerializedSize(), parameters.member());
        }
        txn.cancel();
        if (txn.latency() != null) {
            txn.latency().close();
            parameters.metrics().transactionTimeout();
        }
        txn.onCompletion().accept(null, new TimeoutException("Timeout of transaction: " + hash));
    }
}
