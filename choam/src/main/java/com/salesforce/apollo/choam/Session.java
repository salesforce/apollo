/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static java.util.concurrent.CompletableFuture.supplyAsync;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitResult.Outcome;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.choam.support.TransationFailed;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public class Session {
    private final static Logger log = LoggerFactory.getLogger(Session.class);

    private final Parameters                                                     params;
    private final Function<SubmittedTransaction, ListenableFuture<SubmitResult>> service;
    private AtomicInteger                                                        nonce = new AtomicInteger();

    private final Map<Digest, SubmittedTransaction> submitted = new ConcurrentHashMap<>();

    public Session(Parameters params, Function<SubmittedTransaction, ListenableFuture<SubmitResult>> service) {
        this.params = params;
        this.service = service;
    }

    /**
     * Cancel all pending transactions
     */
    public void cancelAll() {
        submitted.values().forEach(stx -> stx.onCompletion()
                                             .completeExceptionally(new TransationFailed("Transaction cancelled")));
    }

    /**
     * Submit a transaction.
     * 
     * @param transaction - the Message to submit as a transaction
     * @param timeout     - non null timeout of the transaction
     * @return onCompletion - the future result of the submitted transaction
     * @throws InvalidTransaction - if the submitted transaction is invalid in any
     *                            way
     */
    public <T> CompletableFuture<T> submit(Message transaction, Duration timeout) throws InvalidTransaction {
        final int n = nonce.getAndIncrement();
        var buff = ByteBuffer.allocate(4);
        buff.putInt(n);
        buff.flip();
        var signature = params.member().sign(params.member().getId().toByteBuffer(), buff,
                                             transaction.toByteString().asReadOnlyByteBuffer());
        final Transaction txn = Transaction.newBuilder().setSource(params.member().getId().toDigeste()).setNonce(n)
                                           .setContent(transaction.toByteString()).setSignature(signature.toSig())
                                           .build();
        if (!txn.hasSource() || !txn.hasSignature()) {
            throw new InvalidTransaction();
        }
        var hash = CHOAM.hashOf(txn, params.digestAlgorithm());
        var result = new CompletableFuture<T>();
        if (timeout == null) {
            timeout = params.submitTimeout();
        }
        final var timer = params.metrics() == null ? null : params.metrics().transactionLatency().time();
        var futureTimeout = params.scheduler()
                                  .schedule(() -> result.completeExceptionally(new TimeoutException("Transaction timeout")),
                                            timeout.toMillis(), TimeUnit.MILLISECONDS);
        var stxn = new SubmittedTransaction(hash, txn, result);
        submit(stxn);
        return result.whenComplete((r, t) -> {
            futureTimeout.cancel(true);
            complete(hash, timer, t);
        });
    }

    public int submitted() {
        return submitted.size();
    }

    SubmittedTransaction complete(Digest hash) {
        final SubmittedTransaction stxn = submitted.remove(hash);
        if (stxn == null) {
            log.trace("Completed, but not present: {} on: {}", hash, params.member());
        } else {
            log.debug("Completed, present: {} on: {}", hash, params.member());
        }
        return stxn;
    }

    private void complete(Digest hash, final Timer.Context timer, Throwable t) {
        log.trace("Transaction lifecycle complete: {} error: {} on: {}", hash, t, params.member());
        submitted.remove(hash);
        if (timer != null) {
            timer.close();
            if (t != null) {
                params.metrics().transactionComplete(t);
            }
        }
    }

    private void submit(SubmittedTransaction stx) {
        submitted.put(stx.hash(), stx);
        supplyAsync(() -> {
            log.trace("Attempting submission of: {} on: {}", stx.hash(), params.member());
            if (stx.onCompletion().isDone()) {
                return true;
            }
            SubmitResult submitResult;
            try {
                submitResult = service.apply(stx).get();
            } catch (InterruptedException e) {
                log.trace("Submission of: {} success: INTERRUPTED on: {}", stx.hash(), params.member());
                return false;
            } catch (ExecutionException e) {
                log.trace("Submission of: {} success: FAILED on: {}", stx.hash(), params.member(), e.getCause());
                return false;
            }
            if (submitResult == null) {
                log.trace("Submission of: {} success: FAILED on: {}", stx.hash(), params.member());
                return false;
            }
            log.trace("Submission of: {} success: {} on: {}", stx.hash(), submitResult.getOutcome(), params.member());
            return submitResult.getOutcome() == Outcome.SUCCESS;
        }, params.submitDispatcher()).whenComplete((r, t) -> {
            log.trace("Completion of txn: {} submit: {} exceptionally: {} on: {}", stx.hash(), r, t, params.member());
            if (t != null) {
                stx.onCompletion().completeExceptionally(t);
            } else if (!r) {
                stx.onCompletion().completeExceptionally(new TransationFailed("failed to complete transaction"));
            }
        });
    }
}
