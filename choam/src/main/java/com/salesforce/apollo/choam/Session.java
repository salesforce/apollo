/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.protobuf.Message;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitResult.Result;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.choam.support.TransactionCancelled;
import com.salesforce.apollo.choam.support.TransactionFailed;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Verifier;

/**
 * @author hal.hildebrand
 *
 */
public class Session {

    private final static Logger log = LoggerFactory.getLogger(Session.class);

    public static Transaction transactionOf(Digest source, int nonce, Message message, Signer signer) {
        ByteBuffer buff = ByteBuffer.allocate(4);
        buff.putInt(nonce);
        buff.flip();
        final var digeste = source.toDigeste();
        var sig = signer.sign(digeste.toByteString().asReadOnlyByteBuffer(), buff,
                              message.toByteString().asReadOnlyByteBuffer());
        return Transaction.newBuilder()
                          .setSource(digeste)
                          .setNonce(nonce)
                          .setContent(message.toByteString())
                          .setSignature(sig.toSig())
                          .build();
    }

    public static boolean verify(Transaction transaction, Verifier verifier) {
        ByteBuffer buff = ByteBuffer.allocate(4);
        buff.putInt(transaction.getNonce());
        buff.flip();
        return verifier.verify(JohnHancock.of(transaction.getSignature()),
                               transaction.getSource().toByteString().asReadOnlyByteBuffer(),
                               transaction.getContent().asReadOnlyByteBuffer());
    }

    private final Limiter<Void>                                limiter;
    private AtomicInteger                                      nonce     = new AtomicInteger();
    private final Parameters                                   params;
    private final Function<SubmittedTransaction, SubmitResult> service;
    private final Map<Digest, SubmittedTransaction>            submitted = new ConcurrentHashMap<>();

    public Session(Parameters params, Function<SubmittedTransaction, SubmitResult> service) {
        this.params = params;
        this.service = service;
        final var metrics = params.metrics();
        this.limiter = params.txnLimiterBuilder()
                             .build(params.member().getId().shortString(),
                                    metrics == null ? EmptyMetricRegistry.INSTANCE
                                                    : metrics.getMetricRegistry(params.context().getId().shortString()
                                                    + ".txnLimiter"));
    }

    /**
     * Cancel all pending transactions
     */
    public void cancelAll() {
        submitted.values()
                 .forEach(stx -> stx.onCompletion()
                                    .completeExceptionally(new TransactionCancelled("Transaction cancelled")));
    }

    /**
     * Submit a transaction.
     * 
     * @param transaction - the Message to submit as a transaction
     * @param timeout     - non null timeout of the transaction
     * @param scheduler
     * 
     * @return onCompletion - the future result of the submitted transaction
     * @throws InvalidTransaction - if the submitted transaction is invalid in any
     *                            way
     */
    public <T> CompletableFuture<T> submit(Message transaction, Duration timeout,
                                           ScheduledExecutorService scheduler) throws InvalidTransaction {
        final int n = nonce.getAndIncrement();

        final var txn = transactionOf(params.member().getId(), n, transaction, params.member());
        if (!txn.hasSource() || !txn.hasSignature()) {
            throw new InvalidTransaction();
        }
        var hash = CHOAM.hashOf(txn, params.digestAlgorithm());
        var result = new CompletableFuture<T>();
        if (timeout == null) {
            timeout = params.submitTimeout();
        }
        var stxn = new SubmittedTransaction(hash, txn, result);
        submitted.put(stxn.hash(), stxn);

        final var timer = params.metrics() == null ? null : params.metrics().transactionLatency().time();
        var backoff = params.submitPolicy().build();
        boolean submitted = false;
        var target = Instant.now().plus(timeout);
        int i = 0;
        while (Instant.now().isBefore(target)) {
            log.debug("Submitting: {} retry: {} on: {}", stxn.hash(), i, params.member().getId());
            if (stxn.onCompletion().isDone() || submit(stxn)) {
                submitted = true;
                break;
            }
            try {
                final var delay = backoff.nextBackoff();
                log.debug("Failed submitting: {} retry: {} delay: {}ms on: {}", stxn.hash(), i, delay.toMillis(),
                          params.member().getId());
                Thread.sleep(delay.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (params.metrics() != null) {
                params.metrics().transactionSubmitRetry();
            }
            i++;
        }
        if (!submitted) {
            if (params.metrics() != null) {
                params.metrics().transactionSubmittedBufferFull();
            }
            result.completeExceptionally(new TransactionFailed("Buffer Full"));
            return result;
        }
        var futureTimeout = scheduler.schedule(() -> {
            if (result.isDone()) {
                return;
            }
            log.debug("Timeout of txn: {} on: {}", hash, params.member().getId());
            final var to = new TimeoutException("Transaction timeout");
            result.completeExceptionally(to);
            if (params.metrics() != null) {
                params.metrics().transactionComplete(to);
            }
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);

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
        if (stxn != null) {
            log.trace("Completed: {} on: {}", hash, params.member().getId());
        }
        return stxn;
    }

    private void complete(Digest hash, final Timer.Context timer, Throwable t) {
        submitted.remove(hash);
        if (timer != null) {
            timer.close();
            log.trace("Transaction lifecycle complete: {} error: {} on: {}", hash, t, params.member().getId());
            params.metrics().transactionComplete(t);
        }
    }

    private boolean submit(SubmittedTransaction stx) {
        var listener = limiter.acquire(null);
        if (listener.isEmpty()) {
            log.debug("Transaction submission: {} rejected on: {}", stx.hash(), params.member().getId());
            if (params.metrics() != null) {
                params.metrics().transactionSubmittedFail();
            }
            stx.onCompletion().completeExceptionally(new TransactionFailed("Transaction submission rejected"));
            return false;
        }
        var result = service.apply(stx);

        if (result.getResult() == Result.PUBLISHED) {
            listener.get().onSuccess();
            log.trace("Transaction submitted: {} on: {}", stx.hash(), params.member().getId());
            if (params.metrics() != null) {
                params.metrics().transactionSubmittedSuccess();
            }
        } else {
            listener.get().onDropped();
            return false;
        }
        return true;
    }
}
