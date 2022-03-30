/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.choam.support.TransactionCancelled;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Verifier;

import io.grpc.Status;

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

    private AtomicInteger                                                  nonce     = new AtomicInteger();
    private final Parameters                                               params;
    private final Function<SubmittedTransaction, ListenableFuture<Status>> service;
    private final Map<Digest, SubmittedTransaction>                        submitted = new ConcurrentHashMap<>();

    public Session(Parameters params, Function<SubmittedTransaction, ListenableFuture<Status>> service) {
        this.params = params;
        this.service = service;
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
     * @param exec
     * @return onCompletion - the future result of the submitted transaction
     * @throws InvalidTransaction - if the submitted transaction is invalid in any
     *                            way
     */
    public <T> CompletableFuture<T> submit(Executor exec, Message transaction, Duration timeout,
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
        var futureTimeout = scheduler.schedule(() -> {
            log.debug("Timeout of txn: {} on: {}", hash, params.member());
            result.completeExceptionally(new TimeoutException("Transaction timeout"));
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);

        submit(exec, stxn);
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
            log.trace("Completed: {} on: {}", hash, params.member());
        }
        return stxn;
    }

    private void complete(Digest hash, final Timer.Context timer, Throwable t) {
        submitted.remove(hash);
        if (timer != null) {
            timer.close();
            log.trace("Transaction lifecycle complete: {} error: {} on: {}", hash, t, params.member());
            params.metrics().transactionComplete(t);
        }
    }

    private void submit(Executor exec, SubmittedTransaction stx) {
        var listener = params.txnLimiter().acquire(null);
        if (listener.isEmpty()) {
            log.trace("Transaction submission: {} rejected on: {}", stx.hash(), params.member());
            stx.onCompletion().completeExceptionally(new TimeoutException("Transaction submission rejected"));
            return;
        }
        var futureResult = service.apply(stx);

        try {
            futureResult.addListener(() -> {
                if (!stx.onCompletion().isDone()) {
                    listener.get().onIgnore();
                    log.trace("Transaction submission: {} already completed on: {}", stx.hash(), params.member());
                    return;
                }

                try {
                    var status = futureResult.get();
                    if (status == null || !status.isOk()) {
                        listener.get().onDropped();
                        log.trace("Transaction submission: {} status: {} timeout on: {}", stx.hash(), status,
                                  params.member());
                        final var timeoutEx = new TimeoutException("Cannot submit txn");
                        stx.onCompletion().completeExceptionally(timeoutEx);
                        if (params.metrics() != null) {
                            params.metrics().transactionComplete(timeoutEx);
                        }
                        return;
                    }
                    listener.get().onSuccess();
                    log.trace("Transaction submission: {} status: {} on: {}", stx.hash(), status, params.member());
                    if (params.metrics() != null) {
                        params.metrics().transactionSubmittedSuccess();
                    }
                } catch (InterruptedException e) {
                    listener.get().onDropped();
                    log.trace("Transaction submission: {} interrupted on: {}", stx.hash(), params.member(), e);
                    stx.onCompletion().completeExceptionally(e);
                    if (params.metrics() != null) {
                        params.metrics().transactionComplete(e);
                    }
                } catch (ExecutionException e) {
                    listener.get().onDropped();
                    log.trace("Transaction submission: {} completed exceptionally on: {}", stx.hash(), params.member(),
                              e.getCause());
                    stx.onCompletion().completeExceptionally(e.getCause());
                    if (params.metrics() != null) {
                        params.metrics().transactionComplete(e.getCause());
                    }
                }
            }, exec);
        } catch (RejectedExecutionException e) {
            listener.get().onDropped();
            log.trace("Transaction submission: {} rejected on: {}", stx.hash(), params.member(), e);
            stx.onCompletion().completeExceptionally(e);
            if (params.metrics() != null) {
                params.metrics().transactionComplete(e);
            }
        }
    }
}
