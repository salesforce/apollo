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
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.choam.support.TransactionFailed;
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
    private final RateLimiter                                              submitRateLimiter;
    private final Function<SubmittedTransaction, ListenableFuture<Status>> service;
    private final Map<Digest, SubmittedTransaction>                        submitted = new ConcurrentHashMap<>();

    public Session(Parameters params, Function<SubmittedTransaction, ListenableFuture<Status>> service) {
        this.params = params;
        this.service = service;
        submitRateLimiter = RateLimiter.create(params.txnPermits(), Duration.ofMillis(0));
    }

    /**
     * Cancel all pending transactions
     */
    public void cancelAll() {
        submitted.values()
                 .forEach(stx -> stx.onCompletion()
                                    .completeExceptionally(new TransactionFailed("Transaction cancelled")));
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
        submit(exec, stxn, scheduler);
        final var timer = params.metrics() == null ? null : params.metrics().transactionLatency().time();
        var futureTimeout = scheduler.schedule(() -> {
            log.debug("Timeout of txn: {} on: {}", hash, params.member());
            result.completeExceptionally(new TimeoutException("Transaction timeout"));
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

    private void submit(Executor exec, SubmittedTransaction stx, ScheduledExecutorService scheduler) {
        submitted.put(stx.hash(), stx);
        CompletableFuture<Status> submitted = new CompletableFuture<Status>().whenComplete((r, t) -> {
            if (!stx.onCompletion().isDone()) {
                return;
            }
            if (t != null) {
                stx.onCompletion().completeExceptionally(t);
            }
            if (r == null || !r.isOk()) {
                stx.onCompletion().completeExceptionally(new TimeoutException("Cannot submit txn"));
            }
        });
        final var clientBackoff = params.clientBackoff().retryIf(s -> {
            if (stx.onCompletion().isDone()) {
                return false;
            }
            if (s.isOk()) {
                if (params.metrics() != null) {
                    params.metrics().transactionSubmittedSuccess();
                }
                return false;
            }
            if (params.metrics() != null) {
                params.metrics().transactionSubmitRetry();
            }
            log.trace("Retrying: {} status: {} on: {}", stx.hash(), s, params.member());
            return true;
        }).build();
        clientBackoff.executeAsync(exec, () -> {
            if (stx.onCompletion().isDone()) {
                SettableFuture<Status> f = SettableFuture.create();
                f.set(Status.OK);
                return f;
            }
            if (!submitRateLimiter.tryAcquire()) {
                SettableFuture<Status> f = SettableFuture.create();
                f.set(Status.CANCELLED.withDescription("Client side rate limiting"));
                return f;
            }
            log.trace("Attempting submission of: {} on: {}", stx.hash(), params.member());
            return service.apply(stx);
        }, submitted, scheduler);
    }
}
