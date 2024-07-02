/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.codahale.metrics.Timer;
import com.google.protobuf.Message;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.salesforce.apollo.choam.proto.SubmitResult;
import com.salesforce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.choam.support.TransactionFailed;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.Signer;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author hal.hildebrand
 */
public class Session {

    private final static Logger log = LoggerFactory.getLogger(Session.class);

    private final Limiter<Void>                                limiter;
    private final Parameters                                   params;
    private final Function<SubmittedTransaction, SubmitResult> service;
    private final Map<Digest, SubmittedTransaction>            submitted = new ConcurrentHashMap<>();
    private final AtomicReference<HashedCertifiedBlock>        view      = new AtomicReference<>();
    private final ScheduledExecutorService                     scheduler;
    private final AtomicInteger                                nonce     = new AtomicInteger();

    public Session(Parameters params, Function<SubmittedTransaction, SubmitResult> service,
                   ScheduledExecutorService scheduler) {
        this.params = params;
        this.service = service;
        final var metrics = params.metrics();
        this.limiter = params.txnLimiterBuilder()
                             .build(params.member().getId().shortString(),
                                    metrics == null ? EmptyMetricRegistry.INSTANCE : metrics.getMetricRegistry(
                                    params.context().getId().shortString() + ".txnLimiter"));
        this.scheduler = scheduler;
    }

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

    public static <T> CompletableFuture<T> retryNesting(Supplier<CompletableFuture<T>> supplier, int maxRetries) {
        CompletableFuture<T> cf = supplier.get();
        for (int i = 0; i < maxRetries; i++) {
            final var attempt = i;
            cf = cf.thenApply(CompletableFuture::completedFuture).exceptionally(e -> {
                log.info("resubmitting after attempt: {} exception: {}", attempt + 1, e.toString());
                return supplier.get();
            }).thenCompose(java.util.function.Function.identity());
        }
        return cf;
    }

    /**
     * Cancel all pending transactions
     */
    public void cancelAll() {
        submitted.values().forEach(stx -> stx.onCompletion().cancel(true));
    }

    public void setView(HashedCertifiedBlock v) {
        view.set(v);
        var currentHeight = v.height();
        for (var it = submitted.entrySet().iterator(); it.hasNext(); ) {
            var e = it.next();
            if (e.getValue().view().compareTo(currentHeight) < 0) {
                e.getValue().onCompletion().cancel(true);
                it.remove();
            }
        }
    }

    /**
     * Submit a transaction.
     *
     * @param transaction - the Message to submit as a transaction
     * @param timeout     - non-null timeout of the transaction
     * @return onCompletion - the future result of the submitted transaction
     * @throws InvalidTransaction - if the submitted transaction is invalid in any way
     */
    public <T> CompletableFuture<T> submit(Message transaction, Duration timeout) throws InvalidTransaction {
        final var txnView = view.get();
        if (txnView == null) {
            throw new InvalidTransaction("No view available");
        }
        final int n = nonce.getAndIncrement();

        final var txn = transactionOf(params.member().getId(), n, transaction, params.member());
        if (!txn.hasSource() || !txn.hasSignature()) {
            throw new InvalidTransaction();
        }
        var hash = CHOAM.hashOf(txn, params.digestAlgorithm());
        final var timer = params.metrics() == null ? null : params.metrics().transactionLatency().time();

        var result = new CompletableFuture<T>().whenComplete((r, t) -> {
            if (params.metrics() != null) {
                if (t instanceof CancellationException) {
                    params.metrics().transactionCancelled();
                }
            }
        });
        if (timeout == null) {
            timeout = params.submitTimeout();
        }

        var stxn = new SubmittedTransaction(txnView.height(), hash, txn, result, timer);
        submitted.put(stxn.hash(), stxn);

        var backoff = params.submitPolicy().build();
        var target = Instant.now().plus(timeout);
        int i = 0;

        while (!result.isDone() && Instant.now().isBefore(target)) {
            if (i > 0) {
                if (params.metrics() != null) {
                    params.metrics().transactionSubmitRetry();
                }
            }
            log.trace("Submitting: {} retry: {} on: {}", stxn.hash(), i, params.member().getId());
            var submit = submit(stxn);
            switch (submit.result.getResult()) {
            case PUBLISHED -> {
                submit.limiter.get().onSuccess();
                log.trace("Transaction submitted: {} on: {}", stxn.hash(), params.member().getId());
                if (params.metrics() != null) {
                    params.metrics().transactionSubmittedSuccess();
                }
                var futureTimeout = scheduler.schedule(() -> Thread.ofVirtual().start(Utils.wrapped(() -> {
                    if (result.isDone()) {
                        return;
                    }
                    log.debug("Timeout of txn: {} on: {}", hash, params.member().getId());
                    final var to = new TimeoutException("Transaction timeout");
                    result.completeExceptionally(to);
                    if (params.metrics() != null) {
                        params.metrics().transactionComplete(to);
                    }
                }, log)), timeout.toMillis(), TimeUnit.MILLISECONDS);
                return result.whenComplete((r, t) -> {
                    futureTimeout.cancel(true);
                    complete(hash, timer, t);
                });
            }
            case RATE_LIMITED -> {
                if (params.metrics() != null) {
                    params.metrics().transactionSubmitRateLimited();
                }
            }
            case BUFFER_FULL -> {
                if (params.metrics() != null) {
                    params.metrics().transactionSubmittedBufferFull();
                }
                submit.limiter.get().onDropped();
            }
            case INACTIVE, NO_COMMITTEE -> {
                if (params.metrics() != null) {
                    params.metrics().transactionSubmittedInvalidCommittee();
                }
                submit.limiter.get().onDropped();
            }
            case UNAVAILABLE -> {
                if (params.metrics() != null) {
                    params.metrics().transactionSubmittedUnavailable();
                }
                submit.limiter.get().onIgnore();
            }
            case INVALID_SUBMIT, ERROR_SUBMITTING -> {
                if (params.metrics() != null) {
                    params.metrics().transactionSubmissionError();
                }
                result.completeExceptionally(
                new TransactionFailed("Invalid submission: " + submit.result.getErrorMsg()));
                submit.limiter.get().onIgnore();
            }
            case UNRECOGNIZED, INVALID_RESULT -> {
                if (params.metrics() != null) {
                    params.metrics().transactionSubmittedInvalidResult();
                }
                var ex = new TransactionFailed("Unrecognized or invalid result: " + submit.result.getErrorMsg());
                result.completeExceptionally(ex);
                submit.limiter.get().onIgnore();
                return result;
            }
            default -> {
                if (params.metrics() != null) {
                    params.metrics().transactionSubmittedInvalidResult();
                }
                var ex = new TransactionFailed("Illegal result: " + submit.result.getErrorMsg());
                result.completeExceptionally(ex);
                submit.limiter.get().onIgnore();
                return result;
            }
            }
            try {
                final var delay = backoff.nextBackoff();
                log.debug("Failed submitting: {} result: {} retry: {} delay: {}ms on: {}", stxn.hash(), submit.result,
                          i, delay.toMillis(), params.member().getId());
                Thread.sleep(delay.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            i++;
        }
        if (result.isDone()) {
            return result;
        }

        if (params.metrics() != null) {
            params.metrics().transactionSubmitRetriesExhausted();
        }
        result.completeExceptionally(new TransactionFailed("Submission retries exhausted"));
        return result;
    }

    /**
     * Submit a transaction.
     *
     * @param transaction - the Message to submit as a transaction
     * @param retries     - the number of retries for Cancelled transaction submissions
     * @param timeout     - non-null timeout of the transaction
     * @return onCompletion - the future result of the submitted transaction
     * @throws InvalidTransaction - if the submitted transaction is invalid in any way
     */
    public <T> CompletableFuture<T> submit(Message transaction, int retries, Duration timeout)
    throws InvalidTransaction {
        return retryNesting(() -> {
            try {
                return submit(transaction, timeout);
            } catch (InvalidTransaction e) {
                throw new IllegalStateException("Invalid txn", e);
            }
        }, retries);
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

    private Submission submit(SubmittedTransaction stx) {
        var listener = limiter.acquire(null);
        if (listener.isEmpty()) {
            log.debug("Transaction submission: {} rejected on: {}", stx.hash(), params.member().getId());
            if (params.metrics() != null) {
                params.metrics().transactionSubmittedFail();
            }
            stx.onCompletion().completeExceptionally(new TransactionFailed("Transaction rate limited"));
            return new Submission(SubmitResult.newBuilder().setResult(SubmitResult.Result.RATE_LIMITED).build(),
                                  listener);
        }
        log.debug("Submitting txn: {} on: {}", stx.hash(), params.member().getId());
        return new Submission(service.apply(stx), listener);
    }

    private record Submission(SubmitResult result, Optional<Limiter.Listener> limiter) {
    }
}
