/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static java.util.concurrent.CompletableFuture.supplyAsync;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.choam.support.TransationFailed;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.Utils;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.decorators.Decorators;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

/**
 * @author hal.hildebrand
 *
 */
public class Session {
    public static class Builder {
        private BulkheadConfig           bulkhead        = BulkheadConfig.ofDefaults();
        private CircuitBreakerConfig     circuitBreaker  = CircuitBreakerConfig.ofDefaults();
        private DigestAlgorithm          digestAlgorithm = DigestAlgorithm.DEFAULT;
        private String                   label           = "Unlabeled";
        private ChoamMetrics             metrics;
        private RateLimiterConfig        rateLimiter     = RateLimiterConfig.ofDefaults();
        private RetryConfig              retry           = RetryConfig.ofDefaults();
        private ScheduledExecutorService scheduler;

        public Session build(Parameters params, Function<SubmittedTransaction, Boolean> service) {
            return new Session(Bulkhead.of(label, bulkhead), CircuitBreaker.of(label, circuitBreaker),
                               RateLimiter.of(label, rateLimiter), Retry.of(label, retry), params, service);
        }

        public BulkheadConfig getBulkhead() {
            return bulkhead;
        }

        public CircuitBreakerConfig getCircuitBreaker() {
            return circuitBreaker;
        }

        public DigestAlgorithm getDigestAlgorithm() {
            return digestAlgorithm;
        }

        public String getLabel() {
            return label;
        }

        public ChoamMetrics getMetrics() {
            return metrics;
        }

        public RateLimiterConfig getRateLimiter() {
            return rateLimiter;
        }

        public ScheduledExecutorService getScheduler() {
            return scheduler;
        }

        public Builder setBulkhead(BulkheadConfig bulkhead) {
            this.bulkhead = bulkhead;
            return this;
        }

        public Builder setCircuitBreaker(CircuitBreakerConfig circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
            return this;
        }

        public Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
            return this;
        }

        public Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder setMetrics(ChoamMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder setRateLimiter(RateLimiterConfig rateLimiter) {
            this.rateLimiter = rateLimiter;
            return this;
        }

        public Builder setScheduler(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
            return this;
        }
    }

    private final static Logger log = LoggerFactory.getLogger(Session.class);

    public static Builder newBuilder() {
        return new Builder();
    }

    private final Bulkhead                                bulkhead;
    private final CircuitBreaker                          circuitBreaker;
    private final Parameters                              params;
    private final RateLimiter                             rateLimiter;
    private final Retry                                   retry;
    private final Function<SubmittedTransaction, Boolean> service;

    private final Map<Digest, SubmittedTransaction> submitted = new ConcurrentHashMap<>();

    private Session(Bulkhead bulkhead, CircuitBreaker circutBreaker, RateLimiter rateLimiter, Retry retry,
                    Parameters params, Function<SubmittedTransaction, Boolean> service) {
        this.bulkhead = bulkhead;
        this.circuitBreaker = circutBreaker;
        this.rateLimiter = rateLimiter;
        this.params = params;
        this.service = service;
        this.retry = retry;
    }

    /**
     * Cancel all pending transactions
     */
    public void cancelAll() {
        submitted.values().forEach(stx -> stx.onCompletion()
                                             .completeExceptionally(new TransationFailed("Transaction cancelled")));
    }

    public <T> CompletableFuture<T> submit(Join join, Duration timeout) throws InvalidTransaction {
        long[] longs = new long[params.digestAlgorithm().longLength()];
        final SecureRandom entropy = Utils.secureEntropy();
        for (int i = 0; i < longs.length; i++) {
            longs[i] = entropy.nextLong();
        }
        var nonce = new Digest(params.digestAlgorithm().digestCode(), longs);
        var signature = params.member().sign(params.member().getId().toByteBuffer(), nonce.toByteBuffer(),
                                             join.toByteString().asReadOnlyByteBuffer());
        return submit(Transaction.newBuilder().setSource(params.member().getId().toDigeste())
                                 .setNonce(nonce.toDigeste()).setJoin(join).setSignature(signature.toSig()).build(),
                      timeout, true);
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
        long[] longs = new long[params.digestAlgorithm().longLength()];
        final SecureRandom entropy = Utils.secureEntropy();
        for (int i = 0; i < longs.length; i++) {
            longs[i] = entropy.nextLong();
        }
        var nonce = new Digest(params.digestAlgorithm().digestCode(), longs);
        var signature = params.member().sign(params.member().getId().toByteBuffer(), nonce.toByteBuffer(),
                                             transaction.toByteString().asReadOnlyByteBuffer());
        final Transaction txn = Transaction.newBuilder().setSource(params.member().getId().toDigeste())
                                           .setNonce(nonce.toDigeste()).setUser(transaction.toByteString())
                                           .setSignature(signature.toSig()).build();
        return submit(txn, timeout, false);
    }

    /**
     * Submit a transaction.
     * 
     * @param transaction - the Transaction to submit
     * @param timeout     - non null timeout of the transaction
     * @return onCompletion - the future result of the submitted transaction
     * @throws InvalidTransaction - if the transaction is invalid in any way
     */
    public <T> CompletableFuture<T> submit(Transaction transaction, Duration timeout,
                                           boolean join) throws InvalidTransaction {
        if (!transaction.hasNonce() || !transaction.hasSource() || !transaction.hasSignature()) {
            throw new InvalidTransaction();
        }
        var hash = CHOAM.hashOf(transaction, params.digestAlgorithm());
        var result = new CompletableFuture<T>();
        if (timeout == null) {
            timeout = params.submitTimeout();
        }
        final var timer = params.metrics() == null ? null : params.metrics().transactionLatency().time();
        var futureTimeout = params.scheduler()
                                  .schedule(() -> result.completeExceptionally(new TimeoutException("Transaction timeout")),
                                            timeout.toMillis(), TimeUnit.MILLISECONDS);
        var stxn = new SubmittedTransaction(hash, transaction, result);
        if (join) {
            submitJoin(stxn);
        } else {
            submit(stxn);
        }
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

    private void submitJoin(SubmittedTransaction stx) {
        submitted.put(stx.hash(), stx);
        Decorators.ofCompletionStage(() -> supplyAsync(() -> {
            log.trace("Attempting submission of: {} on: {}", stx.hash(), params.member());
            if (stx.onCompletion().isDone()) {
                return true;
            }
            final @Nullable Boolean success = service.apply(stx);
            log.trace("Submission of: {} success: {} on: {}", stx.hash(), success, params.member());
            return success;
        }, params.submitDispatcher())).withRetry(retry, params.scheduler()).get()
                  .whenComplete((r, t) -> {
                      log.trace("Completion of join txn: {} submit: {} exceptionally: {} on: {}", stx.hash(), r, t,
                                params.member());
                      if (t != null) {
                          stx.onCompletion().completeExceptionally(t);
                      } else if (!r) {
                          stx.onCompletion()
                             .completeExceptionally(new TransationFailed("failed to complete transaction"));
                      }
                  });
    }

    private void submit(SubmittedTransaction stx) {
        submitted.put(stx.hash(), stx);
        Decorators.ofCompletionStage(() -> supplyAsync(() -> {
            log.trace("Attempting submission of: {} on: {}", stx.hash(), params.member());
            if (stx.onCompletion().isDone()) {
                return true;
            }
            final @Nullable Boolean success = service.apply(stx);
            log.trace("Submission of: {} success: {} on: {}", stx.hash(), success, params.member());
            return success;
        }, params.submitDispatcher())).withRetry(retry, params.scheduler()).get().whenComplete((r, t) -> {
            log.trace("Completion of txn: {} submit: {} exceptionally: {} on: {}", stx.hash(), r, t, params.member());
            if (t != null) {
                stx.onCompletion().completeExceptionally(t);
            } else if (!r) {
                stx.onCompletion().completeExceptionally(new TransationFailed("failed to complete transaction"));
            }
        });
    }
}
