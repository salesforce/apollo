/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static java.util.concurrent.CompletableFuture.supplyAsync;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.choam.support.TransationFailed;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

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

    public static Builder newBuilder() {
        return new Builder();
    }

    private final Bulkhead                                bulkhead;
    private final CircuitBreaker                          circuitBreaker;
    private final Parameters                              params;
    private final RateLimiter                             rateLimiter;
    private final Retry                                   retry;
    private final Function<SubmittedTransaction, Boolean> service;
    private final Map<Digest, SubmittedTransaction>       submitted = new ConcurrentHashMap<>();

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

    /**
     * Submit a transaction.
     * 
     * @param transaction - the Transaction to submit
     * @param timeout     - non null timeout of the transaction
     * @return onCompletion - the future result of the submitted transaction
     */
    public <T> CompletableFuture<T> submit(Transaction transaction, Duration timeout) {
        var hash = params.digestAlgorithm().digest(transaction.toByteString());
        var result = new CompletableFuture<T>();
        if (timeout == null) {
            timeout = params.submitTimeout();
        }
        final var timer = params.metrics() == null ? null : params.metrics().transactionLatency().time();
        var futureTimeout = params.scheduler()
                                  .schedule(() -> result.completeExceptionally(new TimeoutException("Transaction timeout")),
                                            timeout.toMillis(), TimeUnit.MILLISECONDS);
        var stxn = new SubmittedTransaction(hash, transaction, result);
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
        return submitted.remove(hash);
    }

    private void complete(Digest hash, final Timer.Context timer, Throwable t) {
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
        Decorators.ofCompletionStage(() -> supplyAsync(() -> service.apply(stx), params.dispatcher()))
                  .withBulkhead(bulkhead).withCircuitBreaker(circuitBreaker).withRateLimiter(rateLimiter)
                  .withRetry(retry, params.scheduler()).get().exceptionally(t -> {
                      stx.onCompletion().completeExceptionally(t);
                      return false;
                  });
    }
}
