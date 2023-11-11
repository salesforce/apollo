/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.codahale.metrics.*;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.memberships.comm.EtherealMetrics;
import com.salesforce.apollo.ethereal.memberships.comm.EtherealMetricsImpl;
import com.salesforce.apollo.membership.messaging.rbc.RbcMetrics;
import com.salesforce.apollo.membership.messaging.rbc.RbcMetricsImpl;
import com.salesforce.apollo.protocols.EndpointMetricsImpl;
import com.salesforce.apollo.protocols.LimitsRegistry;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author hal.hildebrand
 */
public class ChoamMetricsImpl extends EndpointMetricsImpl implements ChoamMetrics {

    private final RbcMetrics      combineMetrics;
    private final Meter           cancelledTransactions;
    private final Meter           completedTransactions;
    private final Counter         droppedReassemblies;
    private final Counter         droppedTransactions;
    private final Counter         droppedValidations;
    private final Meter           failedTransactions;
    private final EtherealMetrics genesisMetrics;
    private final EtherealMetrics producerMetrics;
    private final Histogram       publishedBytes;
    private final Meter           publishedReassemblies;
    private final Meter           publishedTransactions;
    private final Meter           publishedValidations;
    private final MetricRegistry  registry;
    private final Timer           transactionLatency;
    private final Meter           transactionSubmitFailed;
    private final Meter           transactionSubmitRetry;
    private final Meter           transactionSubmitSuccess;
    private final Meter           transactionSubmittedBufferFull;
    private final Meter           transactionSubmittedInvalidCommittee;
    private final Meter           transactionTimeout;
    private final Meter           transactionSubmittedUnavailable;
    private final Meter           transactionSubmissionError;
    private final Meter           transactionSubmittedInvalidResult;
    private final Meter           transactionSubmitRetriesExhausted;
    private final  Meter transactionSubmitRateLimited;
    private final Meter transactionCancelled;

    public ChoamMetricsImpl(Digest context, MetricRegistry registry) {
        super(registry);
        this.registry = registry;
        combineMetrics = new RbcMetricsImpl(context, "combine", registry);
        producerMetrics = new EtherealMetricsImpl(context, "producer", registry);
        genesisMetrics = new EtherealMetricsImpl(context, "genesis", registry);

        droppedTransactions = registry.counter(name(context.shortString(), "transactions.dropped"));
        droppedReassemblies = registry.counter(name(context.shortString(), "reassemblies.dropped"));
        droppedValidations = registry.counter(name(context.shortString(), "validations.dropped"));

        cancelledTransactions = registry.meter(name(context.shortString(), "transactions.cancelled"));
        publishedTransactions = registry.meter(name(context.shortString(), "transactions.published"));
        publishedBytes = registry.histogram(name(context.shortString(), "unit.bytes"));
        publishedReassemblies = registry.meter(name(context.shortString(), "reassemblies.published"));
        publishedValidations = registry.meter(name(context.shortString(), "validations.published"));
        transactionLatency = registry.timer(name(context.shortString(), "transaction.latency"));
        transactionSubmitRetry = registry.meter(name(context.shortString(), "transaction.submit.retry"));
        transactionSubmitFailed = registry.meter(name(context.shortString(), "transaction.submit.failed"));
        transactionSubmitSuccess = registry.meter(name(context.shortString(), "transaction.submit.success"));
        transactionTimeout = registry.meter(name(context.shortString(), "transaction.timeout"));
        completedTransactions = registry.meter(name(context.shortString(), "transactions.completed"));
        failedTransactions = registry.meter(name(context.shortString(), "transactions.failed"));
        transactionSubmittedBufferFull = registry.meter(name(context.shortString(), "transaction.submit.buffer.full"));
        transactionSubmittedInvalidCommittee = registry.meter(
        name(context.shortString(), "transaction.submit.invalid.committee"));
        transactionSubmittedUnavailable = registry.meter(name(context.shortString(), "transaction.submit.unavailable"));
        transactionSubmissionError = registry.meter(name(context.shortString(), "transaction.submit.error"));
        transactionSubmittedInvalidResult = registry.meter(
        name(context.shortString(), "transaction.submit.invalid.result"));
        transactionSubmitRetriesExhausted = registry.meter(
        name(context.shortString(), "transaction.submit.retries.exhausted"));
        transactionSubmitRateLimited  = registry.meter(
        name(context.shortString(), "transaction.submit.rate.limited"));
        transactionCancelled = registry.meter(
        name(context.shortString(), "transaction.submit.cancelled"));
    }

    @Override
    public void dropped(int transactions, int validations, int reassemblies) {
        droppedTransactions.inc(transactions);
        droppedValidations.inc(validations);
        droppedReassemblies.inc(reassemblies);
    }

    @Override
    public RbcMetrics getCombineMetrics() {
        return combineMetrics;
    }

    @Override
    public EtherealMetrics getGensisMetrics() {
        return genesisMetrics;
    }

    @Override
    public com.netflix.concurrency.limits.MetricRegistry getMetricRegistry(String prefix) {
        return new LimitsRegistry(prefix, registry);
    }

    @Override
    public EtherealMetrics getProducerMetrics() {
        return producerMetrics;
    }

    @Override
    public void publishedBatch(int transactions, int byteSize, int validations, int reassemblies) {
        publishedTransactions.mark(transactions);
        publishedBytes.update(byteSize);
        publishedValidations.mark(validations);
        publishedReassemblies.mark(reassemblies);
    }

    @Override
    public void transactionComplete(Throwable t) {
        if (t != null) {
            if (t instanceof TimeoutException) {
                transactionTimeout.mark();
            } else if (t instanceof CancellationException) {
                cancelledTransactions.mark();
            } else {
                failedTransactions.mark();
            }
        } else {
            completedTransactions.mark();
        }
    }

    @Override
    public Timer transactionLatency() {
        return transactionLatency;
    }

    @Override
    public void transactionSubmitRetry() {
        transactionSubmitRetry.mark();
    }

    @Override
    public void transactionSubmittedBufferFull() {
        transactionSubmittedBufferFull.mark();
    }

    @Override
    public void transactionSubmittedFail() {
        transactionSubmitFailed.mark();
    }

    @Override
    public void transactionSubmittedSuccess() {
        transactionSubmitSuccess.mark();
    }

    @Override
    public void transactionTimeout() {
        transactionTimeout.mark();
    }

    @Override
    public void transactionSubmittedInvalidCommittee() {
        transactionSubmittedInvalidCommittee.mark();
    }

    @Override
    public void transactionSubmittedUnavailable() {
        transactionSubmittedUnavailable.mark();
    }

    @Override
    public void transactionSubmissionError() {
        transactionSubmissionError.mark();
    }

    @Override
    public void transactionSubmittedInvalidResult() {
        transactionSubmittedInvalidResult.mark();
    }

    @Override
    public void transactionSubmitRetriesExhausted() {
        transactionSubmitRetriesExhausted.mark();
    }

    @Override
    public void transactionSubmitRateLimited() {
        transactionSubmitRateLimited.mark();
    }

    @Override
    public void transactionCancelled() {
        transactionCancelled.mark();
    }
}
