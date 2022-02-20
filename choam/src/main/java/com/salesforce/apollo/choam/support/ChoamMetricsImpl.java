/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import java.util.concurrent.TimeoutException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.ethereal.memberships.EtherealMetrics;
import com.salesforce.apollo.ethereal.memberships.EtherealMetricsImpl;
import com.salesforce.apollo.membership.messaging.rbc.RbcMetrics;
import com.salesforce.apollo.membership.messaging.rbc.RbcMetricsImpl;
import com.salesforce.apollo.protocols.BandwidthMetricsImpl;

/**
 * @author hal.hildebrand
 *
 */
public class ChoamMetricsImpl extends BandwidthMetricsImpl implements ChoamMetrics {

    private final RbcMetrics      combineMetrics;
    private final Counter         completedTransactions;
    private final Counter         droppedTransactions;
    private final Counter         droppedValidations;
    private final Counter         failedTransactions;
    private final EtherealMetrics producerMetrics;
    private final Histogram       publishedBytes;
    private final Meter           publishedTransactions;
    private final Meter           publishedValidations;
    private final EtherealMetrics reconfigureMetrics;
    private final Timer           transactionLatency;
    private final Counter         transactionSubmitFailed;
    private final Counter         transactionSubmitRetry;
    private final Counter         transactionSubmitSuccess;
    private final Counter         transactionTimeout;

    public ChoamMetricsImpl(MetricRegistry registry) {
        super(registry);
        combineMetrics = new RbcMetricsImpl(registry);
        producerMetrics = new EtherealMetricsImpl(registry);
        reconfigureMetrics = new EtherealMetricsImpl(registry);

        droppedTransactions = registry.counter("transactions.dropped");
        droppedValidations = registry.counter("validations.dropped");
        publishedTransactions = registry.meter("transactions.published");
        publishedBytes = registry.histogram("unit.bytes");
        publishedValidations = registry.meter("validations.published");
        transactionLatency = registry.timer("transaction.latency");
        transactionSubmitRetry = registry.counter("transaction.submit.retry");
        transactionSubmitFailed = registry.counter("transaction.submit.failed");
        transactionSubmitSuccess = registry.counter("transaction.submit.success");
        transactionTimeout = registry.counter("transaction.timeout");
        completedTransactions = registry.counter("transactions.completed");
        failedTransactions = registry.counter("transactions.failed");
    }

    @Override
    public void dropped(int transactions, int validations) {
        droppedTransactions.inc(transactions);
        droppedValidations.inc(validations);
    }

    @Override
    public RbcMetrics getCombineMetrics() {
        return combineMetrics;
    }

    @Override
    public EtherealMetrics getProducerMetrics() {
        return producerMetrics;
    }

    @Override
    public EtherealMetrics getReconfigureMetrics() {
        return reconfigureMetrics;
    }

    @Override
    public void publishedBatch(int transactions, int byteSize, int validations) {
        publishedTransactions.mark(transactions);
        publishedBytes.update(byteSize);
        publishedValidations.mark(validations);
    }

    @Override
    public void transactionComplete(Throwable t) {
        if (t != null) {
            if (t instanceof TimeoutException) {
                transactionTimeout.inc();
            } else {
                failedTransactions.inc();
            }
        } else {
            completedTransactions.inc();
        }
    }

    @Override
    public Timer transactionLatency() {
        return transactionLatency;
    }

    @Override
    public void transactionSubmitRetry() {
        transactionSubmitRetry.inc();
    }

    @Override
    public void transactionSubmittedFail() {
        transactionSubmitFailed.inc();
    }

    @Override
    public void transactionSubmittedSuccess() {
        transactionSubmitSuccess.inc();
    }

    @Override
    public void transactionTimeout() {
        transactionTimeout.inc();
    }

}
