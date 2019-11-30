/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * @author hhildebrand
 */
public class AvaMetrics {
    private final Meter          failedTxnQueryRate;
    private final Meter          finalizerRate;
    private final Timer          finalizeTimer;
    private final Meter          inboundQueryRate;
    private final Timer          inboundQueryTimer;
    private final Meter          inboundQueryUnknownRate;
    private final Meter          inputRate;
    private final Meter          preferRate;
    private final Timer          preferTimer;
    private final Meter          purgedNoOps;
    private final Meter          queryRate;
    private final Timer          queryTimer;
    private final MetricRegistry registry;
    private final Meter          resampledRate;
    private final Meter          satisfiedRate;
    private final Meter          submissionRate;
    private final Timer          submissionTimer;
    private final AtomicInteger  unknown = new AtomicInteger();
    private final Meter          unknownLinkRate;
    private final Meter          unknownReplacementRate;
    private final Meter          wantedRate;
    private Meter                noOpGeneration;

    public AvaMetrics(MetricRegistry r) {
        registry = r;
        submissionTimer = registry.timer("Txn submission duration");
        submissionRate = registry.meter("Txn submission rate");

        inputRate = registry.meter("Input rate");

        preferTimer = registry.timer("Prefer batch duration");
        preferRate = registry.meter("Prefer rate");

        finalizeTimer = registry.timer("Finalize batch duration");
        finalizerRate = registry.meter("Finalize rate");

        queryTimer = registry.timer("Query batch duration");
        queryRate = registry.meter("Query rate");

        inboundQueryTimer = registry.timer("Inbound query batch duration");
        inboundQueryRate = registry.meter("Inbound query rate");
        inboundQueryUnknownRate = registry.meter("Inbound query unknown rate");

        noOpGeneration = registry.meter("NoOp txn rate");

        unknownReplacementRate = registry.meter("Unknown replacement rate");

        unknownLinkRate = registry.meter("Unknown link rate");

        wantedRate = registry.meter("Wanted rate");

        satisfiedRate = registry.meter("Satisfied rate");

        failedTxnQueryRate = registry.meter("Failed txn query rate");

        resampledRate = registry.meter("Resampled rate");

        purgedNoOps = registry.meter("Purged NoOps rate");

        registry.gauge("Unknown", () -> new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return unknown.get();
            }
        });
    }

    public Meter getFailedTxnQueryRate() {
        return failedTxnQueryRate;
    }

    public Meter getFinalizerRate() {
        return finalizerRate;
    }

    public Timer getFinalizeTimer() {
        return finalizeTimer;
    }

    public Meter getInboundQueryRate() {
        return inboundQueryRate;
    }

    public Timer getInboundQueryTimer() {
        return inboundQueryTimer;
    }

    public Meter getInboundQueryUnknownRate() {
        return inboundQueryUnknownRate;
    }

    public Meter getInputRate() {
        return inputRate;
    }

    public Meter getNoOpGenerationRate() {
        return noOpGeneration;
    }

    public Meter getPreferRate() {
        return preferRate;
    }

    public Timer getPreferTimer() {
        return preferTimer;
    }

    public Meter getQueryRate() {
        return queryRate;
    }

    public Timer getQueryTimer() {
        return queryTimer;
    }

    public Meter getResampledRate() {
        return resampledRate;
    }

    public Meter getSatisfiedRate() {
        return satisfiedRate;
    }

    public Meter getSubmissionRate() {
        return submissionRate;
    }

    public Timer getSubmissionTimer() {
        return submissionTimer;
    }

    public AtomicInteger getUnknown() {
        return unknown;
    }

    public Meter getUnknownLinkRate() {
        return unknownLinkRate;
    }

    public Meter getUnknownReplacementRate() {
        return unknownReplacementRate;
    }

    public Meter getWantedRate() {
        return wantedRate;
    }

    public Meter purgeNoOps() {
        return purgedNoOps;
    }

}
