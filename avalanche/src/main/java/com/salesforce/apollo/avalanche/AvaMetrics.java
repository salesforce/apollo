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
    private final Meter         failedTxnQueryRate;
    private final Meter         finalizerRate;
    private final Timer         finalizeTimer;
    private final Meter         inboundQueryRate;
    private final Timer         inboundQueryTimer;
    private final Meter         inboundQueryUnknownRate;
    private final Meter         inputRate;
    private final Timer         noOpTimer;
    private final Meter         parentSampleRate;
    private final Timer         parentSampleTimer;
    private final Meter         preferRate;
    private final Timer         preferTimer;
    private final Meter         purgedNoOps;
    private final Meter         queryRate;
    private final Timer         queryTimer;
    private final Meter         resampledRate;
    private final Meter         satisfiedRate;
    private final Meter         submissionRate;
    private final Timer         submissionTimer;
    private final AtomicInteger unknown = new AtomicInteger();
    private final Meter         unknownLinkRate;
    private final Meter         unknownReplacementRate;
    private final Meter         wantedRate;

    public AvaMetrics(MetricRegistry registry) {
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

        noOpTimer = registry.timer("NoOp txn generation duration");

        parentSampleTimer = registry.timer("Parent sample duration");
        parentSampleRate = registry.meter("Parent sample rate");

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

    /**
     * @return the finalizerRate
     */

    public Meter getFinalizerRate() {
        return finalizerRate;
    }

    /**
     * @return the finalizeTimer
     */

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

    /**
     * @return the inputRate
     */

    public Meter getInputRate() {
        return inputRate;
    }

    /**
     * @return the noOpTimer
     */

    public Timer getNoOpTimer() {
        return noOpTimer;
    }

    /**
     * @return the parentSampleRate
     */
    public Meter getParentSampleRate() {
        return parentSampleRate;
    }

    /**
     * @return the parentSampleTimer
     */
    public Timer getParentSampleTimer() {
        return parentSampleTimer;
    }

    /**
     * @return the preferRate
     */

    public Meter getPreferRate() {
        return preferRate;
    }

    /**
     * @return the preferTimer
     */

    public Timer getPreferTimer() {
        return preferTimer;
    }

    /**
     * @return the queryRate
     */

    public Meter getQueryRate() {
        return queryRate;
    }

    /**
     * @return the queryTimer
     */

    public Timer getQueryTimer() {
        return queryTimer;
    }

    public Meter getResampledRate() {
        return resampledRate;
    }

    public Meter getSatisfiedRate() {
        return satisfiedRate;
    }

    /**
     * @return the submissionRate
     */

    public Meter getSubmissionRate() {
        return submissionRate;
    }

    /**
     * @return the submissionTimer
     */

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
