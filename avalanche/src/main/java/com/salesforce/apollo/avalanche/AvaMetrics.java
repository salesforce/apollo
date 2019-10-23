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
    private final AtomicInteger finalizerBacklog = new AtomicInteger();
    private final Meter finalizerRate;
    private final Timer finalizeTimer;
    private final AtomicInteger inputBacklog = new AtomicInteger();
    private final Meter inputRate;
    private final Timer inputTimer;
    private final Timer noOpTimer;
    private final Meter parentSampleRate;
    private final Timer parentSampleTimer;
    private final AtomicInteger preferBacklog = new AtomicInteger();
    private final Meter preferRate;
    private final Timer preferTimer;
    private final Meter queryRate;
    private final Timer queryTimer;
    private final Meter submissionRate;
    private final Timer submissionTimer;

    public AvaMetrics(MetricRegistry registry) {
        submissionTimer = registry.timer("Txn submission duration");
        submissionRate = registry.meter("Txn submission rate");

        inputTimer = registry.timer("Input batch duration");
        inputRate = registry.meter("Input rate");
        registry.gauge("Input backlog", () -> new Gauge<>() {
            @Override
            public Object getValue() {
                return inputBacklog.get();
            }
        });

        preferTimer = registry.timer("Prefer batch duration");
        preferRate = registry.meter("Prefer rate");
        registry.gauge("Prefer backlog", () -> new Gauge<>() {
            @Override
            public Object getValue() {
                return preferBacklog.get();
            }
        });

        finalizeTimer = registry.timer("Finalize batch duration");
        finalizerRate = registry.meter("Finalize rate");
        registry.gauge("Finalize backlog", () -> new Gauge<>() {
            @Override
            public Object getValue() {
                return finalizerBacklog.get();
            }
        });

        queryTimer = registry.timer("Query batch duration");
        queryRate = registry.meter("Query rate");

        noOpTimer = registry.timer("NoOp txn generation duration");

        parentSampleTimer = registry.timer("Parent sample duration");
        parentSampleRate = registry.meter("Parent sample rate");
    }

    public AtomicInteger getFinalizerBacklog() {
        return finalizerBacklog;
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

    public AtomicInteger getInputBacklog() {
        return inputBacklog;
    }

    /**
     * @return the inputRate
     */

    public Meter getInputRate() {
        return inputRate;
    }

    /**
     * @return the inputTimer
     */

    public Timer getInputTimer() {
        return inputTimer;
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

    public AtomicInteger getPreferBacklog() {
        return preferBacklog;
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

}
