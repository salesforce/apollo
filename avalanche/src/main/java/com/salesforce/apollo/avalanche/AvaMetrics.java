/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * @author hhildebrand
 */
public class AvaMetrics implements AvalancheMetrics {
    private final Meter         failedTxnQueryRate;
    private final Meter         finalizerRate;
    private final Timer         finalizeTimer;
    private final Meter         inboundBandwidth;
    private final Histogram     inboundQuery;
    private final Meter         inboundQueryRate;
    private final Timer         inboundQueryTimer;
    private final Meter         inboundQueryUnknownRate;
    private final Histogram     inboundRequery;
    private final Histogram     inboundRequestDag;
    private final Meter         inputRate;
    private final Meter         noOpGeneration;
    private final Meter         outboundBandwidth;
    private final Histogram     outboundQuery;
    private final Histogram     outboundRequery;
    private final Histogram     outboundRequestDag;
    private final Meter         preferRate;
    private final Timer         preferTimer;
    private final Meter         purgedNoOps;
    private final Meter         queryRate;
    private final Histogram     queryReply;
    private final Histogram     queryResponse;
    private final Timer         queryTimer;
    private final Histogram     requestDagReply;
    private final Histogram     requestDagResponse;
    private final Meter         resampledRate;
    private final Meter         satisfiedRate;
    private final Meter         submissionRate;
    private final Timer         submissionTimer;
    private final AtomicInteger unknown = new AtomicInteger();
    private final Meter         unknownLinkRate;
    private final Meter         unknownReplacementRate;
    private final Meter         wantedRate;

    public AvaMetrics(MetricRegistry registry) {
        inboundBandwidth = registry.meter(INBOUND_BANDWIDTH);
        outboundBandwidth = registry.meter(OUTBOUND_BANDWIDTH);

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

        inboundQuery = registry.histogram("Inbound Query Bytes");
        inboundRequery = registry.histogram("Inbound Requery Bytes");
        inboundRequestDag = registry.histogram("Inbound Request DAG Bytes");
        outboundQuery = registry.histogram("Outbound Query Bytes");
        outboundRequery = registry.histogram("Outbound Requery Bytes");
        outboundRequestDag = registry.histogram("Outbound Request DAG Bytes");
        queryReply = registry.histogram("Outbound Query Reply Bytes");
        queryResponse = registry.histogram("Inbound Query Response Bytes");
        requestDagReply = registry.histogram("Outbound Request Dag Reply Bytes");
        requestDagResponse = registry.histogram("Inbound Request DAG Response Bytes");
    }

    @Override
    public Meter getFailedTxnQueryRate() {
        return failedTxnQueryRate;
    }

    @Override
    public Meter getFinalizerRate() {
        return finalizerRate;
    }

    @Override
    public Timer getFinalizeTimer() {
        return finalizeTimer;
    }

    @Override
    public Meter getInboundQueryRate() {
        return inboundQueryRate;
    }

    @Override
    public Timer getInboundQueryTimer() {
        return inboundQueryTimer;
    }

    @Override
    public Meter getInboundQueryUnknownRate() {
        return inboundQueryUnknownRate;
    }

    @Override
    public Meter getInputRate() {
        return inputRate;
    }

    @Override
    public Meter getNoOpGenerationRate() {
        return noOpGeneration;
    }

    @Override
    public Meter getPreferRate() {
        return preferRate;
    }

    @Override
    public Timer getPreferTimer() {
        return preferTimer;
    }

    @Override
    public Meter getQueryRate() {
        return queryRate;
    }

    @Override
    public Timer getQueryTimer() {
        return queryTimer;
    }

    @Override
    public Meter getResampledRate() {
        return resampledRate;
    }

    @Override
    public Meter getSatisfiedRate() {
        return satisfiedRate;
    }

    @Override
    public Meter getSubmissionRate() {
        return submissionRate;
    }

    @Override
    public Timer getSubmissionTimer() {
        return submissionTimer;
    }

    @Override
    public AtomicInteger getUnknown() {
        return unknown;
    }

    @Override
    public Meter getUnknownLinkRate() {
        return unknownLinkRate;
    }

    @Override
    public Meter getUnknownReplacementRate() {
        return unknownReplacementRate;
    }

    @Override
    public Meter getWantedRate() {
        return wantedRate;
    }

    @Override
    public Meter inboundBandwidth() {
        return inboundBandwidth;
    }

    @Override
    public Histogram inboundQuery() {
        return inboundQuery;
    }

    @Override
    public Histogram inboundRequery() {
        return inboundRequery;
    }

    @Override
    public Histogram inboundRequestDag() {
        return inboundRequestDag;
    }

    @Override
    public Meter outboundBandwidth() {
        return outboundBandwidth;
    }

    @Override
    public Histogram outboundQuery() {
        return outboundQuery;
    }

    @Override
    public Histogram outboundRequery() {
        return outboundRequery;
    }

    @Override
    public Histogram outboundRequestDag() {
        return outboundRequestDag;
    }

    @Override
    public Meter purgeNoOps() {
        return purgedNoOps;
    }

    @Override
    public Histogram queryReply() {
        return queryReply;
    }

    @Override
    public Histogram queryResponse() {
        return queryResponse;
    }

    @Override
    public Histogram requestDagReply() {
        return requestDagReply;
    }

    @Override
    public Histogram requestDagResponse() {
        return requestDagResponse;
    }

}
