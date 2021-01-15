/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.BandwidthMetrics;

/**
 * @author hal.hildebrand
 *
 */
public interface AvalancheMetrics extends BandwidthMetrics {

    Meter getFailedTxnQueryRate();

    Meter getFinalizerRate();

    Timer getFinalizeTimer();

    Meter getInboundQueryRate();

    Timer getInboundQueryTimer();

    Meter getInboundQueryUnknownRate();

    Meter getInputRate();

    Meter getNoOpGenerationRate();

    Meter getPreferRate();

    Timer getPreferTimer();

    Meter getQueryRate();

    Timer getQueryTimer();

    Meter getResampledRate();

    Meter getSatisfiedRate();

    Meter getSubmissionRate();

    Timer getSubmissionTimer();

    AtomicInteger getUnknown();

    Meter getUnknownLinkRate();

    Meter getUnknownReplacementRate();

    Meter getWantedRate();

    @Override
    Meter inboundBandwidth();

    Histogram inboundQuery();

    Histogram inboundRequestDag();

    @Override
    Meter outboundBandwidth();

    Histogram outboundQuery();

    Histogram outboundRequestDag();

    Meter purgeNoOps();

    Histogram queryReply();

    Histogram queryResponse();

    Histogram requestDagReply();

    Histogram requestDagResponse();

    Histogram inboundRequery();

    Histogram outboundRequery();
}
