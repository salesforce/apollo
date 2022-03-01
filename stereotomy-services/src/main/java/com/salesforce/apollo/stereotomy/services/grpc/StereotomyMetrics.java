/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.BandwidthMetrics;

/**
 * @author hal.hildebrand
 *
 */
public interface StereotomyMetrics extends BandwidthMetrics {

    Timer kerlClient();

    Timer kerlService();

    Meter inboundKerlRequest();

    Meter inboundKerlResponse();

    Meter inboundLookupRequest();

    Meter inboundLookupResponse();

    Meter inboundResolveCoodsRequest();

    Meter inboundResolveCoordsResponse();

    Meter inboundResolveRequest();

    Meter inboundResolveResponse();

    Timer lookupClient();

    Timer lookupService();

    Meter outboundKerlRequest();

    Meter outboundKerlResponse();

    Meter outboundLookupRequest();

    Meter outboundLookupResponse();

    Meter outboundResolveCoordsRequest();

    Meter outboundResolveCoordsResponse();

    Meter outboundResolveRequest();

    Meter outboundResolveResponse();

    Timer resolveCoordsClient();

    Timer resolveCoordsService();

    Timer resolveClient();

    Timer resolveService();

}
