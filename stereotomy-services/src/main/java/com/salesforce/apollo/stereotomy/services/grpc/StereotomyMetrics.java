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

    Timer appendService();

    Timer bindService();

    Meter inboundAppendRequest();

    Meter inboundBindRequest();

    Meter inboundKerlRequest();

    Meter inboundKerlResponse();

    Meter inboundLookupRequest();

    Meter inboundLookupResponse();

    Meter inboundPublishRequest();

    Meter inboundResolveCoodsRequest();

    Meter inboundResolveCoordsResponse();

    Meter inboundResolveRequest();

    Meter inboundResolveResponse();

    Meter inboundUnbindRequest();

    Timer kerlClient();

    Timer kerlService();

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

    Timer publishService();

    Timer resolveClient();

    Timer resolveCoordsClient();

    Timer resolveCoordsService();

    Timer resolveService();

    Timer unbindService();

    Timer appendClient();

    Timer bindClient();

    Meter outboundBindRequest();

    Meter outboudAppendRequest();

    Timer unbindClient();

    Meter outboudUnbindRequest();

}
