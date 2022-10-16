/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import com.codahale.metrics.Meter;

/**
 * @author hal.hildebrand
 *
 */
public interface EndpointMetrics {

    String INBOUND_BANDWIDTH  = "bandwidth.inbound";
    String OUTBOUND_BANDWIDTH = "bandwidth.outbound";

    Meter inboundBandwidth();

    LimitsRegistry limitsMetrics();

    Meter outboundBandwidth();
}
