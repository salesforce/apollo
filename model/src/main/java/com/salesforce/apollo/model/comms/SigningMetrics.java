/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.comms;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.EndpointMetrics;

/**
 * @author hal.hildebrand
 *
 */
public interface SigningMetrics extends EndpointMetrics {

    Meter inboundRequest();

    Timer inboundSign();

    Meter inboundSignature();

    Meter outboundRequest();

}
