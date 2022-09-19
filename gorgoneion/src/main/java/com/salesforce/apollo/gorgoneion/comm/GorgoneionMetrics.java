/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.EdpointMetrics;

/**
 * @author hal.hildebrand
 *
 */
public interface GorgoneionMetrics extends EdpointMetrics {

    Timer enrollDuration();

    Histogram inboundApplication();

    Histogram inboundCredentials();

    Histogram inboundEnroll();

    Histogram inboundInvitation();

    Histogram outboundApplication();

    Histogram outboundCredentials();

    Histogram outboundNotarization();

    Timer registerDuration();

}
