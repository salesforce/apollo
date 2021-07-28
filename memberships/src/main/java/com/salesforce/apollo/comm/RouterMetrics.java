/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionCacheMetrics;
import com.salesforce.apollo.protocols.BandwidthMetrics;

/**
 * @author hal.hildebrand
 *
 */
public interface RouterMetrics extends BandwidthMetrics, ServerConnectionCacheMetrics, GossipMetrics {

}
