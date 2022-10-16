/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.archipelago.ServerConnectionCache.ServerConnectionCacheMetrics;

/**
 * @author hal.hildebrand
 *
 */
public class ServerConnectionCacheMetricsImpl implements ServerConnectionCacheMetrics {
    private final Meter   borrowRate;
    private final Timer   channelOpenDuration;
    private final Meter   closeConnectionRate;
    private final Counter createConnection;
    private final Meter   failedConnectionRate;
    private final Counter failedOpenConnection;
    private final Counter openConnections;
    private final Meter   releaseRate;

    public ServerConnectionCacheMetricsImpl(MetricRegistry registry) {
        failedOpenConnection = registry.counter("client.connection.open.fail");
        createConnection = registry.counter("client.connection.created");
        openConnections = registry.counter("client.connection.open");
        closeConnectionRate = registry.meter("client.connection.close");
        failedConnectionRate = registry.meter("client.connection.fail");
        borrowRate = registry.meter("client.connection.borrow");
        releaseRate = registry.meter("client.connection.release");
        channelOpenDuration = registry.timer("client.connection.open.duration");
    }

    @Override
    public Meter borrowRate() {
        return borrowRate;
    }

    @Override
    public Timer channelOpenDuration() {
        return channelOpenDuration;
    }

    @Override
    public Meter closeConnectionRate() {
        return closeConnectionRate;
    }

    @Override
    public Counter createConnection() {
        return createConnection;
    }

    @Override
    public Meter failedConnectionRate() {
        return failedConnectionRate;
    }

    @Override
    public Counter failedOpenConnection() {
        return failedOpenConnection;
    }

    @Override
    public Counter openConnections() {
        return openConnections;
    }

    @Override
    public Meter releaseRate() {
        return releaseRate;
    }
}
