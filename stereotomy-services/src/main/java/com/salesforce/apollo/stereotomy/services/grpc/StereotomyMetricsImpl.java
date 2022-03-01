/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.BandwidthMetricsImpl;

/**
 * @author hal.hildebrand
 *
 */
public class StereotomyMetricsImpl extends BandwidthMetricsImpl implements StereotomyMetrics {

    private final Meter inboundKerlRequest;
    private final Meter inboundKerlResponse;
    private final Meter inboundLookupRequest;
    private final Meter inboundLookupResponse;
    private final Meter inboundResolveCoodsRequest;
    private final Meter inboundResolveCoordsResponse;
    private final Meter inboundResolveRequest;
    private final Timer kerlClient;
    private final Timer kerlService;
    private final Meter inboundResolveResponse;
    private final Timer lookupClient;
    private final Timer lookupService;
    private final Meter outboundKerlRequest;
    private final Meter outboundKerlResponse;
    private final Meter outboundLookupRequest;
    private final Meter outboundLookupResponse;
    private final Meter outboundResolveCoordsRequest;
    private final Meter outboundResolveCoordsResponse;
    private final Meter outboundResolveRequest;
    private final Meter outboundResolveResponse;
    private final Timer resolveClient;
    private final Timer resolveCoordsClient;
    private final Timer resolveCoordsService;
    private final Timer resolveService;

    /**
     * @param registry
     */
    public StereotomyMetricsImpl(Digest context, MetricRegistry registry) {
        super(registry);
        this.inboundKerlRequest = registry.meter(name(context.shortString(), "inbound.kerl.request"));
        this.inboundKerlResponse = registry.meter(name(context.shortString(), "inbound.kerl.response"));
        this.inboundLookupRequest = registry.meter(name(context.shortString(), "inbound.lookup.request"));
        this.inboundLookupResponse = registry.meter(name(context.shortString(), "inbound.lookup.response"));
        this.inboundResolveCoodsRequest = registry.meter(name(context.shortString(), "inbound.resolve.coords.request"));
        this.inboundResolveCoordsResponse = registry.meter(name(context.shortString(),
                                                                "inbound.resolve.coords.response"));
        this.inboundResolveRequest = registry.meter(name(context.shortString(), "inbound.resolve.response"));
        this.kerlClient = registry.timer(name(context.shortString(), "kerl.client.duration"));
        this.kerlService = registry.timer(name(context.shortString(), "kerl.service.duration"));
        this.inboundResolveResponse = new Meter();
        this.lookupClient = registry.timer(name(context.shortString(), "lookup.client.duration"));
        this.lookupService = registry.timer(name(context.shortString(), "lookup.service.duration"));
        this.outboundKerlRequest = registry.meter(name(context.shortString(), "outbound.kerl.request"));
        this.outboundKerlResponse = registry.meter(name(context.shortString(), "outbound.kerl.response"));
        this.outboundLookupRequest = registry.meter(name(context.shortString(), "outbound.lookup.request"));
        this.outboundLookupResponse = registry.meter(name(context.shortString(), "outbound.lookup.response"));
        this.outboundResolveCoordsRequest = registry.meter(name(context.shortString(),
                                                                "outbound.resolve.coords.request"));
        this.outboundResolveCoordsResponse = registry.meter(name(context.shortString(),
                                                                 "outbound.resolve.coords.response"));
        this.outboundResolveRequest = registry.meter(name(context.shortString(), "outbound.resolve.request"));
        this.outboundResolveResponse = registry.meter(name(context.shortString(), "outbound.resolve.response"));
        this.resolveClient = registry.timer(name(context.shortString(), "resolve.client.duration"));
        this.resolveCoordsClient = registry.timer(name(context.shortString(), "resolve.coords.client.duration"));
        this.resolveCoordsService = registry.timer(name(context.shortString(), "resolve.coords.service.duration"));
        this.resolveService = registry.timer(name(context.shortString(), "resolve.service.duration"));
    }

    @Override
    public Meter inboundKerlRequest() {
        return inboundKerlRequest;
    }

    @Override
    public Meter inboundKerlResponse() {
        return inboundKerlResponse;
    }

    @Override
    public Meter inboundLookupRequest() {
        return inboundLookupRequest;
    }

    @Override
    public Meter inboundLookupResponse() {
        return inboundLookupResponse;
    }

    @Override
    public Meter inboundResolveCoodsRequest() {
        return inboundResolveCoodsRequest;
    }

    @Override
    public Meter inboundResolveCoordsResponse() {
        return inboundResolveCoordsResponse;
    }

    @Override
    public Meter inboundResolveRequest() {
        return inboundResolveRequest;
    }

    @Override
    public Meter inboundResolveResponse() {
        return inboundResolveResponse;
    }

    @Override
    public Timer kerlClient() {
        return kerlClient;
    }

    @Override
    public Timer kerlService() {
        return kerlService;
    }

    @Override
    public Timer lookupClient() {
        return lookupClient;
    }

    @Override
    public Timer lookupService() {
        return lookupService;
    }

    @Override
    public Meter outboundKerlRequest() {
        return outboundKerlRequest;
    }

    @Override
    public Meter outboundKerlResponse() {
        return outboundKerlResponse;
    }

    @Override
    public Meter outboundLookupRequest() {
        return outboundLookupRequest;
    }

    @Override
    public Meter outboundLookupResponse() {
        return outboundLookupResponse;
    }

    @Override
    public Meter outboundResolveCoordsRequest() {
        return outboundResolveCoordsRequest;
    }

    @Override
    public Meter outboundResolveCoordsResponse() {
        return outboundResolveCoordsResponse;
    }

    @Override
    public Meter outboundResolveRequest() {
        return outboundResolveRequest;
    }

    @Override
    public Meter outboundResolveResponse() {
        return outboundResolveResponse;
    }

    @Override
    public Timer resolveClient() {
        return resolveClient;
    }

    @Override
    public Timer resolveCoordsClient() {
        return resolveCoordsClient;
    }

    @Override
    public Timer resolveCoordsService() {
        return resolveCoordsService;
    }

    @Override
    public Timer resolveService() {
        return resolveService;
    }
}
