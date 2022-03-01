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

    private final Timer appendClient;
    private final Timer appendService;
    private final Timer bindClient;
    private final Timer bindService;
    private final Meter inboundAppendRequest;
    private final Meter inboundBindRequest;
    private final Meter inboundKerlRequest;
    private final Meter inboundKerlResponse;
    private final Meter inboundLookupRequest;
    private final Meter inboundLookupResponse;
    private final Meter inboundPublishRequest;
    private final Meter inboundResolveCoodsRequest;
    private final Meter inboundResolveCoordsResponse;
    private final Meter inboundResolveRequest;
    private final Meter inboundResolveResponse;
    private final Meter inboundUnbindRequest;
    private final Timer kerlClient;
    private final Timer kerlService;
    private final Timer lookupClient;
    private final Timer lookupService;
    private final Meter outboudAppendRequest;
    private final Meter outboudUnbindRequest;
    private final Meter outboundBindRequest;
    private final Meter outboundKerlRequest;
    private final Meter outboundKerlResponse;
    private final Meter outboundLookupRequest;
    private final Meter outboundLookupResponse;
    private final Meter outboundResolveCoordsRequest;
    private final Meter outboundResolveCoordsResponse;
    private final Meter outboundResolveRequest;
    private final Meter outboundResolveResponse;
    private final Timer publishService;
    private final Timer resolveClient;
    private final Timer resolveCoordsClient;
    private final Timer resolveCoordsService;
    private final Timer resolveService;
    private final Timer unbindClient;
    private final Timer unbindService;

    /**
     * @param registry
     */
    public StereotomyMetricsImpl(Digest context, MetricRegistry registry) {
        super(registry);
        this.appendClient = new Timer();
        this.bindClient = new Timer();
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
        this.inboundResolveResponse = registry.meter(name(context.shortString(), "inbound.resolve.respone"));
        this.lookupClient = registry.timer(name(context.shortString(), "lookup.client.duration"));
        this.lookupService = registry.timer(name(context.shortString(), "lookup.service.duration"));
        this.outboudAppendRequest = registry.meter(name(context.shortString(), "outbound.append.request"));
        this.outboudUnbindRequest = registry.meter(name(context.shortString(), "outbound.unbind.request"));
        this.outboundBindRequest = registry.meter(name(context.shortString(), "outbound.bind.request"));
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
        this.appendService = registry.timer(name(context.shortString(), "append.service.duration"));
        this.bindService = registry.timer(name(context.shortString(), "bind.service.duration"));
        this.inboundAppendRequest = registry.meter(name(context.shortString(), "inbound.append.request"));
        this.inboundBindRequest = registry.meter(name(context.shortString(), "inbound.bind.request"));
        this.inboundPublishRequest = registry.meter(name(context.shortString(), "inbound.publish.request"));
        this.inboundUnbindRequest = registry.meter(name(context.shortString(), "inbound.unbind.request"));
        this.publishService = registry.timer(name(context.shortString(), "publish.service.duration"));
        this.unbindClient = registry.timer(name(context.shortString(), "unbind.client.duration"));
        this.unbindService = registry.timer(name(context.shortString(), "unbind.service.duration"));
    }

    @Override
    public Timer appendClient() {
        return appendClient;
    }

    @Override
    public Timer appendService() {
        return appendService;
    }

    @Override
    public Timer bindClient() {
        return bindClient;
    }

    @Override
    public Timer bindService() {
        return bindService;
    }

    @Override
    public Meter inboundAppendRequest() {
        return inboundAppendRequest;
    }

    @Override
    public Meter inboundBindRequest() {
        return inboundBindRequest;
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
    public Meter inboundPublishRequest() {
        return inboundPublishRequest;
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
    public Meter inboundUnbindRequest() {
        return inboundUnbindRequest;
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
    public Meter outboudAppendRequest() {
        return outboudAppendRequest;
    }

    @Override
    public Meter outboudUnbindRequest() {
        return outboudUnbindRequest;
    }

    @Override
    public Meter outboundBindRequest() {
        return outboundBindRequest;
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
    public Timer publishService() {
        return publishService;
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

    @Override
    public Timer unbindClient() {
        return unbindClient;
    }

    @Override
    public Timer unbindService() {
        return unbindService;
    }
}
