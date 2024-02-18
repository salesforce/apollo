/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.resolver;

import com.codahale.metrics.Timer.Context;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.event.proto.Binding;
import com.salesforce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.grpc.proto.ResolverGrpc;
import com.salesforce.apollo.stereotomy.services.grpc.proto.ResolverGrpc.ResolverBlockingStub;

import java.util.Optional;

/**
 * @author hal.hildebrand
 */
public class ResolverClient implements ResolverService {

    private final ManagedServerChannel channel;
    private final ResolverBlockingStub client;
    private final StereotomyMetrics    metrics;

    public ResolverClient(ManagedServerChannel channel, StereotomyMetrics metrics) {
        this.channel = channel;
        this.client = channel.wrap(ResolverGrpc.newBlockingStub(channel));
        this.metrics = metrics;
    }

    public static CreateClientCommunications<ResolverService> getCreate(StereotomyMetrics metrics) {
        return (c) -> {
            return new ResolverClient(c, metrics);
        };

    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    @Override
    public Optional<Binding> lookup(Ident prefix) {
        Context timer = metrics == null ? null : metrics.lookupClient().time();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(prefix.getSerializedSize());
            metrics.outboundLookupRequest().mark(prefix.getSerializedSize());
        }
        var result = client.lookup(prefix);
        var serializedSize = result.getSerializedSize();
        if (timer != null) {
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundLookupResponse().mark(serializedSize);
        }
        return Optional.ofNullable(result);
    }
}
