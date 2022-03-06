/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.ResolverGrpc;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.ResolverGrpc.ResolverBlockingStub;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.Binder.Binding;
import com.salesforce.apollo.stereotomy.services.Resolver;

/**
 * @author hal.hildebrand
 *
 */
public class ResolverClient implements Resolver, Link {

    public static CreateClientCommunications<ResolverClient> getCreate(Digest context, StereotomyMetrics metrics) {
        return (t, f, c) -> {
            return new ResolverClient(context, c, t, metrics);
        };

    }

    private final ManagedServerConnection channel;
    private final ResolverBlockingStub    client;
    private final Member                  member;
    private final StereotomyMetrics       metrics;
    private final Digeste                 context;

    public ResolverClient(Digest context, ManagedServerConnection channel, Member member, StereotomyMetrics metrics) {
        this.context = context.toDigeste();
        this.member = member;
        this.channel = channel;
        this.client = ResolverGrpc.newBlockingStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public Optional<Binding> lookup(Identifier prefix) throws TimeoutException {
        Context timer = metrics == null ? null : metrics.lookupClient().time();
        IdentifierContext request = IdentifierContext.newBuilder()
                                                     .setContext(context)
                                                     .setIdentifier(prefix.toIdent())
                                                     .build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundLookupRequest().mark(request.getSerializedSize());
        }
        var result = client.lookup(request);
        var serializedSize = result.getSerializedSize();
        if (timer != null) {
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundLookupResponse().mark(serializedSize);
        }
        return Optional.ofNullable(Binding.from(result));
    }
}
