/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.stereotomy.event.proto.KERL;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.ResolverGrpc;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.ResolverGrpc.ResolverBlockingStub;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.protobuf.InceptionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.ResolverService;

/**
 * @author hal.hildebrand
 *
 */
public class ResolverClient implements ResolverService, Link {

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
    public Optional<List<EventWithAttachments>> kerl(Identifier prefix) throws TimeoutException {
        Context timer = metrics == null ? null : metrics.kerlClient().time();
        IdentifierContext request = IdentifierContext.newBuilder()
                                                     .setContext(context)
                                                     .setIdentifier(prefix.toIdent())
                                                     .build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundKerlRequest().mark(request.getSerializedSize());
        }
        KERL result = client.kerl(request);
        var serializedSize = result.getSerializedSize();
        if (timer != null) {
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundKerlResponse().mark(serializedSize);
        }
        return Optional.of(result.getEventsList().stream().map(ke -> ProtobufEventFactory.from(ke)).toList());
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
        var bound = result.getValue();
        URI uri;
        try {
            uri = new URI(bound.getUri());
        } catch (URISyntaxException e) {
            return Optional.empty();
        }
        Binding binding = result.equals(com.salesfoce.apollo.stereotomy.event.proto.Binding.getDefaultInstance()) ? null
                                                                                                                  : new Binding(new Bound(new InceptionEventImpl(bound.getIdentifier()),
                                                                                                                                          uri),
                                                                                                                                JohnHancock.from(result.getSignature()));
        return Optional.of(binding);
    }

    @Override
    public Optional<KeyState> resolve(EventCoordinates coordinates) throws TimeoutException {
        Context timer = metrics == null ? null : metrics.resolveCoordsClient().time();
        var request = EventContext.newBuilder().setContext(context).setCoordinates(coordinates.toEventCoords()).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundResolveCoordsRequest().mark(request.getSerializedSize());
        }
        var result = client.resolveCoords(request);
        var serializedSize = result.getSerializedSize();
        if (timer != null) {
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundResolveCoodsRequest().mark(serializedSize);
        }
        return Optional.of(new KeyStateImpl(result));
    }

    @Override
    public Optional<KeyState> resolve(Identifier prefix) throws TimeoutException {
        Context timer = metrics == null ? null : metrics.resolveClient().time();
        var request = IdentifierContext.newBuilder().setContext(context).setIdentifier(prefix.toIdent()).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundResolveRequest().mark(request.getSerializedSize());
        }
        var result = client.resolve(request);
        var serializedSize = result.getSerializedSize();
        if (timer != null) {
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundResolveRequest().mark(serializedSize);
        }
        return Optional.of(new KeyStateImpl(result));
    }

}
