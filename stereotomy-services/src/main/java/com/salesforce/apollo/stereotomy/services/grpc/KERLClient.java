/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.stereotomy.event.proto.KERL;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc.KERLServiceBlockingStub;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventContext;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.KERLProvider;
import com.salesforce.apollo.stereotomy.services.KERLRecorder;

/**
 * @author hal.hildebrand
 *
 */
public class KERLClient implements KERLProvider, KERLRecorder, Link {

    public static CreateClientCommunications<KERLClient> getCreate(Digest context, StereotomyMetrics metrics) {
        return (t, f, c) -> {
            return new KERLClient(context, c, t, metrics);
        };

    }

    private final ManagedServerConnection channel;
    private final KERLServiceBlockingStub client;
    private final Member                  member;
    private final StereotomyMetrics       metrics;
    private final Digeste                 context;

    public KERLClient(Digest context, ManagedServerConnection channel, Member member, StereotomyMetrics metrics) {
        this.context = context.toDigeste();
        this.member = member;
        this.channel = channel;
        this.client = KERLServiceGrpc.newBlockingStub(channel.channel).withCompression("gzip");
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

    @Override
    public void append(EventWithAttachments ewa) {
        Context timer = metrics == null ? null : metrics.appendClient().time();
        com.salesfoce.apollo.stereotomy.event.proto.KeyEvent keyEvent = null;
        var request = KeyEventContext.newBuilder().setKeyEvent(keyEvent).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboudAppendRequest().mark(request.getSerializedSize());
        }
        client.append(request);
        if (timer != null) {
            timer.stop();
        }
    }

    @Override
    public void publish(List<EventWithAttachments> kerl) throws TimeoutException {
        // TODO Auto-generated method stub

    }

}
