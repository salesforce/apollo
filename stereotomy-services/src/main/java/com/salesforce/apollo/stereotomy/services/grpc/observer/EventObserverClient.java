/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.observer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.AttachmentEvents;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.AttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventObserverGrpc;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventObserverGrpc.EventObserverFutureStub;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventsContext;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;

/**
 * @author hal.hildebrand
 *
 */
public class EventObserverClient implements EventObserverService {

    public static CreateClientCommunications<EventObserverService> getCreate(Digest context,
                                                                             StereotomyMetrics metrics) {
        return (t, f, c) -> {
            return new EventObserverClient(context, c, t, metrics);
        };

    }

    public static EventObserverService getLocalLoopback() {
        return null;
    }

    private final ManagedServerConnection channel;
    private final EventObserverFutureStub client;
    private final Digeste                 context;
    private final Member                  member;
    private final StereotomyMetrics       metrics;

    public EventObserverClient(Digest context, ManagedServerConnection channel, Member member,
                               StereotomyMetrics metrics) {
        this.context = context.toDigeste();
        this.member = member;
        this.channel = channel;
        this.client = EventObserverGrpc.newFutureStub(channel.channel).withCompression("gzip");
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
    public CompletableFuture<List<AttachmentEvent>> publish(KERL_ kerl) {
        Context timer = metrics == null ? null : metrics.publishKERLClient().time();
        var request = KERLContext.newBuilder().setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundPublishKERLRequest().mark(request.getSerializedSize());
        }
        var result = client.publish(request);
        var f = new CompletableFuture<List<AttachmentEvent>>();
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            AttachmentEvents attachments;
            try {
                attachments = result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            if (timer != null) {
                metrics.inboundBandwidth().mark(attachments.getSerializedSize());
                metrics.inboundPublishKERLResponse().mark(attachments.getSerializedSize());
            }
            f.complete(attachments.getAttachmentsList());
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments) {
        Context timer = metrics == null ? null : metrics.publishAttachmentsClient().time();
        var request = AttachmentsContext.newBuilder().setContext(context).addAllAttachments(attachments).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundPublishAttachmentsRequest().mark(request.getSerializedSize());
        }
        var result = client.publishAttachments(request);
        var f = new CompletableFuture<Void>();
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            try {
                result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            f.complete(null);
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<List<AttachmentEvent>> publishEvents(List<KeyEvent_> events) {
        Context timer = metrics == null ? null : metrics.publishEventsClient().time();
        KeyEventsContext request = KeyEventsContext.newBuilder().addAllKeyEvent(events).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundPublishEventsRequest().mark(request.getSerializedSize());
        }
        var result = client.publishEvents(request);
        var f = new CompletableFuture<List<AttachmentEvent>>();
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            AttachmentEvents attachments;
            try {
                attachments = result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            if (timer != null) {
                metrics.inboundBandwidth().mark(attachments.getSerializedSize());
                metrics.inboundPublishEventsResponse().mark(attachments.getSerializedSize());
            }
            f.complete(attachments.getAttachmentsList());
        }, r -> r.run());
        return f;
    }
}
