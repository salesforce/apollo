/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.observer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.AttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventObserverGrpc;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventObserverGrpc.EventObserverFutureStub;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventsContext;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;

/**
 * @author hal.hildebrand
 *
 */
public class EventObserverClient implements EventObserverService {

    public static CreateClientCommunications<EventObserverService> getCreate(Digest context,
                                                                             StereotomyMetrics metrics) {
        return (c) -> {
            return new EventObserverClient(context, c, metrics);
        };

    }

    public static EventObserverService getLocalLoopback(ProtoEventObserver service, Member member) {
        return new EventObserverService() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public CompletableFuture<Void> publish(KERL_ kerl, List<Validations> validations) {
                return service.publish(kerl, validations);
            }

            @Override
            public CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments) {
                return service.publishAttachments(attachments);
            }

            @Override
            public CompletableFuture<Void> publishEvents(List<KeyEvent_> events, List<Validations> validations) {
                return service.publishEvents(events, validations);
            }
        };
    }

    private final ManagedServerChannel    channel;
    private final EventObserverFutureStub client;
    private final Digeste                 context;
    private final StereotomyMetrics       metrics;

    public EventObserverClient(Digest context, ManagedServerChannel channel, StereotomyMetrics metrics) {
        this.context = context.toDigeste();
        this.channel = channel;
        this.client = EventObserverGrpc.newFutureStub(channel).withCompression("gzip");
        this.metrics = metrics;
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
    public CompletableFuture<Void> publish(KERL_ kerl, List<Validations> validations) {
        Context timer = metrics == null ? null : metrics.publishKERLClient().time();
        var request = KERLContext.newBuilder().setContext(context).setKerl(kerl).addAllValidations(validations).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundPublishKERLRequest().mark(request.getSerializedSize());
        }
        var result = client.publish(request);
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
    public CompletableFuture<Void> publishEvents(List<KeyEvent_> events, List<Validations> validations) {
        Context timer = metrics == null ? null : metrics.publishEventsClient().time();
        KeyEventsContext request = KeyEventsContext.newBuilder()
                                                   .addAllKeyEvent(events)
                                                   .addAllValidations(validations)
                                                   .setContext(context)
                                                   .build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundPublishEventsRequest().mark(request.getSerializedSize());
        }
        var result = client.publishEvents(request);
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
}
