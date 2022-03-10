/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.AttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventDigestContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc.KERLServiceFutureStub;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventWitAttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyStates;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class KERLClient implements KERLService {

    public static CreateClientCommunications<KERLService> getCreate(Digest context, StereotomyMetrics metrics) {
        return (t, f, c) -> {
            return new KERLClient(context, c, t, metrics);
        };

    }

    public static KERLService getLocalLoopback() {
        return null;
    }

    private final ManagedServerConnection channel;
    private final KERLServiceFutureStub   client;
    private final Digeste                 context;
    private final Member                  member;
    private final StereotomyMetrics       metrics;

    public KERLClient(Digest context, ManagedServerConnection channel, Member member, StereotomyMetrics metrics) {
        this.context = context.toDigeste();
        this.member = member;
        this.channel = channel;
        this.client = KERLServiceGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(KERL_ kerl) {
        Context timer = metrics == null ? null : metrics.appendKERLClient().time();
        var request = KERLContext.newBuilder().setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundAppendKERLRequest().mark(request.getSerializedSize());
        }
        var result = client.appendKERL(request);
        var f = new CompletableFuture<List<KeyState_>>();
        result.addListener(() -> {
            KeyStates ks;
            try {
                ks = result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }

            if (timer != null) {
                timer.stop();
                metrics.inboundBandwidth().mark(ks.getSerializedSize());
                metrics.inboundAppendKERLResponse().mark(request.getSerializedSize());
            }

            if (ks.getKeyStatesCount() == 0) {
                f.complete(Collections.emptyList());
            } else {
                f.complete(ks.getKeyStatesList());
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> keyEventList) {
        Context timer = metrics == null ? null : metrics.appendEventsClient().time();
        KeyEventsContext request = KeyEventsContext.newBuilder()
                                                   .addAllKeyEvent(keyEventList)
                                                   .setContext(context)
                                                   .build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundAppendEventsRequest().mark(request.getSerializedSize());
        }
        var result = client.append(request);
        var f = new CompletableFuture<List<KeyState_>>();
        result.addListener(() -> {
            KeyStates ks;
            try {
                ks = result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            if (ks.getKeyStatesCount() == 0) {
                f.complete(Collections.emptyList());
            } else {
                f.complete(ks.getKeyStatesList());
            }
            if (timer != null) {
                timer.stop();
                metrics.inboundBandwidth().mark(ks.getSerializedSize());
                metrics.inboundAppendEventsResponse().mark(request.getSerializedSize());
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> eventsList,
                                                     List<AttachmentEvent> attachmentsList) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        var request = KeyEventWitAttachmentsContext.newBuilder()
                                                   .addAllEvents(eventsList)
                                                   .addAllAttachments(attachmentsList)
                                                   .setContext(context)
                                                   .build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundAppendWithAttachmentsRequest().mark(request.getSerializedSize());
        }
        var result = client.appendWithAttachments(request);
        var f = new CompletableFuture<List<KeyState_>>();
        result.addListener(() -> {
            KeyStates ks;
            try {
                ks = result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            if (timer != null) {
                timer.stop();
            }
            if (ks.getKeyStatesCount() == 0) {
                f.complete(Collections.emptyList());
            } else {
                f.complete(ks.getKeyStatesList());
            }
            if (timer != null) {
                metrics.inboundBandwidth().mark(ks.getSerializedSize());
                metrics.inboundAppendWithAttachmentsResponse().mark(request.getSerializedSize());
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Optional<Attachment> getAttachment(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundGetAttachmentRequest().mark(request.getSerializedSize());
        }
        Attachment attachment;
        try {
            attachment = client.getAttachment(request).get();
        } catch (InterruptedException e) {
            return Optional.empty();
        } catch (ExecutionException e) {
            return Optional.empty();
        }
        if (timer != null) {
            timer.stop();
            metrics.inboundBandwidth().mark(attachment.getSerializedSize());
            metrics.inboundGetAttachmentResponse().mark(request.getSerializedSize());
        }
        return Optional.ofNullable(attachment.equals(Attachment.getDefaultInstance()) ? null : attachment);
    }

    @Override
    public Optional<KERL_> getKERL(Ident identifier) {
        Context timer = metrics == null ? null : metrics.getKERLClient().time();
        IdentifierContext request = IdentifierContext.newBuilder()
                                                     .setIdentifier(identifier)
                                                     .setContext(context)
                                                     .build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundGetKERLRequest().mark(request.getSerializedSize());
        }
        KERL_ event;
        try {
            event = client.getKERL(request).get();
        } catch (InterruptedException | ExecutionException e) {
            return Optional.empty();
        }
        if (timer != null) {
            timer.stop();
            metrics.inboundBandwidth().mark(event.getSerializedSize());
            metrics.inboundGetKERLResponse().mark(event.getSerializedSize());
        }
        return Optional.ofNullable(event.equals(KERL_.getDefaultInstance()) ? null : event);
    }

    @Override
    public Optional<KeyEvent_> getKeyEvent(Digeste digest) {
        Context timer = metrics == null ? null : metrics.getKeyEventClient().time();
        EventDigestContext request = EventDigestContext.newBuilder().setDigest(digest).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundGetKeyEventRequest().mark(request.getSerializedSize());
        }
        KeyEvent_ event;
        try {
            event = client.getKeyEvent(request).get();
        } catch (InterruptedException | ExecutionException e) {
            return Optional.empty();
        }
        if (timer != null) {
            timer.stop();
        }
        metrics.inboundBandwidth().mark(event.getSerializedSize());
        metrics.inboundGetKeyEventResponse().mark(request.getSerializedSize());
        return Optional.ofNullable(event.equals(KeyEvent_.getDefaultInstance()) ? null : event);
    }

    @Override
    public Optional<KeyEvent_> getKeyEvent(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getKeyEventCoordsClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundGetKeyEventCoordsRequest().mark(request.getSerializedSize());
        }
        KeyEvent_ event;
        try {
            event = client.getKeyEventCoords(request).get();
        } catch (InterruptedException | ExecutionException e) {
            return Optional.empty();
        }
        if (timer != null) {
            timer.stop();
            metrics.inboundBandwidth().mark(event.getSerializedSize());
            metrics.inboundGetKeyEventCoordsResponse().mark(event.getSerializedSize());
        }
        return Optional.ofNullable(event.equals(KeyEvent_.getDefaultInstance()) ? null : event);
    }

    @Override
    public Optional<KeyState_> getKeyState(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getKeyStateCoordsClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundGetKeyStateCoordsRequest().mark(request.getSerializedSize());
        }
        KeyState_ event;
        try {
            event = client.getKeyStateCoords(request).get();
        } catch (InterruptedException | ExecutionException e) {
            return Optional.empty();
        }
        if (timer != null) {
            timer.stop();
            metrics.inboundBandwidth().mark(event.getSerializedSize());
            metrics.inboundGetKeyStateCoordsResponse().mark(event.getSerializedSize());
        }
        return Optional.ofNullable(event.equals(KeyState_.getDefaultInstance()) ? null : event);
    }

    @Override
    public Optional<KeyState_> getKeyState(Ident identifier) {
        Context timer = metrics == null ? null : metrics.getKeyStateClient().time();
        IdentifierContext request = IdentifierContext.newBuilder()
                                                     .setIdentifier(identifier)
                                                     .setContext(context)
                                                     .build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundGetKeyStateRequest().mark(request.getSerializedSize());
        }
        KeyState_ event;
        try {
            event = client.getKeyState(request).get();
        } catch (InterruptedException | ExecutionException e) {
            return Optional.empty();
        }
        if (timer != null) {
            timer.stop();
            metrics.inboundBandwidth().mark(event.getSerializedSize());
            metrics.inboundGetKeyStateResponse().mark(event.getSerializedSize());
        }
        return Optional.ofNullable(event.equals(KeyState_.getDefaultInstance()) ? null : event);
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public CompletableFuture<Void> publish(KERL_ kerl) {
        Context timer = metrics == null ? null : metrics.publishKERLClient().time();
        var request = KERLContext.newBuilder().setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundPublishKERLRequest().mark(request.getSerializedSize());
        }
        client.appendKERL(request);
        if (timer != null) {
            timer.stop();
        }
        var f = new CompletableFuture<Void>();
        f.complete(null);
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
        client.publishAttachments(request);
        if (timer != null) {
            timer.stop();
        }
        var f = new CompletableFuture<Void>();
        f.complete(null);
        return f;
    }

    @Override
    public CompletableFuture<Void> publishEvents(List<KeyEvent_> events) {
        Context timer = metrics == null ? null : metrics.publishEventsClient().time();
        KeyEventsContext request = KeyEventsContext.newBuilder().addAllKeyEvent(events).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundPublishEventsRequest().mark(request.getSerializedSize());
        }
        client.append(request);
        if (timer != null) {
            timer.stop();
        }
        var f = new CompletableFuture<Void>();
        f.complete(null);
        return f;
    }
}
