/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.kerl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyStateWithAttachments_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyStateWithEndorsementsAndValidations_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.AttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc.KERLServiceFutureStub;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventWithAttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyStates;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.ValidationsContext;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;

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

    public static KERLService getLocalLoopback(ProtoKERLService service, Member member) {
        return new KERLService() {

            @Override
            public CompletableFuture<List<KeyState_>> append(KERL_ kerl) {
                return service.append(kerl);
            }

            @Override
            public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events) {
                return service.append(events);
            }

            @Override
            public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events,
                                                             List<AttachmentEvent> attachments) {
                return service.append(events, attachments);
            }

            @Override
            public CompletableFuture<Empty> appendAttachments(List<AttachmentEvent> attachments) {
                return service.appendAttachments(attachments);
            }

            @Override
            public CompletableFuture<Empty> appendValidations(Validations validations) {
                return service.appendValidations(validations);
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public CompletableFuture<Attachment> getAttachment(EventCoords coordinates) {
                return service.getAttachment(coordinates);
            }

            @Override
            public CompletableFuture<KERL_> getKERL(Ident identifier) {
                return service.getKERL(identifier);
            }

            @Override
            public CompletableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
                return service.getKeyEvent(coordinates);
            }

            @Override
            public CompletableFuture<KeyState_> getKeyState(EventCoords coordinates) {
                return service.getKeyState(coordinates);
            }

            @Override
            public CompletableFuture<KeyState_> getKeyState(Ident identifier) {
                return service.getKeyState(identifier);
            }

            @Override
            public CompletableFuture<KeyStateWithAttachments_> getKeyStateWithAttachments(EventCoords coords) {
                return service.getKeyStateWithAttachments(coords);
            }

            @Override
            public CompletableFuture<KeyStateWithEndorsementsAndValidations_> getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public CompletableFuture<Validations> getValidations(EventCoords coords) {
                return service.getValidations(coords);
            }
        };
    }

    private final ManagedServerChannel channel;
    private final KERLServiceFutureStub   client;
    private final Digeste                 context;
    private final Member                  member;
    private final StereotomyMetrics       metrics;

    public KERLClient(Digest context, ManagedServerChannel channel, Member member, StereotomyMetrics metrics) {
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
        final var bsize = request.getSerializedSize();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundAppendKERLRequest().mark(bsize);
        }
        var result = client.appendKERL(request);
        var f = new CompletableFuture<List<KeyState_>>();
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
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
                final var serializedSize = ks.getSerializedSize();
                metrics.inboundBandwidth().mark(serializedSize);
                metrics.inboundAppendKERLResponse().mark(serializedSize);
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
        final var bsize = request.getSerializedSize();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundAppendEventsRequest().mark(bsize);
        }
        var result = client.append(request);
        var f = new CompletableFuture<List<KeyState_>>();
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
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
                final var serializedSize = ks.getSerializedSize();
                metrics.inboundBandwidth().mark(serializedSize);
                metrics.inboundAppendEventsResponse().mark(serializedSize);
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> eventsList,
                                                     List<AttachmentEvent> attachmentsList) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        var request = KeyEventWithAttachmentsContext.newBuilder()
                                                    .addAllEvents(eventsList)
                                                    .addAllAttachments(attachmentsList)
                                                    .setContext(context)
                                                    .build();
        final var bsize = request.getSerializedSize();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundAppendWithAttachmentsRequest().mark(bsize);
        }
        var result = client.appendWithAttachments(request);
        var f = new CompletableFuture<List<KeyState_>>();
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
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
            f.complete(ks.getKeyStatesList());
            if (timer != null) {
                final var serializedSize = ks.getSerializedSize();
                metrics.inboundBandwidth().mark(serializedSize);
                metrics.inboundAppendWithAttachmentsResponse().mark(serializedSize);
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<Empty> appendAttachments(List<AttachmentEvent> attachments) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        var request = AttachmentsContext.newBuilder().addAllAttachments(attachments).setContext(context).build();
        final var bsize = request.getSerializedSize();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundAppendWithAttachmentsRequest().mark(bsize);
        }
        var result = client.appendAttachments(request);
        var f = new CompletableFuture<Empty>();
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
    public CompletableFuture<Empty> appendValidations(Validations validations) {
        var f = new CompletableFuture<Empty>();
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        var request = ValidationsContext.newBuilder().setValidations(validations).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundAppendWithAttachmentsRequest().mark(request.getSerializedSize());
        }
        var result = client.appendValidations(request);
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            f.complete(Empty.getDefaultInstance());
        }, r -> r.run());
        return f;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public CompletableFuture<Attachment> getAttachment(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            final var bsize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetAttachmentRequest().mark(bsize);
        }
        var f = new CompletableFuture<Attachment>();
        ListenableFuture<Attachment> complete = client.getAttachment(request);
        complete.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            try {
                var attachment = client.getAttachment(request).get();
                final var serializedSize = attachment.getSerializedSize();
                f.complete(attachment.equals(Attachment.getDefaultInstance()) ? null : attachment);
                if (metrics != null) {
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGetAttachmentResponse().mark(serializedSize);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                f.completeExceptionally(e);
            } catch (ExecutionException e) {
                f.completeExceptionally(e);
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<KERL_> getKERL(Ident identifier) {
        Context timer = metrics == null ? null : metrics.getKERLClient().time();
        IdentifierContext request = IdentifierContext.newBuilder()
                                                     .setIdentifier(identifier)
                                                     .setContext(context)
                                                     .build();
        if (metrics != null) {
            final var bsize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetKERLRequest().mark(bsize);
        }
        var f = new CompletableFuture<KERL_>();
        ListenableFuture<KERL_> complete = client.getKERL(request);
        complete.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            try {
                var kerl = client.getKERL(request).get();
                final var serializedSize = kerl.getSerializedSize();
                if (metrics != null) {
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGetKERLResponse().mark(serializedSize);
                }
                f.complete(kerl.equals(KERL_.getDefaultInstance()) ? null : kerl);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                f.completeExceptionally(e);
            } catch (ExecutionException e) {
                f.completeExceptionally(e);
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getKeyEventCoordsClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            final var bsize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetKeyEventCoordsRequest().mark(bsize);
        }
        var result = client.getKeyEventCoords(request);
        var f = new CompletableFuture<KeyEvent_>();
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            KeyEvent_ ks;
            try {
                ks = result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            f.complete(ks.equals(KeyEvent_.getDefaultInstance()) ? null : ks);
            if (timer != null) {
                final var serializedSize = ks.getSerializedSize();
                metrics.inboundBandwidth().mark(serializedSize);
                metrics.inboundGetKeyEventResponse().mark(serializedSize);
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<KeyState_> getKeyState(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getKeyStateCoordsClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            final var bs = request.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateCoordsRequest().mark(bs);
        }
        var result = client.getKeyStateCoords(request);
        var f = new CompletableFuture<KeyState_>();
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            KeyState_ ks;
            try {
                ks = result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            f.complete(ks.equals(KeyState_.getDefaultInstance()) ? null : ks);
            if (timer != null) {
                final var serializedSize = ks.getSerializedSize();
                timer.stop();
                metrics.inboundBandwidth().mark(serializedSize);
                metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<KeyState_> getKeyState(Ident identifier) {
        Context timer = metrics == null ? null : metrics.getKeyStateClient().time();
        IdentifierContext request = IdentifierContext.newBuilder()
                                                     .setIdentifier(identifier)
                                                     .setContext(context)
                                                     .build();
        if (metrics != null) {
            final var bs = request.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateRequest().mark(bs);
        }
        var result = client.getKeyState(request);
        var f = new CompletableFuture<KeyState_>();
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            KeyState_ ks;
            try {
                ks = result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            f.complete(ks.equals(KeyState_.getDefaultInstance()) ? null : ks);
            if (timer != null) {
                final var serializedSize = ks.getSerializedSize();
                timer.stop();
                metrics.inboundBandwidth().mark(serializedSize);
                metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<KeyStateWithAttachments_> getKeyStateWithAttachments(EventCoords coords) {
        Context timer = metrics == null ? null : metrics.getKeyStateCoordsClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coords).setContext(context).build();
        if (metrics != null) {
            final var bs = request.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateCoordsRequest().mark(bs);
        }
        var result = client.getKeyStateWithAttachments(request);
        var f = new CompletableFuture<KeyStateWithAttachments_>();
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            KeyStateWithAttachments_ ks;
            try {
                ks = result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            f.complete(ks.equals(KeyStateWithAttachments_.getDefaultInstance()) ? null : ks);
            if (timer != null) {
                final var serializedSize = ks.getSerializedSize();
                timer.stop();
                metrics.inboundBandwidth().mark(serializedSize);
                metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public CompletableFuture<KeyStateWithEndorsementsAndValidations_> getKeyStateWithEndorsementsAndValidations(EventCoords coords) {
        Context timer = metrics == null ? null : metrics.getKeyStateCoordsClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coords).setContext(context).build();
        if (metrics != null) {
            final var bs = request.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateCoordsRequest().mark(bs);
        }
        var result = client.getKeyStateWithEndorsementsAndValidations(request);
        var f = new CompletableFuture<KeyStateWithEndorsementsAndValidations_>();
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            KeyStateWithEndorsementsAndValidations_ ks;
            try {
                ks = result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            f.complete(ks.equals(KeyStateWithEndorsementsAndValidations_.getDefaultInstance()) ? null : ks);
            if (timer != null) {
                final var serializedSize = ks.getSerializedSize();
                timer.stop();
                metrics.inboundBandwidth().mark(serializedSize);
                metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
            }
        }, r -> r.run());
        return f;
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public CompletableFuture<Validations> getValidations(EventCoords coords) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coords).setContext(context).build();
        if (metrics != null) {
            final var bsize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetAttachmentRequest().mark(bsize);
        }
        var f = new CompletableFuture<Validations>();
        ListenableFuture<Attachment> complete = client.getAttachment(request);
        complete.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            try {
                var validations = client.getValidations(request).get();
                final var serializedSize = validations.getSerializedSize();
                f.complete(validations.equals(Validations.getDefaultInstance()) ? null : validations);
                if (metrics != null) {
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGetAttachmentResponse().mark(serializedSize);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                f.completeExceptionally(e);
            } catch (ExecutionException e) {
                f.completeExceptionally(e);
            }
        }, r -> r.run());
        return f;
    }
}
