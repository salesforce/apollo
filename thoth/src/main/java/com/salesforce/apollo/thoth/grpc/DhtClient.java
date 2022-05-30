/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth.grpc;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyStateWithAttachments_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.AttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventWithAttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventsContext;
import com.salesfoce.apollo.thoth.proto.KerlDhtGrpc;
import com.salesfoce.apollo.thoth.proto.KerlDhtGrpc.KerlDhtFutureStub;
import com.salesfoce.apollo.thoth.proto.KeyStateWithEndorsementsAndValidations;
import com.salesfoce.apollo.thoth.proto.Validations;
import com.salesfoce.apollo.thoth.proto.ValidationsContext;
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
public class DhtClient implements DhtService {

    public static CreateClientCommunications<DhtService> getCreate(Digest context, StereotomyMetrics metrics) {
        return (t, f, c) -> {
            return new DhtClient(context, c, t, metrics);
        };
    }

    public static DhtService getLocalLoopback(Dht service, Member member) {
        return new DhtService() {

            @Override
            public ListenableFuture<Empty> append(KERL_ kerl) {
                return wrap(service.append(kerl).thenApply(ks -> Empty.getDefaultInstance()));
            }

            @Override
            public ListenableFuture<Empty> append(List<KeyEvent_> events) {
                return wrap(service.append(events).thenApply(ks -> Empty.getDefaultInstance()));
            }

            @Override
            public ListenableFuture<Empty> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
                return wrap(service.append(events, attachments).thenApply(ks -> Empty.getDefaultInstance()));
            }

            @Override
            public ListenableFuture<Empty> appendAttachments(List<AttachmentEvent> attachments) {
                return wrap(service.appendAttachments(attachments));
            }

            @Override
            public ListenableFuture<Empty> appendValidations(List<Validations> validations) {
                return wrap(service.appendValidations(validations));
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public ListenableFuture<Attachment> getAttachment(EventCoords coordinates) {
                return wrap(service.getAttachment(coordinates));
            }

            @Override
            public ListenableFuture<KERL_> getKERL(Ident identifier) {
                return wrap(service.getKERL(identifier));
            }

            @Override
            public ListenableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
                return wrap(service.getKeyEvent(coordinates));
            }

            @Override
            public ListenableFuture<KeyState_> getKeyState(EventCoords coordinates) {
                return wrap(service.getKeyState(coordinates));
            }

            @Override
            public ListenableFuture<KeyState_> getKeyState(Ident identifier) {
                return wrap(service.getKeyState(identifier));
            }

            @Override
            public ListenableFuture<KeyStateWithAttachments_> getKeyStateWithAttachments(EventCoords coordinates) {
                return wrap(service.getKeyStateWithAttachments(coordinates));
            }

            @Override
            public ListenableFuture<KeyStateWithEndorsementsAndValidations> getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
                return wrap(service.getKeyStateWithEndorsementsAndValidations(coordinates));
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<Validations> getValidations(EventCoords coordinates) {
                return wrap(service.getValidations(coordinates));
            }
        };
    }

    public static <T> ListenableFuture<T> wrap(CompletableFuture<T> future) {
        SettableFuture<T> fs = SettableFuture.create();
        future.whenComplete((r, t) -> {
            if (t != null) {
                fs.setException(t);
            } else {
                fs.set(r);
            }
        });
        return fs;
    }

    private final ManagedServerConnection channel;
    private final KerlDhtFutureStub       client;
    private final Digeste                 context;
    private final Member                  member;
    private final StereotomyMetrics       metrics;

    public DhtClient(Digest context, ManagedServerConnection channel, Member member, StereotomyMetrics metrics) {
        this.context = context.toDigeste();
        this.member = member;
        this.channel = channel;
        this.client = KerlDhtGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public ListenableFuture<Empty> append(KERL_ kerl) {
        Context timer = metrics == null ? null : metrics.appendKERLClient().time();
        var request = KERLContext.newBuilder().setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundAppendKERLRequest().mark(request.getSerializedSize());
        }
        var result = client.appendKERL(request);
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
        }, r -> r.run());
        return result;
    }

    @Override
    public ListenableFuture<Empty> append(List<KeyEvent_> keyEventList) {
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
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
        }, r -> r.run());
        return result;
    }

    @Override
    public ListenableFuture<Empty> append(List<KeyEvent_> eventsList, List<AttachmentEvent> attachmentsList) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        var request = KeyEventWithAttachmentsContext.newBuilder()
                                                    .addAllEvents(eventsList)
                                                    .addAllAttachments(attachmentsList)
                                                    .setContext(context)
                                                    .build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundAppendWithAttachmentsRequest().mark(request.getSerializedSize());
        }
        var result = client.appendWithAttachments(request);
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
        }, r -> r.run());
        return result;
    }

    @Override
    public ListenableFuture<Empty> appendAttachments(List<AttachmentEvent> attachmentsList) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        var request = AttachmentsContext.newBuilder().addAllAttachments(attachmentsList).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundAppendWithAttachmentsRequest().mark(request.getSerializedSize());
        }
        var result = client.appendAttachments(request);
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
        }, r -> r.run());
        return result;
    }

    @Override
    public ListenableFuture<Empty> appendValidations(List<Validations> validations) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        var request = ValidationsContext.newBuilder().addAllValidations(validations).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundAppendWithAttachmentsRequest().mark(request.getSerializedSize());
        }
        var result = client.appendValidations(request);
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
        }, r -> r.run());
        return result;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public ListenableFuture<Attachment> getAttachment(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundGetAttachmentRequest().mark(request.getSerializedSize());
        }
        ListenableFuture<Attachment> complete = client.getAttachment(request);
        complete.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            try {
                var attachment = complete.get();
                if (metrics != null) {
                    final var serializedSize = attachment.getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGetAttachmentResponse().mark(serializedSize);
                }
            } catch (InterruptedException e) {
            } catch (ExecutionException e) {
            }
        }, r -> r.run());
        return complete;
    }

    @Override
    public ListenableFuture<KERL_> getKERL(Ident identifier) {
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
        ListenableFuture<KERL_> complete = client.getKERL(request);
        complete.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            try {
                var kerl = complete.get();
                final var serializedSize = kerl.getSerializedSize();
                if (metrics != null) {
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGetKERLResponse().mark(serializedSize);
                }
            } catch (InterruptedException | ExecutionException e) {
            }
        }, r -> r.run());
        return complete;
    }

    @Override
    public ListenableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getKeyEventCoordsClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            final var bsize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetKeyEventCoordsRequest().mark(bsize);
        }
        var result = client.getKeyEventCoords(request);
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            KeyEvent_ ks;
            try {
                ks = result.get();
                if (timer != null) {
                    final var serializedSize = ks.getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGetKeyEventResponse().mark(serializedSize);
                }
            } catch (InterruptedException | ExecutionException e) {
            }
        }, r -> r.run());
        return result;
    }

    @Override
    public ListenableFuture<KeyState_> getKeyState(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getKeyStateCoordsClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            final var bs = request.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateCoordsRequest().mark(bs);
        }
        var result = client.getKeyStateCoords(request);
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            KeyState_ ks;
            try {
                ks = result.get();
                if (timer != null) {
                    final var serializedSize = ks.getSerializedSize();
                    timer.stop();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
                }
            } catch (InterruptedException | ExecutionException e) {
            }
        }, r -> r.run());
        return result;
    }

    @Override
    public ListenableFuture<KeyState_> getKeyState(Ident identifier) {
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
        result.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            KeyState_ ks;
            try {
                ks = result.get();
                if (timer != null) {
                    final var serializedSize = ks.getSerializedSize();
                    timer.stop();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
                }
            } catch (InterruptedException | ExecutionException e) {
            }
        }, r -> r.run());
        return result;
    }

    @Override
    public ListenableFuture<KeyStateWithAttachments_> getKeyStateWithAttachments(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundGetAttachmentRequest().mark(request.getSerializedSize());
        }
        ListenableFuture<KeyStateWithAttachments_> complete = client.getKeyStateWithAttachments(request);
        complete.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            try {
                var ksa = complete.get();
                if (metrics != null) {
                    final var serializedSize = ksa.getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGetAttachmentResponse().mark(serializedSize);
                }
            } catch (InterruptedException e) {
            } catch (ExecutionException e) {
            }
        }, r -> r.run());
        return complete;
    }

    @Override
    public ListenableFuture<KeyStateWithEndorsementsAndValidations> getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundGetAttachmentRequest().mark(request.getSerializedSize());
        }
        ListenableFuture<KeyStateWithEndorsementsAndValidations> complete = client.getKeyStateWithEndorsementsAndValidations(request);
        complete.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            try {
                var ksav = complete.get();
                if (metrics != null) {
                    final var serializedSize = ksav.getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGetAttachmentResponse().mark(serializedSize);
                }
            } catch (InterruptedException e) {
            } catch (ExecutionException e) {
            }
        }, r -> r.run());
        return complete;
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<Validations> getValidations(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        EventContext request = EventContext.newBuilder().setCoordinates(coordinates).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundGetAttachmentRequest().mark(request.getSerializedSize());
        }
        ListenableFuture<Validations> complete = client.getValidations(request);
        complete.addListener(() -> {
            if (timer != null) {
                timer.stop();
            }
            try {
                var v = complete.get();
                if (metrics != null) {
                    final var serializedSize = v.getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGetAttachmentResponse().mark(serializedSize);
                }
            } catch (InterruptedException e) {
            } catch (ExecutionException e) {
            }
        }, r -> r.run());
        return complete;
    }
}
