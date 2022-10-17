/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth.grpc.dht;

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
import com.salesfoce.apollo.stereotomy.event.proto.KeyStateWithEndorsementsAndValidations_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.AttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventWithAttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyStates;
import com.salesfoce.apollo.thoth.proto.KerlDhtGrpc;
import com.salesfoce.apollo.thoth.proto.KerlDhtGrpc.KerlDhtFutureStub;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;

/**
 * @author hal.hildebrand
 *
 */
public class DhtClient implements DhtService {

    public static CreateClientCommunications<DhtService> getCreate(StereotomyMetrics metrics) {
        return (c) -> {
            return new DhtClient(c, metrics);
        };
    }

    public static DhtService getLocalLoopback(ProtoKERLService service, Member member) {
        return new DhtService() {

            @Override
            public ListenableFuture<KeyStates> append(KERL_ kerl) {
                return wrap(service.append(kerl).thenApply(lks -> KeyStates.newBuilder().addAllKeyStates(lks).build()));
            }

            @Override
            public ListenableFuture<KeyStates> append(List<KeyEvent_> events) {
                return wrap(service.append(events)
                                   .thenApply(lks -> KeyStates.newBuilder().addAllKeyStates(lks).build()));
            }

            @Override
            public ListenableFuture<KeyStates> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
                return wrap(service.append(events, attachments)
                                   .thenApply(lks -> KeyStates.newBuilder().addAllKeyStates(lks).build()));
            }

            @Override
            public ListenableFuture<Empty> appendAttachments(List<AttachmentEvent> attachments) {
                return wrap(service.appendAttachments(attachments));
            }

            @Override
            public ListenableFuture<Empty> appendValidations(Validations validations) {
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
            public ListenableFuture<KeyStateWithEndorsementsAndValidations_> getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
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

    private final ManagedServerChannel channel;
    private final KerlDhtFutureStub    client;
    private final StereotomyMetrics    metrics;

    public DhtClient(ManagedServerChannel channel, StereotomyMetrics metrics) {
        this.channel = channel;
        this.client = KerlDhtGrpc.newFutureStub(channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public ListenableFuture<KeyStates> append(KERL_ kerl) {
        Context timer = metrics == null ? null : metrics.appendKERLClient().time();
        var request = KERLContext.newBuilder().build();
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundAppendKERLRequest().mark(serializedSize);
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
    public ListenableFuture<KeyStates> append(List<KeyEvent_> keyEventList) {
        Context timer = metrics == null ? null : metrics.appendEventsClient().time();
        KeyEventsContext request = KeyEventsContext.newBuilder().addAllKeyEvent(keyEventList).build();
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
    public ListenableFuture<KeyStates> append(List<KeyEvent_> eventsList, List<AttachmentEvent> attachmentsList) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        var request = KeyEventWithAttachmentsContext.newBuilder()
                                                    .addAllEvents(eventsList)
                                                    .addAllAttachments(attachmentsList)
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
        var request = AttachmentsContext.newBuilder().addAllAttachments(attachmentsList).build();
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
    public ListenableFuture<Empty> appendValidations(Validations validations) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        if (metrics != null) {
            final var serializedSize = validations.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundAppendWithAttachmentsRequest().mark(serializedSize);
        }
        var result = client.appendValidations(validations);
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
        if (metrics != null) {
            final var serializedSize = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGetAttachmentRequest().mark(serializedSize);
        }
        ListenableFuture<Attachment> complete = client.getAttachment(coordinates);
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
        if (metrics != null) {
            final var bsize = identifier.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetKERLRequest().mark(bsize);
        }
        ListenableFuture<KERL_> complete = client.getKERL(identifier);
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
        if (metrics != null) {
            final var bsize = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetKeyEventCoordsRequest().mark(bsize);
        }
        var result = client.getKeyEventCoords(coordinates);
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
        if (metrics != null) {
            final var bs = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateCoordsRequest().mark(bs);
        }
        var result = client.getKeyStateCoords(coordinates);
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
        if (metrics != null) {
            final var bs = identifier.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateRequest().mark(bs);
        }
        var result = client.getKeyState(identifier);
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
        if (metrics != null) {
            final var serializedSize = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGetAttachmentRequest().mark(serializedSize);
        }
        ListenableFuture<KeyStateWithAttachments_> complete = client.getKeyStateWithAttachments(coordinates);
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
    public ListenableFuture<KeyStateWithEndorsementsAndValidations_> getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        if (metrics != null) {
            final var serializedSize = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGetAttachmentRequest().mark(serializedSize);
        }
        ListenableFuture<KeyStateWithEndorsementsAndValidations_> complete = client.getKeyStateWithEndorsementsAndValidations(coordinates);
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
        return channel.getMember();
    }

    @Override
    public ListenableFuture<Validations> getValidations(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        if (metrics != null) {
            final var serializedSize = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGetAttachmentRequest().mark(serializedSize);
        }
        ListenableFuture<Validations> complete = client.getValidations(coordinates);
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
