/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth.grpc.dht;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.event.proto.*;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.grpc.proto.*;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;
import com.salesforce.apollo.thoth.proto.KerlDhtGrpc;

import java.io.IOException;
import java.util.List;

/**
 * @author hal.hildebrand
 */
public class DhtClient implements DhtService {

    private final ManagedServerChannel            channel;
    private final KerlDhtGrpc.KerlDhtBlockingStub client;
    private final StereotomyMetrics               metrics;

    public DhtClient(ManagedServerChannel channel, StereotomyMetrics metrics) {
        this.channel = channel;
        this.client = KerlDhtGrpc.newBlockingStub(channel).withCompression("gzip");
        this.metrics = metrics;
    }

    public static CreateClientCommunications<DhtService> getCreate(StereotomyMetrics metrics) {
        return (c) -> {
            return new DhtClient(c, metrics);
        };
    }

    public static DhtService getLocalLoopback(ProtoKERLService service, Member member) {
        return new DhtService() {

            @Override
            public KeyStates append(KERL_ kerl) {
                return KeyStates.newBuilder().addAllKeyStates(service.append(kerl)).build();
            }

            @Override
            public KeyStates append(List<KeyEvent_> events) {
                return KeyStates.newBuilder().addAllKeyStates(service.append(events)).build();
            }

            @Override
            public KeyStates append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
                return KeyStates.newBuilder().addAllKeyStates(service.append(events, attachments)).build();
            }

            @Override
            public Empty appendAttachments(List<AttachmentEvent> attachments) {
                return service.appendAttachments(attachments);
            }

            @Override
            public Empty appendValidations(Validations validations) {
                return service.appendValidations(validations);
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public Attachment getAttachment(EventCoords coordinates) {
                return service.getAttachment(coordinates);
            }

            @Override
            public KERL_ getKERL(Ident identifier) {
                return service.getKERL(identifier);
            }

            @Override
            public KeyEvent_ getKeyEvent(EventCoords coordinates) {
                return service.getKeyEvent(coordinates);
            }

            @Override
            public KeyState_ getKeyState(EventCoords coordinates) {
                return service.getKeyState(coordinates);
            }

            @Override
            public KeyState_ getKeyState(Ident identifier) {
                return service.getKeyState(identifier);
            }

            @Override
            public KeyState_ getKeyState(IdentAndSeq identAndSeq) {
                return service.getKeyState(identAndSeq.getIdentifier(), identAndSeq.getSequenceNumber());
            }

            @Override
            public KeyStateWithAttachments_ getKeyStateWithAttachments(EventCoords coordinates) {
                return service.getKeyStateWithAttachments(coordinates);
            }

            @Override
            public KeyStateWithEndorsementsAndValidations_ getKeyStateWithEndorsementsAndValidations(
            EventCoords coordinates) {
                return service.getKeyStateWithEndorsementsAndValidations(coordinates);
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public Validations getValidations(EventCoords coordinates) {
                return service.getValidations(coordinates);
            }
        };
    }

    @Override
    public KeyStates append(KERL_ kerl) {
        Context timer = metrics == null ? null : metrics.appendKERLClient().time();
        var request = KERLContext.newBuilder().build();
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundAppendKERLRequest().mark(serializedSize);
        }
        var result = client.appendKERL(request);
        if (timer != null) {
            timer.stop();
        }
        return result;
    }

    @Override
    public KeyStates append(List<KeyEvent_> keyEventList) {
        Context timer = metrics == null ? null : metrics.appendEventsClient().time();
        KeyEventsContext request = KeyEventsContext.newBuilder().addAllKeyEvent(keyEventList).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundAppendEventsRequest().mark(request.getSerializedSize());
        }
        var result = client.append(request);
        if (timer != null) {
            timer.stop();
        }
        return result;
    }

    @Override
    public KeyStates append(List<KeyEvent_> eventsList, List<AttachmentEvent> attachmentsList) {
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
        if (timer != null) {
            timer.stop();
        }
        return result;
    }

    @Override
    public Empty appendAttachments(List<AttachmentEvent> attachmentsList) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        var request = AttachmentsContext.newBuilder().addAllAttachments(attachmentsList).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundAppendWithAttachmentsRequest().mark(request.getSerializedSize());
        }
        var result = client.appendAttachments(request);
        if (timer != null) {
            timer.stop();
        }
        return result;
    }

    @Override
    public Empty appendValidations(Validations validations) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        if (metrics != null) {
            final var serializedSize = validations.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundAppendWithAttachmentsRequest().mark(serializedSize);
        }
        var result = client.appendValidations(validations);
        if (timer != null) {
            timer.stop();
        }
        return result;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Attachment getAttachment(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        if (metrics != null) {
            final var serializedSize = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGetAttachmentRequest().mark(serializedSize);
        }
        Attachment complete = client.getAttachment(coordinates);
        if (timer != null) {
            timer.stop();
        }
        if (metrics != null) {
            final var serializedSize = complete.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetAttachmentResponse().mark(serializedSize);
        }
        return complete;
    }

    @Override
    public KERL_ getKERL(Ident identifier) {
        Context timer = metrics == null ? null : metrics.getKERLClient().time();
        if (metrics != null) {
            final var bsize = identifier.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetKERLRequest().mark(bsize);
        }
        KERL_ complete = client.getKERL(identifier);
        if (timer != null) {
            timer.stop();
        }
        final var serializedSize = complete.getSerializedSize();
        if (metrics != null) {
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKERLResponse().mark(serializedSize);
        }
        return complete;
    }

    @Override
    public KeyEvent_ getKeyEvent(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getKeyEventCoordsClient().time();
        if (metrics != null) {
            final var bsize = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetKeyEventCoordsRequest().mark(bsize);
        }
        var result = client.getKeyEventCoords(coordinates);
        if (timer != null) {
            timer.stop();
        }
        if (timer != null) {
            final var serializedSize = result.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyEventResponse().mark(serializedSize);
        }
        return result;
    }

    @Override
    public KeyState_ getKeyState(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getKeyStateCoordsClient().time();
        if (metrics != null) {
            final var bs = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateCoordsRequest().mark(bs);
        }
        var result = client.getKeyStateCoords(coordinates);
        if (timer != null) {
            timer.stop();
        }
        if (timer != null) {
            final var serializedSize = result.getSerializedSize();
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
        }
        return result;
    }

    @Override
    public KeyState_ getKeyState(Ident identifier) {
        Context timer = metrics == null ? null : metrics.getKeyStateClient().time();
        if (metrics != null) {
            final var bs = identifier.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateRequest().mark(bs);
        }
        var result = client.getKeyState(identifier);
        if (timer != null) {
            timer.stop();
        }
        if (timer != null) {
            final var serializedSize = result.getSerializedSize();
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
        }
        return result;
    }

    @Override
    public KeyState_ getKeyState(IdentAndSeq identAndSeq) {
        Context timer = metrics == null ? null : metrics.getKeyStateClient().time();
        if (metrics != null) {
            final var bs = identAndSeq.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateRequest().mark(bs);
        }
        var result = client.getKeyStateSeqNum(identAndSeq);
        if (timer != null) {
            timer.stop();
        }
        if (timer != null) {
            final var serializedSize = result.getSerializedSize();
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
        }
        return result;
    }

    @Override
    public KeyStateWithAttachments_ getKeyStateWithAttachments(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        if (metrics != null) {
            final var serializedSize = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGetAttachmentRequest().mark(serializedSize);
        }
        KeyStateWithAttachments_ complete = client.getKeyStateWithAttachments(coordinates);
        if (timer != null) {
            timer.stop();
        }
        if (metrics != null) {
            final var serializedSize = complete.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetAttachmentResponse().mark(serializedSize);
        }
        return complete;
    }

    @Override
    public KeyStateWithEndorsementsAndValidations_ getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        if (metrics != null) {
            final var serializedSize = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGetAttachmentRequest().mark(serializedSize);
        }
        KeyStateWithEndorsementsAndValidations_ complete = client.getKeyStateWithEndorsementsAndValidations(
        coordinates);
        if (timer != null) {
            timer.stop();
        }
        if (metrics != null) {
            final var serializedSize = complete.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetAttachmentResponse().mark(serializedSize);
        }
        return complete;
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    @Override
    public Validations getValidations(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        if (metrics != null) {
            final var serializedSize = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGetAttachmentRequest().mark(serializedSize);
        }
        Validations complete = client.getValidations(coordinates);
        if (timer != null) {
            timer.stop();
        }
        if (metrics != null) {
            final var serializedSize = complete.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetAttachmentResponse().mark(serializedSize);
        }
        return complete;
    }
}
