/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.kerl;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.event.proto.*;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.*;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author hal.hildebrand
 */
public class CommonKERLClient implements ProtoKERLService {

    protected final KERLServiceGrpc.KERLServiceBlockingStub client;
    protected final StereotomyMetrics                       metrics;

    public CommonKERLClient(KERLServiceGrpc.KERLServiceBlockingStub client, StereotomyMetrics metrics) {
        this.client = client;
        this.metrics = metrics;
    }

    public static KERLService getLocalLoopback(ProtoKERLService service, Member member) {
        return new KERLService() {

            @Override
            public List<KeyState_> append(KERL_ kerl) {
                return service.append(kerl);
            }

            @Override
            public List<KeyState_> append(List<KeyEvent_> events) {
                return service.append(events);
            }

            @Override
            public List<KeyState_> append(List<KeyEvent_> events, List<AttachmentEvent> attachments) {
                return service.append(events, attachments);
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
            public KeyState_ getKeyState(Ident identifier, long sequenceNumber) {
                return service.getKeyState(identifier, sequenceNumber);
            }

            @Override
            public KeyState_ getKeyState(Ident identifier) {
                return service.getKeyState(identifier);
            }

            @Override
            public KeyStateWithAttachments_ getKeyStateWithAttachments(EventCoords coords) {
                return service.getKeyStateWithAttachments(coords);
            }

            @Override
            public KeyStateWithEndorsementsAndValidations_ getKeyStateWithEndorsementsAndValidations(
            EventCoords coordinates) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public Validations getValidations(EventCoords coords) {
                return service.getValidations(coords);
            }
        };
    }

    @Override
    public List<KeyState_> append(KERL_ kerl) {
        Context timer = metrics == null ? null : metrics.appendKERLClient().time();
        var request = KERLContext.newBuilder().setKerl(kerl).build();
        final var bsize = request.getSerializedSize();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundAppendKERLRequest().mark(bsize);
        }
        var ks = client.appendKERL(request);
        if (timer != null) {
            timer.stop();
        }

        if (timer != null) {
            final var serializedSize = ks.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundAppendKERLResponse().mark(serializedSize);
        }

        if (ks.getKeyStatesCount() == 0) {
            return Collections.emptyList();
        } else {
            return ks.getKeyStatesList();
        }
    }

    @Override
    public List<KeyState_> append(List<KeyEvent_> keyEventList) {
        Context timer = metrics == null ? null : metrics.appendEventsClient().time();
        KeyEventsContext request = KeyEventsContext.newBuilder().addAllKeyEvent(keyEventList).build();
        final var bsize = request.getSerializedSize();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundAppendEventsRequest().mark(bsize);
        }
        var result = client.append(request);
        if (timer != null) {
            timer.stop();
        }
        KeyStates ks;
        ks = result;
        if (timer != null) {
            final var serializedSize = ks.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundAppendEventsResponse().mark(serializedSize);
        }
        if (ks.getKeyStatesCount() == 0) {
            return Collections.emptyList();
        } else {
            return ks.getKeyStatesList();
        }
    }

    @Override
    public List<KeyState_> append(List<KeyEvent_> eventsList, List<AttachmentEvent> attachmentsList) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        var request = KeyEventWithAttachmentsContext.newBuilder()
                                                    .addAllEvents(eventsList)
                                                    .addAllAttachments(attachmentsList)
                                                    .build();
        final var bsize = request.getSerializedSize();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundAppendWithAttachmentsRequest().mark(bsize);
        }
        var result = client.appendWithAttachments(request);
        if (timer != null) {
            timer.stop();
        }
        KeyStates ks = result;
        if (timer != null) {
            final var serializedSize = ks.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundAppendWithAttachmentsResponse().mark(serializedSize);
        }
        return ks.getKeyStatesList();
    }

    @Override
    public Empty appendAttachments(List<AttachmentEvent> attachments) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        var request = AttachmentsContext.newBuilder().addAllAttachments(attachments).build();
        final var bsize = request.getSerializedSize();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundAppendWithAttachmentsRequest().mark(bsize);
        }
        client.appendAttachments(request);
        return Empty.getDefaultInstance();
    }

    @Override
    public Empty appendValidations(Validations validations) {
        Context timer = metrics == null ? null : metrics.appendWithAttachmentsClient().time();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(validations.getSerializedSize());
            metrics.outboundAppendWithAttachmentsRequest().mark(validations.getSerializedSize());
        }
        var result = client.appendValidations(validations);
        if (timer != null) {
            timer.stop();
        }
        return result;
    }

    @Override
    public Attachment getAttachment(EventCoords coordinates) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        if (metrics != null) {
            final var bsize = coordinates.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetAttachmentRequest().mark(bsize);
        }
        var attachment = client.getAttachment(coordinates);
        if (timer != null) {
            timer.stop();
        }
        final var serializedSize = attachment.getSerializedSize();
        if (metrics != null) {
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetAttachmentResponse().mark(serializedSize);
        }
        return attachment.equals(Attachment.getDefaultInstance()) ? null : attachment;
    }

    @Override
    public KERL_ getKERL(Ident identifier) {
        Context timer = metrics == null ? null : metrics.getKERLClient().time();
        if (metrics != null) {
            final var bsize = identifier.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetKERLRequest().mark(bsize);
        }
        if (timer != null) {
            timer.stop();
        }
        var kerl = client.getKERL(identifier);
        final var serializedSize = kerl.getSerializedSize();
        if (metrics != null) {
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKERLResponse().mark(serializedSize);
        }
        return kerl.equals(KERL_.getDefaultInstance()) ? null : kerl;
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
        KeyEvent_ ks;
        ks = result;
        if (timer != null) {
            final var serializedSize = ks.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyEventResponse().mark(serializedSize);
        }
        return ks.equals(KeyEvent_.getDefaultInstance()) ? null : ks;
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
        KeyState_ ks;
        ks = result;
        if (timer != null) {
            final var serializedSize = ks.getSerializedSize();
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
        }
        return ks.equals(KeyState_.getDefaultInstance()) ? null : ks;
    }

    @Override
    public KeyState_ getKeyState(Ident identifier, long sequenceNumber) {
        Context timer = metrics == null ? null : metrics.getKeyStateClient().time();
        var identAndSeq = IdentAndSeq.newBuilder().setIdentifier(identifier).setSequenceNumber(sequenceNumber).build();
        if (metrics != null) {
            final var bs = identAndSeq.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateRequest().mark(bs);
        }
        var result = client.getKeyStateSeqNum(identAndSeq);
        if (timer != null) {
            timer.stop();
        }
        KeyState_ ks;
        ks = result;
        if (timer != null) {
            final var serializedSize = ks.getSerializedSize();
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
        }
        return ks.equals(KeyState_.getDefaultInstance()) ? null : ks;
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
        KeyState_ ks;
        ks = result;
        if (timer != null) {
            final var serializedSize = ks.getSerializedSize();
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
        }
        return ks.equals(KeyState_.getDefaultInstance()) ? null : ks;
    }

    @Override
    public KeyStateWithAttachments_ getKeyStateWithAttachments(EventCoords coords) {
        Context timer = metrics == null ? null : metrics.getKeyStateCoordsClient().time();
        if (metrics != null) {
            final var bs = coords.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateCoordsRequest().mark(bs);
        }
        var result = client.getKeyStateWithAttachments(coords);
        if (timer != null) {
            timer.stop();
        }
        KeyStateWithAttachments_ ks;
        ks = result;
        if (timer != null) {
            final var serializedSize = ks.getSerializedSize();
            timer.stop();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateCoordsResponse().mark(serializedSize);
        }
        return ks.equals(KeyStateWithAttachments_.getDefaultInstance()) ? null : ks;
    }

    @Override
    public KeyStateWithEndorsementsAndValidations_ getKeyStateWithEndorsementsAndValidations(EventCoords coords) {
        Context timer = metrics == null ? null : metrics.getKeyStateCoordsClient().time();
        if (metrics != null) {
            final var bs = coords.getSerializedSize();
            metrics.outboundBandwidth().mark(bs);
            metrics.outboundGetKeyStateCoordsRequest().mark(bs);
        }
        var result = client.getKeyStateWithEndorsementsAndValidations(coords);
        if (timer != null) {
            timer.stop();
        }
        KeyStateWithEndorsementsAndValidations_ ks;
        ks = result;
        return ks.equals(KeyStateWithEndorsementsAndValidations_.getDefaultInstance()) ? null : ks;
    }

    @Override
    public Validations getValidations(EventCoords coords) {
        Context timer = metrics == null ? null : metrics.getAttachmentClient().time();
        if (metrics != null) {
            final var bsize = coords.getSerializedSize();
            metrics.outboundBandwidth().mark(bsize);
            metrics.outboundGetAttachmentRequest().mark(bsize);
        }
        if (timer != null) {
            timer.stop();
        }
        var validations = client.getValidations(coords);
        final var serializedSize = validations.getSerializedSize();
        if (metrics != null) {
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetAttachmentResponse().mark(serializedSize);
        }
        return validations.equals(Validations.getDefaultInstance()) ? null : validations;
    }

}
