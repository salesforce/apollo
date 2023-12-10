/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import com.google.protobuf.Empty;
import com.salesforce.apollo.stereotomy.event.proto.*;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KEL;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.KeyStateWithEndorsementsAndValidations;
import com.salesforce.apollo.stereotomy.event.protobuf.AttachmentEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import org.joou.ULong;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author hal.hildebrand
 */
public class ProtoKERLAdapter implements ProtoKERLService {

    private final KERL kerl;

    public ProtoKERLAdapter(KERL kerl) {
        this.kerl = kerl;
    }

    @Override
    public List<KeyState_> append(KERL_ k) {
        List<KeyEvent> events = new ArrayList<>();
        List<com.salesforce.apollo.stereotomy.event.AttachmentEvent> attachments = new ArrayList<>();
        k.getEventsList().stream().map(e -> ProtobufEventFactory.from(e)).forEach(ewa -> {
            events.add(ewa.event());
            attachments.add(
            ProtobufEventFactory.INSTANCE.attachment((EstablishmentEvent) ewa.event(), ewa.attachments()));
        });
        return kerl.append(events, attachments)
                   .stream()
                   .map(ks -> ks == null ? KeyState_.getDefaultInstance() : ks.toKeyState_())
                   .toList();
    }

    @Override
    public List<KeyState_> append(List<KeyEvent_> keyEventList) {
        KeyEvent[] events = new KeyEvent[keyEventList.size()];
        int i = 0;
        for (KeyEvent event : keyEventList.stream().map(ke -> ProtobufEventFactory.from(ke)).toList()) {
            events[i++] = event;
        }
        List<KeyState> keyStates = kerl.append(events);
        return keyStates == null ? Collections.emptyList() : (keyStates.stream()
                                                                       .map(
                                                                       ks -> ks == null ? KeyState_.getDefaultInstance()
                                                                                        : ks.toKeyState_())
                                                                       .toList());
    }

    @Override
    public List<KeyState_> append(List<KeyEvent_> eventsList, List<AttachmentEvent> attachmentsList) {
        return kerl.append(eventsList.stream().map(ke -> ProtobufEventFactory.from(ke)).toList(),
                           attachmentsList.stream()
                                          .map(ae -> new AttachmentEventImpl(ae))
                                          .map(e -> (com.salesforce.apollo.stereotomy.event.AttachmentEvent) e)
                                          .toList()).stream().map(ks -> ks == null ? null : ks.toKeyState_()).toList();
    }

    @Override
    public Empty appendAttachments(List<AttachmentEvent> attachments) {
        kerl.append(attachments.stream()
                               .map(e -> new AttachmentEventImpl(e))
                               .map(e -> (com.salesforce.apollo.stereotomy.event.AttachmentEvent) e)
                               .toList());
        return Empty.getDefaultInstance();
    }

    @Override
    public Empty appendValidations(Validations validations) {
        kerl.appendValidations(EventCoordinates.from(validations.getCoordinates()), validations.getValidationsList()
                                                                                               .stream()
                                                                                               .collect(
                                                                                               Collectors.toMap(
                                                                                               v -> EventCoordinates.from(
                                                                                               v.getValidator()),
                                                                                               v -> JohnHancock.from(
                                                                                               v.getSignature()))));
        return Empty.getDefaultInstance();
    }

    @Override
    public Attachment getAttachment(EventCoords coordinates) {
        var attch = kerl.getAttachment(EventCoordinates.from(coordinates));
        return attch == null ? Attachment.getDefaultInstance() : attch.toAttachemente();
    }

    public DigestAlgorithm getDigestAlgorithm() {
        return kerl.getDigestAlgorithm();
    }

    @Override
    public KERL_ getKERL(Ident identifier) {
        List<EventWithAttachments> kerl = this.kerl.kerl(Identifier.from(identifier));
        return kerl == null ? KERL_.getDefaultInstance() : kerl(kerl);
    }

    @Override
    public KeyEvent_ getKeyEvent(EventCoords coordinates) {
        var event = kerl.getKeyEvent(EventCoordinates.from(coordinates));
        return event == null ? KeyEvent_.getDefaultInstance() : event.toKeyEvent_();
    }

    @Override
    public KeyState_ getKeyState(EventCoords coordinates) {
        KeyState ks = kerl.getKeyState(EventCoordinates.from(coordinates));
        return ks == null ? KeyState_.getDefaultInstance() : ks.toKeyState_();
    }

    @Override
    public KeyState_ getKeyState(Ident identifier, long sequenceNumber) {
        KeyState ks = kerl.getKeyState(Identifier.from(identifier), ULong.valueOf(sequenceNumber));
        return ks == null ? KeyState_.getDefaultInstance() : ks.toKeyState_();
    }

    @Override
    public KeyState_ getKeyState(Ident identifier) {
        KeyState ks = kerl.getKeyState(Identifier.from(identifier));
        return ks == null ? KeyState_.getDefaultInstance() : ks.toKeyState_();
    }

    @Override
    public KeyStateWithAttachments_ getKeyStateWithAttachments(EventCoords coords) {
        KEL.KeyStateWithAttachments ksa = kerl.getKeyStateWithAttachments(EventCoordinates.from(coords));
        return ksa == null ? KeyStateWithAttachments_.getDefaultInstance() : ksa.toEvente();
    }

    @Override
    public KeyStateWithEndorsementsAndValidations_ getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
        KeyStateWithEndorsementsAndValidations ks = kerl.getKeyStateWithEndorsementsAndValidations(
        EventCoordinates.from(coordinates));
        return ks == null ? KeyStateWithEndorsementsAndValidations_.getDefaultInstance() : ks.toKS();
    }

    @Override
    public Validations getValidations(EventCoords coords) {
        Map<EventCoordinates, JohnHancock> vs = kerl.getValidations(EventCoordinates.from(coords));
        return Validations.newBuilder()
                          .addAllValidations(vs.entrySet()
                                               .stream()
                                               .map(e -> Validation_.newBuilder()
                                                                    .setValidator(e.getKey().toEventCoords())
                                                                    .setSignature(e.getValue().toSig())
                                                                    .build())
                                               .toList())
                          .build();
    }

    private KERL_ kerl(List<EventWithAttachments> k) {
        var builder = KERL_.newBuilder();
        k.forEach(ewa -> builder.addEvents(ewa.toKeyEvente()));
        return builder.build();
    }
}
