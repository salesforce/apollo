/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.AttachmentEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class ProtoKERLAdapter implements ProtoKERLService {

    private final KERL kerl;

    public ProtoKERLAdapter(KERL kerl) {
        this.kerl = kerl;
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(KERL_ k) {
        List<KeyEvent> events = new ArrayList<>();
        List<com.salesforce.apollo.stereotomy.event.AttachmentEvent> attachments = new ArrayList<>();
        k.getEventsList().stream().map(e -> ProtobufEventFactory.from(e)).forEach(ewa -> {
            events.add(ewa.event());
            attachments.add(ProtobufEventFactory.INSTANCE.attachment((EstablishmentEvent) ewa.event(),
                                                                     ewa.attachments()));
        });
        return kerl.append(events, attachments).thenApply(l -> l.stream().map(ks -> ks.toKeyState_()).toList());
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> keyEventList) {
        KeyEvent[] events = new KeyEvent[keyEventList.size()];
        int i = 0;
        for (KeyEvent event : keyEventList.stream().map(ke -> ProtobufEventFactory.from(ke)).toList()) {
            events[i++] = event;
        }
        return kerl.append(events).thenApply(l -> l.stream().map(ks -> ks.toKeyState_()).toList());
    }

    @Override
    public CompletableFuture<List<KeyState_>> append(List<KeyEvent_> eventsList,
                                                     List<AttachmentEvent> attachmentsList) {
        return kerl.append(eventsList.stream().map(ke -> ProtobufEventFactory.from(ke)).toList(),
                           attachmentsList.stream()
                                          .map(ae -> new AttachmentEventImpl(ae))
                                          .map(e -> (com.salesforce.apollo.stereotomy.event.AttachmentEvent) e)
                                          .toList())
                   .thenApply(l -> l.stream().map(ks -> ks == null ? null : ks.toKeyState_()).toList());
    }

    @Override
    public CompletableFuture<Empty> appendAttachments(List<AttachmentEvent> attachments) {
        return kerl.append(attachments.stream()
                                      .map(e -> new AttachmentEventImpl(e))
                                      .map(e -> (com.salesforce.apollo.stereotomy.event.AttachmentEvent) e)
                                      .toList())
                   .thenApply(n -> Empty.getDefaultInstance());
    }

    @Override
    public CompletableFuture<Empty> appendValidations(Validations validations) {
        return kerl.appendValidations(EventCoordinates.from(validations.getCoordinates()),
                                      validations.getValidationsList()
                                                 .stream()
                                                 .collect(Collectors.toMap(v -> Identifier.from(v.getValidator()),
                                                                           v -> JohnHancock.from(v.getSignature()))))
                   .thenApply(v -> Empty.getDefaultInstance());
    }

    @Override
    public CompletableFuture<Attachment> getAttachment(EventCoords coordinates) {
        return kerl.getAttachment(EventCoordinates.from(coordinates))
                   .thenApply(attch -> attch == null ? null : attch.toAttachemente());
    }

    public DigestAlgorithm getDigestAlgorithm() {
        return kerl.getDigestAlgorithm();
    }

    @Override
    public CompletableFuture<KERL_> getKERL(Ident identifier) {
        return kerl.kerl(Identifier.from(identifier)).thenApply(kerl -> kerl == null ? null : kerl(kerl));
    }

    @Override
    public CompletableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
        return kerl.getKeyEvent(EventCoordinates.from(coordinates))
                   .thenApply(event -> event == null ? null : event.toKeyEvent_());
    }

    @Override
    public CompletableFuture<KeyState_> getKeyState(EventCoords coordinates) {
        return kerl.getKeyState(EventCoordinates.from(coordinates))
                   .thenApply(ks -> ks == null ? null : ks.toKeyState_());
    }

    @Override
    public CompletableFuture<KeyState_> getKeyState(Ident identifier) {
        return kerl.getKeyState(Identifier.from(identifier)).thenApply(ks -> ks == null ? null : ks.toKeyState_());
    }

    @Override
    public CompletableFuture<KeyStateWithAttachments_> getKeyStateWithAttachments(EventCoords coords) {
        return kerl.getKeyStateWithAttachments(EventCoordinates.from(coords))
                   .thenApply(ksa -> ksa == null ? null : ksa.toEvente());
    }

    @Override
    public CompletableFuture<KeyStateWithEndorsementsAndValidations_> getKeyStateWithEndorsementsAndValidations(EventCoords coordinates) {
        return kerl.getKeyStateWithEndorsementsAndValidations(EventCoordinates.from(coordinates))
                   .thenApply(ks -> ks.toKS());
    }

    @Override
    public CompletableFuture<Validations> getValidations(EventCoords coords) {
        return kerl.getValidations(EventCoordinates.from(coords))
                   .thenApply(vs -> Validations.newBuilder()
                                               .addAllValidations(vs.entrySet()
                                                                    .stream()
                                                                    .map(e -> Validation_.newBuilder()
                                                                                         .setValidator(e.getKey()
                                                                                                        .toIdent())
                                                                                         .setSignature(e.getValue()
                                                                                                        .toSig())
                                                                                         .build())
                                                                    .toList())
                                               .build());
    }

    private KERL_ kerl(List<EventWithAttachments> k) {
        var builder = KERL_.newBuilder();
        k.forEach(ewa -> builder.addEvents(ewa.toKeyEvente()));
        return builder.build();
    }
}
