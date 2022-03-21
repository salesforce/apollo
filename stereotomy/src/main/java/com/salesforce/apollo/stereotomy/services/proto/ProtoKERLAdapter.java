/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
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
        List<AttachmentEvent> attachments = new ArrayList<>();
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
                                                     List<com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent> attachmentsList) {
        return kerl.append(eventsList.stream().map(ke -> ProtobufEventFactory.from(ke)).toList(),
                           attachmentsList.stream()
                                          .map(ae -> new AttachmentEventImpl(ae))
                                          .map(ae -> (AttachmentEvent) ae)
                                          .toList())
                   .thenApply(l -> l.stream().map(ks -> ks.toKeyState_()).toList());
    }

    @Override
    public Optional<Attachment> getAttachment(EventCoords coordinates) {
        return kerl.getAttachment(EventCoordinates.from(coordinates)).map(attch -> attch.toAttachemente());
    }

    @Override
    public Optional<KERL_> getKERL(Ident identifier) {
        return kerl.kerl(Identifier.from(identifier)).map(kerl -> kerl(kerl));
    }

    @Override
    public Optional<KeyEvent_> getKeyEvent(Digeste digest) {
        return kerl.getKeyEvent(Digest.from(digest)).map(event -> event.toKeyEvent_());
    }

    @Override
    public Optional<KeyEvent_> getKeyEvent(EventCoords coordinates) {
        return kerl.getKeyEvent(EventCoordinates.from(coordinates)).map(event -> event.toKeyEvent_());
    }

    @Override
    public Optional<KeyState_> getKeyState(EventCoords coordinates) {
        return kerl.getKeyState(EventCoordinates.from(coordinates)).map(ks -> ks.toKeyState_());
    }

    @Override
    public Optional<KeyState_> getKeyState(Ident identifier) {
        return kerl.getKeyState(Identifier.from(identifier)).map(ks -> ks.toKeyState_());
    }

    private KERL_ kerl(List<EventWithAttachments> k) {
        var builder = KERL_.newBuilder();
        k.forEach(ewa -> builder.addEvents(ewa.toKeyEvente()));
        return builder.build();
    }
}
