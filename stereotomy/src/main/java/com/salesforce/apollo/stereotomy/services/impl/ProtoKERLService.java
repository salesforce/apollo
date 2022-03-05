/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEventWithAttachments;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLProvider;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLRecorder;

/**
 * @author hal.hildebrand
 *
 */
public class ProtoKERLService implements ProtoKERLProvider, ProtoKERLRecorder {

    private final KERL kerl;

    public ProtoKERLService(KERL kerl) {
        this.kerl = kerl;
    }

    @Override
    public CompletableFuture<Boolean> append(KeyEventWithAttachments keyEvent) {
        var ewa = ProtobufEventFactory.from(keyEvent);
        return kerl.append(Collections.singletonList(ewa.event()),
                           Collections.singletonList(ProtobufEventFactory.INSTANCE.attachment((EstablishmentEvent) ewa.event(),
                                                                                              ewa.attachments())))
                   .thenApply(ks -> true);
    }

    @Override
    public CompletableFuture<KeyState_> appendWithReturn(KeyEvent_ keyEvent) {
        return kerl.append(ProtobufEventFactory.from(keyEvent)).thenApply(ks -> ks.toKeyState_());
    }

    @Override
    public Optional<KERL_> kerl(Ident prefix) {
        return kerl.kerl(Identifier.from(prefix)).map(kerl -> kerl(kerl));
    }

    @Override
    public CompletableFuture<Boolean> publish(KERL_ k) {
        List<com.salesforce.apollo.stereotomy.event.KeyEvent> events = new ArrayList<>();
        List<AttachmentEvent> attachments = new ArrayList<>();
        k.getEventsList().stream().map(e -> ProtobufEventFactory.from(e)).forEach(ewa -> {
            events.add(ewa.event());
            attachments.add(ProtobufEventFactory.INSTANCE.attachment((EstablishmentEvent) ewa.event(),
                                                                     ewa.attachments()));
        });
        return kerl.append(events, attachments).thenApply(ks -> true);
    }

    @Override
    public CompletableFuture<List<KeyState_>> publishWithReturn(KERL_ k) {
        List<com.salesforce.apollo.stereotomy.event.KeyEvent> events = new ArrayList<>();
        List<AttachmentEvent> attachments = new ArrayList<>();
        k.getEventsList().stream().map(e -> ProtobufEventFactory.from(e)).forEach(ewa -> {
            events.add(ewa.event());
            attachments.add(ProtobufEventFactory.INSTANCE.attachment((EstablishmentEvent) ewa.event(),
                                                                     ewa.attachments()));
        });
        return kerl.append(events, attachments).thenApply(ks -> ks.stream().map(state -> state.toKeyState_()).toList());
    }

    @Override
    public Optional<KeyState_> resolve(EventCoords coordinates) {
        return kerl.getKeyState(EventCoordinates.from(coordinates)).map(ks -> ks.toKeyState_());
    }

    @Override
    public Optional<KeyState_> resolve(Ident prefix) {
        return kerl.getKeyState(Identifier.from(prefix)).map(ks -> ks.toKeyState_());
    }

    private KERL_ kerl(List<EventWithAttachments> k) {
        var builder = KERL_.newBuilder();
        k.forEach(ewa -> builder.addEvents(ewa.toKeyEvente()));
        return builder.build();
    }
}
