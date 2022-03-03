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
import com.salesfoce.apollo.stereotomy.event.proto.KERL;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent;
import com.salesforce.apollo.stereotomy.EventCoordinates;
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

    private final com.salesforce.apollo.stereotomy.KERL kerl;

    public ProtoKERLService(com.salesforce.apollo.stereotomy.KERL kerl) {
        this.kerl = kerl;
    }

    @Override
    public CompletableFuture<Boolean> append(KeyEvent keyEvent) {
        var ewa = ProtobufEventFactory.from(keyEvent);
        return kerl.append(Collections.singletonList(ewa.event()),
                           Collections.singletonList(ProtobufEventFactory.INSTANCE.attachment((EstablishmentEvent) ewa.event(),
                                                                                              ewa.attachments())))
                   .thenApply(ks -> true);
    }

    @Override
    public Optional<KERL> kerl(Ident prefix) {
        return kerl.kerl(Identifier.from(prefix)).map(kerl -> kerl(kerl));
    }

    @Override
    public CompletableFuture<Boolean> publish(KERL k) {
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
    public Optional<com.salesfoce.apollo.stereotomy.event.proto.KeyState> resolve(EventCoords coordinates) {
        return kerl.getKeyState(EventCoordinates.from(coordinates)).map(ks -> ks.toKeyState());
    }

    @Override
    public Optional<com.salesfoce.apollo.stereotomy.event.proto.KeyState> resolve(Ident prefix) {
        return kerl.getKeyState(Identifier.from(prefix)).map(ks -> ks.toKeyState());
    }

    private KERL kerl(List<EventWithAttachments> kerl) {
        var builder = KERL.newBuilder();
        kerl.forEach(ewa -> builder.addEvents(ewa.toKeyEvente()));
        return builder.build();
    }
}
