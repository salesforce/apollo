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
import java.util.concurrent.TimeoutException;

import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.KERLProvider;
import com.salesforce.apollo.stereotomy.services.KERLRecorder;

/**
 * @author hal.hildebrand
 *
 */
public class MemKERLService implements KERLProvider, KERLRecorder {

    private final KERL kerl;

    public MemKERLService(KERL kerl) {
        this.kerl = kerl;
    }

    @Override
    public void append(EventWithAttachments event) {
        kerl.append(Collections.singletonList(event.event()),
                    Collections.singletonList(ProtobufEventFactory.INSTANCE.attachment((EstablishmentEvent) event.event(),
                                                                                       event.attachments())));
    }

    @Override
    public Optional<List<EventWithAttachments>> kerl(Identifier prefix) throws TimeoutException {
        return kerl.kerl(prefix);
    }

    @Override
    public void publish(List<EventWithAttachments> k) {
        List<KeyEvent> events = new ArrayList<KeyEvent>();
        List<AttachmentEvent> attachments = new ArrayList<AttachmentEvent>();
        k.forEach(e -> {
            events.add(e.event());
            attachments.add(ProtobufEventFactory.INSTANCE.attachment((EstablishmentEvent) e.event(), e.attachments()));
        });
        kerl.append(events, attachments);
    }

    @Override
    public Optional<KeyState> resolve(EventCoordinates coordinates) throws TimeoutException {
        return kerl.getKeyState(coordinates);
    }

    @Override
    public Optional<KeyState> resolve(Identifier prefix) throws TimeoutException {
        return kerl.getKeyState(prefix);
    }

    @Override
    public CompletableFuture<KeyState> appendWithReturn(KeyEvent event) {
        return kerl.append(event);
    }

    @Override
    public CompletableFuture<List<KeyState>> publishWithReturn(List<EventWithAttachments> k) {
        return kerl.append(k.stream().map(ewa -> ewa.event()).toList(),
                           k.stream()
                            .map(ewa -> ProtobufEventFactory.INSTANCE.attachment((EstablishmentEvent) ewa.event(),
                                                                                 ewa.attachments()))
                            .toList());
    }

}
