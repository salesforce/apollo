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

import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyStateWithAttachments_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
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
    public CompletableFuture<Attachment> getAttachment(EventCoords coordinates) {
        var fs = new CompletableFuture<Attachment>();
        try {
            fs.complete(kerl.getAttachment(EventCoordinates.from(coordinates))
                            .map(attch -> attch.toAttachemente())
                            .get());
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    public DigestAlgorithm getDigestAlgorithm() {
        return kerl.getDigestAlgorithm();
    }

    @Override
    public CompletableFuture<KERL_> getKERL(Ident identifier) {
        var fs = new CompletableFuture<KERL_>();
        try {
            fs.complete(kerl.kerl(Identifier.from(identifier)).map(kerl -> kerl(kerl)).get());
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<KeyEvent_> getKeyEvent(Digeste digest) {
        var fs = new CompletableFuture<KeyEvent_>();
        try {
            fs.complete(kerl.getKeyEvent(Digest.from(digest)).map(event -> event.toKeyEvent_()).get());
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates) {
        var fs = new CompletableFuture<KeyEvent_>();
        try {
            fs.complete(kerl.getKeyEvent(EventCoordinates.from(coordinates)).map(event -> event.toKeyEvent_()).get());
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<KeyState_> getKeyState(EventCoords coordinates) {
        var fs = new CompletableFuture<KeyState_>();
        try {
            fs.complete(kerl.getKeyState(EventCoordinates.from(coordinates)).map(ks -> ks.toKeyState_()).get());
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<KeyState_> getKeyState(Ident identifier) {
        var fs = new CompletableFuture<KeyState_>();
        try {
            fs.complete(kerl.getKeyState(Identifier.from(identifier)).map(ks -> ks.toKeyState_()).get());
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<KeyStateWithAttachments_> getKeyStateWithAttachments(EventCoords coords) {
        var fs = new CompletableFuture<KeyStateWithAttachments_>();
        try {
            fs.complete(kerl.getKeyStateWithAttachments(EventCoordinates.from(coords))
                            .map(ksa -> ksa.toEvente())
                            .get());
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    private KERL_ kerl(List<EventWithAttachments> k) {
        var builder = KERL_.newBuilder();
        k.forEach(ewa -> builder.addEvents(ewa.toKeyEvente()));
        return builder.build();
    }
}
