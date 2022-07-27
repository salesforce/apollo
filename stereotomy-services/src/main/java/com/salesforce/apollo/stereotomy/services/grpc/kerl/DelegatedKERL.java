/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.kerl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;

/**
 * @author hal.hildebrand
 *
 */
public class DelegatedKERL implements KERL {
    private final DigestAlgorithm  algorithm;
    private final ProtoKERLService kerl;

    public DelegatedKERL(ProtoKERLService kerl, DigestAlgorithm algorithm) {
        this.kerl = kerl;
        this.algorithm = algorithm;
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        return kerl.append(Collections.singletonList(event.toKeyEvent_()))
                   .thenApply(l -> l.isEmpty() ? null : new KeyStateImpl(l.get(0)));
    }

    @Override
    public CompletableFuture<Void> append(List<AttachmentEvent> events) {
        return kerl.appendAttachments(events.stream().map(e -> e.toEvent_()).toList()).thenApply(l -> null);
    }

    @Override
    public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        return kerl.append(events.stream().map(ke -> ke.toKeyEvent_()).toList(),
                           attachments.stream().map(ae -> ae.toEvent_()).toList())
                   .thenApply(l -> l.stream().map(ks -> new KeyStateImpl(ks)).map(ks -> (KeyState) ks).toList());
    }

    @Override
    public CompletableFuture<Attachment> getAttachment(EventCoordinates coordinates) {
        return kerl.getAttachment(coordinates.toEventCoords()).thenApply(attch -> Attachment.of(attch));
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return algorithm;
    }

    @Override
    public CompletableFuture<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        return kerl.getKeyEvent(coordinates.toEventCoords()).thenApply(event -> ProtobufEventFactory.from(event));
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(EventCoordinates coordinates) {
        return kerl.getKeyState(coordinates.toEventCoords()).thenApply(ks -> new KeyStateImpl(ks));
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(Identifier identifier) {
        return kerl.getKeyState(identifier.toIdent()).thenApply(ks -> new KeyStateImpl(ks));
    }

    @Override
    public CompletableFuture<KeyStateWithAttachments> getKeyStateWithAttachments(EventCoordinates coordinates) {
        return kerl.getKeyStateWithAttachments(coordinates.toEventCoords())
                   .thenApply(ksa -> KeyStateWithAttachments.from(ksa));
    }

    @Override
    public CompletableFuture<List<EventWithAttachments>> kerl(Identifier identifier) {
        return kerl.getKERL(identifier.toIdent())
                   .thenApply(k -> k.getEventsList().stream().map(kwa -> ProtobufEventFactory.from(kwa)).toList());
    }
}
