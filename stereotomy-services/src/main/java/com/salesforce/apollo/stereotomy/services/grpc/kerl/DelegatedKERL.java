/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.kerl;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.salesforce.apollo.crypto.Digest;
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

/**
 * @author hal.hildebrand
 *
 */
public class DelegatedKERL implements KERL {
    private final DigestAlgorithm algorithm;
    private final KERLService     client;

    public DelegatedKERL(KERLService client, DigestAlgorithm algorithm) {
        this.client = client;
        this.algorithm = algorithm;
    }

    @Override
    public CompletableFuture<Void> append(AttachmentEvent event) {
        var builder = com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent.newBuilder();
        return client.append(Collections.emptyList(),
                             Collections.singletonList(builder.setCoordinates(event.coordinates().toEventCoords())
                                                              .setAttachment(event.attachments().toAttachemente())
                                                              .build()))
                     .thenApply(l -> null);
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        return client.append(Collections.singletonList(event.toKeyEvent_()))
                     .thenApply(l -> l.isEmpty() ? null : new KeyStateImpl(l.get(0)));
    }

    @Override
    public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        return client.append(events.stream().map(ke -> ke.toKeyEvent_()).toList(),
                             attachments.stream().map(ae -> ae.toEvent_()).toList())
                     .thenApply(l -> l.stream().map(ks -> new KeyStateImpl(ks)).map(ks -> (KeyState) ks).toList());
    }

    @Override
    public Optional<Attachment> getAttachment(EventCoordinates coordinates) {
        return client.getAttachment(coordinates.toEventCoords()).map(attch -> Attachment.of(attch));
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return algorithm;
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(Digest digest) {
        return client.getKeyEvent(digest.toDigeste()).map(event -> ProtobufEventFactory.from(event));
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        return client.getKeyEvent(coordinates.toEventCoords()).map(event -> ProtobufEventFactory.from(event));
    }

    @Override
    public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
        return client.getKeyState(coordinates.toEventCoords()).map(ks -> new KeyStateImpl(ks));
    }

    @Override
    public Optional<KeyState> getKeyState(Identifier identifier) {
        return client.getKeyState(identifier.toIdent()).map(ks -> new KeyStateImpl(ks));
    }

    @Override
    public Optional<List<EventWithAttachments>> kerl(Identifier identifier) {
        return client.getKERL(identifier.toIdent())
                     .map(k -> k.getEventsList().stream().map(kwa -> ProtobufEventFactory.from(kwa)).toList());
    }
}
