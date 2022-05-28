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
import java.util.concurrent.ExecutionException;

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
        try {
            return Optional.ofNullable(client.getAttachment(coordinates.toEventCoords())
                                             .thenApply(attch -> Attachment.of(attch))
                                             .get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return algorithm;
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(Digest digest) {
        try {
            return Optional.ofNullable(client.getKeyEvent(digest.toDigeste())
                                             .thenApply(event -> ProtobufEventFactory.from(event))
                                             .get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        try {
            return Optional.ofNullable(client.getKeyEvent(coordinates.toEventCoords())
                                             .thenApply(event -> ProtobufEventFactory.from(event))
                                             .get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
        try {
            return Optional.ofNullable(client.getKeyState(coordinates.toEventCoords())
                                             .thenApply(ks -> new KeyStateImpl(ks))
                                             .get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public Optional<KeyState> getKeyState(Identifier identifier) {
        try {
            return Optional.ofNullable(client.getKeyState(identifier.toIdent())
                                             .thenApply(ks -> new KeyStateImpl(ks))
                                             .get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public Optional<KeyStateWithAttachments> getKeyStateWithAttachments(EventCoordinates coordinates) {
        try {
            return Optional.ofNullable(client.getKeyStateWithAttachments(coordinates.toEventCoords())
                                             .thenApply(ksa -> KeyStateWithAttachments.from(ksa))
                                             .get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public Optional<List<EventWithAttachments>> kerl(Identifier identifier) {
        try {
            return Optional.ofNullable(client.getKERL(identifier.toIdent())
                                             .thenApply(k -> k.getEventsList()
                                                              .stream()
                                                              .map(kwa -> ProtobufEventFactory.from(kwa))
                                                              .toList())
                                             .get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }
}
