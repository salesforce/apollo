/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEventWithAttachments;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.KeyStateWithEndorsementsAndValidations;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * The Key Event Receipt Log
 * 
 * @author hal.hildebrand
 *
 */
public interface KERL extends KEL {

    record EventWithAttachments(KeyEvent event, Attachment attachments) {

        public KeyEventWithAttachments toKeyEvente() {
            var builder = KeyEventWithAttachments.newBuilder();
            event.setEventOf(builder);
            if (attachments != null) {
                builder.setAttachment(attachments.toAttachemente());
            }
            return builder.build();
        }

        public String toBase64() {
            var encoder = Base64.getUrlEncoder().withoutPadding();
            var attachBytes = attachments == null ? com.salesfoce.apollo.stereotomy.event.proto.Attachment.getDefaultInstance()
                                                                                                          .toByteArray()
                                                  : attachments.toAttachemente().toByteArray();
            var encoded = event.getIlk() + "|" + encoder.encodeToString(event.getBytes()) + "|"
            + encoder.encodeToString(attachBytes);
            return encoded;
        }

        static EventWithAttachments fromBase64(String encoded) {
            var decoder = Base64.getUrlDecoder();
            String[] split = encoded.split("\\|");
            Attachment attachment = null;
            if (split.length == 3) {
                try {
                    attachment = Attachment.of(com.salesfoce.apollo.stereotomy.event.proto.Attachment.parseFrom(decoder.decode(split[2])));
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException("Invalid attachment: " + encoded);
                }
            } else if (split.length != 2) {
                throw new IllegalArgumentException("Invalid encoding: " + encoded);
            }
            return new EventWithAttachments(ProtobufEventFactory.toKeyEvent(decoder.decode(split[1]), split[0]),
                                            attachment);
        }
    }

    CompletableFuture<Void> append(List<AttachmentEvent> events);

    CompletableFuture<Void> appendValidations(EventCoordinates coordinates, Map<Identifier, JohnHancock> validations);

    default CompletableFuture<KeyStateWithEndorsementsAndValidations> getKeyStateWithEndorsementsAndValidations(EventCoordinates coordinates) {
        return getKeyStateWithAttachments(coordinates).thenCombine(getValidations(coordinates), (ksa, validations) -> {
            return ksa == null ? null
                               : KeyStateWithEndorsementsAndValidations.create(ksa.state(),
                                                                               ksa.attachments().endorsements(),
                                                                               validations);
        });
    }

    CompletableFuture<Map<Identifier, JohnHancock>> getValidations(EventCoordinates coordinates);

    default CompletableFuture<List<EventWithAttachments>> kerl(Identifier identifier) {
        // TODO use a real DB query instead of this really expensive iterative lookup
        return getKeyState(identifier).thenApply(ks -> ks == null ? null : ks.getCoordinates())
                                      .thenCompose(c -> c == null ? complete(Collections.emptyList())
                                                                  : getKeyEvent(c).thenCompose(ks -> kerl(ks)));
    }

    private <T> CompletableFuture<T> complete(T value) {
        var fs = new CompletableFuture<T>();
        fs.complete(value);
        return fs;
    }

    private CompletableFuture<EventWithAttachments> completeKerl(EventCoordinates c,
                                                                 List<EventWithAttachments> result) {
        if (c == null) {
            var fs = new CompletableFuture<EventWithAttachments>();
            fs.complete(null);
            return fs;
        }
        return getAttachment(c).thenCombine(getKeyEvent(c), (a, e) -> {
            if (e == null) {
                return null;
            }
            result.add(new EventWithAttachments(e, a));
            return e.getPrevious();
        }).thenCompose(coords -> completeKerl(coords, result));
    }

    private CompletableFuture<List<EventWithAttachments>> kerl(KeyEvent event) {
        var fs = new CompletableFuture<List<EventWithAttachments>>();
        var result = new ArrayList<EventWithAttachments>();
        getAttachment(event.getCoordinates()).thenApply(a -> {
            result.add(new EventWithAttachments(event, a));
            return event.getPrevious();
        }).thenCompose(c -> completeKerl(c, result)).whenComplete((r, t) -> {
            if (t != null) {
                fs.completeExceptionally(t);
            } else {
                Collections.reverse(result);
                fs.complete(result);
            }
        });
        return fs;
    }
}
