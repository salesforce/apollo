/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEventWithAttachments;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
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

    CompletableFuture<Void> append(AttachmentEvent event);

    Optional<List<EventWithAttachments>> kerl(Identifier identifier);

}
