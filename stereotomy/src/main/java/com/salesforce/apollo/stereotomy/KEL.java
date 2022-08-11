/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.salesfoce.apollo.stereotomy.event.proto.KeyStateWithAttachments_;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * The Key Event Log
 * 
 * @author hal.hildebrand
 *
 */
public interface KEL {

    record KeyStateWithAttachments(KeyState state, Attachment attachments) {
        public KeyStateWithAttachments_ toEvente() {
            final var builder = KeyStateWithAttachments_.newBuilder().setState(state.toKeyState_());
            if (attachments != null) {
                builder.setAttachment(attachments.toAttachemente());
            }
            return builder.build();
        }

        public static KeyStateWithAttachments from(KeyStateWithAttachments_ ksa) {
            return new KeyStateWithAttachments(new KeyStateImpl(ksa.getState()), Attachment.of(ksa.getAttachment()));
        }
    }

    /**
     * Answer the Verifier using key state at the supplied key coordinates
     */
    default public CompletableFuture<Verifier> getVerifier(KeyCoordinates coordinates) {
        return getKeyState(coordinates.getEstablishmentEvent()).thenApply(ks -> new Verifier.DefaultVerifier(ks.getKeys()
                                                                                                               .get(coordinates.getKeyIndex())));
    }

    /**
     * Append the event. The event will be validated before inserted.
     */
    CompletableFuture<KeyState> append(KeyEvent event);

    /**
     * Append the list of events. The events will be validated before inserted.
     */
    default CompletableFuture<List<KeyState>> append(KeyEvent... event) {
        return append(Arrays.asList(event), Collections.emptyList());
    }

    /**
     * Append the list of events and attachments. The events will be validated
     * before inserted.
     */
    CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments);

    /**
     * Answer the Attachment for the coordinates
     */
    CompletableFuture<Attachment> getAttachment(EventCoordinates coordinates);

    /**
     * The digest algorithm used
     */
    DigestAlgorithm getDigestAlgorithm();

    /**
     * Answer the KeyEvent of the coordinates
     */
    CompletableFuture<KeyEvent> getKeyEvent(EventCoordinates coordinates);

    /**
     * Answer the KeyState of the coordinates
     */
    CompletableFuture<KeyState> getKeyState(EventCoordinates coordinates);

    /**
     * Answer the current KeyState of an identifier
     */
    CompletableFuture<KeyState> getKeyState(Identifier identifier);

    /**
     * Answer the combined KeyState and Attachment for this state
     * 
     * @param coordinates
     * @return the KeyStateWithAttachments for these coordinates
     */
    default CompletableFuture<KeyStateWithAttachments> getKeyStateWithAttachments(EventCoordinates coordinates) {
        var ks = new AtomicReference<KeyState>();
        return getKeyState(coordinates).thenApply(k -> {
            ks.set(k);
            return k;
        }).thenCompose(k -> getAttachment(coordinates)).thenApply(a -> new KeyStateWithAttachments(ks.get(), a));
    }
}
