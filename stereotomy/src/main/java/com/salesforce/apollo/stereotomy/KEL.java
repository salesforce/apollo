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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * The Key Event Log
 * 
 * @author hal.hildebrand
 *
 */
public interface KEL {

    /**
     * Answer the Verifier using key state at the supplied key coordinates
     */
    default public Optional<Verifier> getVerifier(KeyCoordinates coordinates) {
        var state = getKeyState(coordinates.getEstablishmentEvent());
        if (state.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new Verifier.DefaultVerifier(state.get().getKeys().get(coordinates.getKeyIndex())));
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
    Optional<Attachment> getAttachment(EventCoordinates coordinates);

    /**
     * The digest algorithm used
     */
    DigestAlgorithm getDigestAlgorithm();

    /**
     * Answer the KeyEvent that has the matching digest
     */
    Optional<KeyEvent> getKeyEvent(Digest digest);

    /**
     * Answer the KeyEvent of the coordinates
     */
    Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates);

    /**
     * Answer the KeyState of the coordinates
     */
    Optional<KeyState> getKeyState(EventCoordinates coordinates);

    /**
     * Answer the current KeyState of an identifier
     */
    Optional<KeyState> getKeyState(Identifier identifier);
}
