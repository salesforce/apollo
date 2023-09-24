/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesfoce.apollo.stereotomy.event.proto.KeyStateWithAttachments_;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The Key Event Log
 *
 * @author hal.hildebrand
 */
public interface KEL {

    /**
     * Answer the Verifier using key state at the supplied key coordinates
     *
     * @return
     */
    default Verifier.DefaultVerifier getVerifier(KeyCoordinates coordinates) {
        return new Verifier.DefaultVerifier(getKeyState(coordinates.getEstablishmentEvent()).getKeys()
                .get(coordinates.getKeyIndex()));
    }

    /**
     * Append the event. The event will be validated before inserted.
     */
    KeyState append(KeyEvent event);

    /**
     * Append the list of events. The events will be validated before inserted.
     */
    default List<KeyState> append(KeyEvent... event) {
        return append(Arrays.asList(event), Collections.emptyList());
    }

    /**
     * Append the list of events and attachments. The events will be validated
     * before inserted.
     */
    List<KeyState> append(List<KeyEvent> events, List<AttachmentEvent> attachments);

    /**
     * Answer the Attachment for the coordinates
     */
    Attachment getAttachment(EventCoordinates coordinates);

    /**
     * The digest algorithm used
     */
    DigestAlgorithm getDigestAlgorithm();

    /**
     * Answer the KeyEvent of the coordinates
     */
    KeyEvent getKeyEvent(EventCoordinates coordinates);

    /**
     * Answer the KeyState of the coordinates
     */
    KeyState getKeyState(EventCoordinates coordinates);

    /**
     * Answer the current KeyState of an identifier
     */
    KeyState getKeyState(Identifier identifier);

    /**
     * Answer the combined KeyState and Attachment for this state
     *
     * @param coordinates
     * @return the KeyStateWithAttachments for these coordinates
     */
    default KeyStateWithAttachments getKeyStateWithAttachments(EventCoordinates coordinates) {
        return new KeyStateWithAttachments(getKeyState(coordinates), getAttachment(coordinates));
    }

    record KeyStateWithAttachments(KeyState state, Attachment attachments) {
        public static KeyStateWithAttachments from(KeyStateWithAttachments_ ksa) {
            return new KeyStateWithAttachments(new KeyStateImpl(ksa.getState()), Attachment.of(ksa.getAttachment()));
        }

        public KeyStateWithAttachments_ toEvente() {
            final var builder = KeyStateWithAttachments_.newBuilder().setState(state.toKeyState_());
            if (attachments != null) {
                builder.setAttachment(attachments.toAttachemente());
            }
            return builder.build();
        }
    }
}
