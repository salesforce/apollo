/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.io.InputStream;
import java.util.Optional;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier.Filtered;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * The EventValidation provides validation predicates for EstablishmentEvents
 * 
 * @author hal.hildebrand
 *
 */
public interface EventValidation {

    EventValidation NONE = new EventValidation() {
        @Override
        public Filtered filtered(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                                 InputStream message) {
            return null;
        }

        @Override
        public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
            return Optional.empty();
        }

        @Override
        public boolean validate(EstablishmentEvent event) {
            return true;
        }

        @Override
        public boolean validate(EventCoordinates coordinates) {
            return true;
        }

        @Override
        public boolean verify(EventCoordinates coordinates, JohnHancock signature, InputStream message) {
            return true;
        }

        @Override
        public boolean verify(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                              InputStream message) {
            return true;
        }
    };

    Filtered filtered(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                      InputStream message);

    Optional<KeyState> getKeyState(EventCoordinates coordinates);

    /**
     * Answer true if the event is validated. This means that thresholds have been
     * met from indicated witnesses and trusted validators.
     */
    boolean validate(EstablishmentEvent event);

    /**
     * Answer true if the event indicated by the coordinates is validated. This
     * means that thresholds have been met from indicated witnesses and trusted
     * validators.
     */
    boolean validate(EventCoordinates coordinates);

    default boolean verify(EventCoordinates coordinates, JohnHancock signature, ByteString byteString) {
        return verify(coordinates, signature, BbBackedInputStream.aggregate(byteString));
    }

    boolean verify(EventCoordinates coordinates, JohnHancock signature, InputStream message);

    boolean verify(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                   InputStream message);
}
