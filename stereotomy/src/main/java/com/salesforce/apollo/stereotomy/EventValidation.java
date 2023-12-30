/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.SigningThreshold;
import com.salesforce.apollo.cryptography.Verifier.Filtered;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import org.joou.ULong;

import java.io.InputStream;
import java.util.Optional;

/**
 * The EventValidation provides validation predicates for EstablishmentEvents
 *
 * @author hal.hildebrand
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
        public Optional<KeyState> getKeyState(Identifier identifier, ULong seqNum) {
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
    };

    EventValidation NO_VALIDATION = new EventValidation() {
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
        public Optional<KeyState> getKeyState(Identifier identifier, ULong seqNum) {
            return Optional.empty();
        }

        @Override
        public boolean validate(EstablishmentEvent event) {
            return false;
        }

        @Override
        public boolean validate(EventCoordinates coordinates) {
            return false;
        }
    };

    Filtered filtered(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                      InputStream message);

    Optional<KeyState> getKeyState(EventCoordinates coordinates);

    Optional<KeyState> getKeyState(Identifier identifier, ULong seqNum);

    /**
     * Answer true if the event is validated. This means that thresholds have been met from indicated witnesses and
     * trusted validators.
     */
    boolean validate(EstablishmentEvent event);

    /**
     * Answer true if the event indicated by the coordinates is validated. This means that thresholds have been met from
     * indicated witnesses and trusted validators.
     */
    boolean validate(EventCoordinates coordinates);
}
