/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import org.joou.ULong;

/**
 * The EventValidation provides validation predicates for EstablishmentEvents
 *
 * @author hal.hildebrand
 */
public interface EventValidation {

    EventValidation NONE = new EventValidation() {

        @Override
        public KeyState keyState(Identifier id, ULong sequenceNumber) {
            return null;
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
        public KeyState keyState(Identifier id, ULong sequenceNumber) {
            return null;
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

    KeyState keyState(Identifier id, ULong sequenceNumber);

    /**
     * Answer true if the event indicated by the coordinates is validated. This means that thresholds have been met from
     * indicated witnesses and trusted validators.
     */
    boolean validate(EventCoordinates coordinates);

    /**
     * Answer true if the event is validated. This means that thresholds have been met from indicated witnesses and
     * trusted validators.
     */
    boolean validate(EstablishmentEvent event);
}
