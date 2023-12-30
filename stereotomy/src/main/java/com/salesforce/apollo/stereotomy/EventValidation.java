/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;

/**
 * The EventValidation provides validation predicates for EstablishmentEvents
 *
 * @author hal.hildebrand
 */
public interface EventValidation {

    EventValidation NONE = new EventValidation() {

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
        public boolean validate(EstablishmentEvent event) {
            return false;
        }

        @Override
        public boolean validate(EventCoordinates coordinates) {
            return false;
        }
    };

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
