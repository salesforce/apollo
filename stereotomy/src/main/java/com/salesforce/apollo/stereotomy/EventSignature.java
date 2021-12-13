/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.Collections;
import java.util.Map;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;

public class EventSignature {

    private final EventCoordinates          event;
    private final EventCoordinates          keyEstablishmentEvent;
    private final Map<Integer, JohnHancock> signatures;

    public EventSignature(EventCoordinates event, EventCoordinates keyEstablishmentEvent,
                          Map<Integer, JohnHancock> signatures) {
        this.event = event;
        this.keyEstablishmentEvent = keyEstablishmentEvent;
        this.signatures = Collections.unmodifiableMap(signatures);
    }

    public EventCoordinates getEvent() {
        return this.event;
    }

    public EventCoordinates getKeyEstablishmentEvent() {
        return this.keyEstablishmentEvent;
    }

    public Map<Integer, JohnHancock> getSignatures() {
        return this.signatures;
    }

}