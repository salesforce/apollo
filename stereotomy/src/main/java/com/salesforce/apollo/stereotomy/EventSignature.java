/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.crypto.JohnHancock;

/**
 * Signature for an event by an identifier
 * 
 * @author hal.hildebrand
 *
 */
public class EventSignature {

    private final EventCoordinates event;
    private final EventCoordinates keyEstablishmentEvent;
    private final JohnHancock      signatures;

    public EventSignature(EventCoordinates event, EventCoordinates keyEstablishmentEvent, JohnHancock signatures) {
        this.event = event;
        this.keyEstablishmentEvent = keyEstablishmentEvent;
        this.signatures = signatures;
    }

    public EventCoordinates getEvent() {
        return this.event;
    }

    public EventCoordinates getKeyEstablishmentEvent() {
        return this.keyEstablishmentEvent;
    }

    public JohnHancock getSignatures() {
        return this.signatures;
    }

}
