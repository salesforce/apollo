/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import java.util.Map;
import java.util.Objects;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;

/**
 * @author hal.hildebrand
 *
 */
public class EventSignature {

    private final EventCoordinates          event;
    private final EventCoordinates          keyEstablishmentEvent;
    private final Map<Integer, JohnHancock> signatures;

    public EventSignature(EventCoordinates event, EventCoordinates keyEstablishmentEvent,
                          Map<Integer, JohnHancock> signatures) {
        this.event = event;
        this.keyEstablishmentEvent = keyEstablishmentEvent;
        this.signatures = Map.copyOf(signatures);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof EventSignature)) {
            return false;
        }
        EventSignature other = (EventSignature) obj;
        return Objects.equals(event, other.event) &&
               Objects.equals(keyEstablishmentEvent, other.keyEstablishmentEvent) &&
               Objects.equals(signatures, other.signatures);
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

    @Override
    public int hashCode() {
        return Objects.hash(event, keyEstablishmentEvent, signatures);
    }

    @Override
    public String toString() {
        return "ImmutableEventSignature [" + "event=" + this.event + ", " + "keyEstablishmentEvent="
        + this.keyEstablishmentEvent + ", " + "signatures=" + this.signatures + "]";
    }

}
