/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class KeyCoordinates {

    public static KeyCoordinates convert(KeyCoordinates coordinates) {
        if (coordinates instanceof KeyCoordinates) {
            return (KeyCoordinates) coordinates;
        }

        return new KeyCoordinates(coordinates.getEstablishmentEvent(), coordinates.getKeyIndex());
    }

    public static KeyCoordinates of(BasicIdentifier basicIdentifier) {
        EventCoordinates coordinates = EventCoordinates.of(basicIdentifier);
        return new KeyCoordinates(coordinates, 0);
    }

    public static KeyCoordinates of(EstablishmentEvent establishmentEvent, int keyIndex) {
        EventCoordinates coordinates = EventCoordinates.of(establishmentEvent);
        return new KeyCoordinates(coordinates, keyIndex);
    }

    private final EventCoordinates establishmentEvent;
    private final int              keyIndex;

    public KeyCoordinates(EventCoordinates establishmentEvent, int keyIndex) {
        if (keyIndex < 0) {
            throw new IllegalArgumentException("keyIndex must be >= 0");
        }

        this.establishmentEvent = requireNonNull(establishmentEvent, "establishmentEvent");
        this.keyIndex = keyIndex;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof KeyCoordinates)) {
            return false;
        }
        KeyCoordinates other = (KeyCoordinates) obj;
        return Objects.equals(establishmentEvent, other.establishmentEvent) && keyIndex == other.keyIndex;
    }

    public EventCoordinates getEstablishmentEvent() {
        return establishmentEvent;
    }

    public int getKeyIndex() {
        return keyIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(establishmentEvent, keyIndex);
    }

    @Override
    public String toString() {
        return this.establishmentEvent + ":" + this.keyIndex;
    }
}
