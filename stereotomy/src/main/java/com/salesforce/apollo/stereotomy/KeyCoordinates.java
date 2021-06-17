/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;

/**
 * @author hal.hildebrand
 *
 */
public class KeyCoordinates {

    public static KeyCoordinates of(EstablishmentEvent establishmentEvent, int keyIndex) {
        EventCoordinates coordinates = EventCoordinates.of(establishmentEvent);
        return new KeyCoordinates(coordinates, keyIndex);
    }

    private final EventCoordinates establishmentEvent;
    private final int              keyIndex;

    public KeyCoordinates(ByteBuffer buff) {
        this(EventCoordinates.from(buff), buff.getInt());
    }

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

    public ByteString toByteString() {
        ByteBuffer ki = ByteBuffer.allocate(2);
        ki.putInt(keyIndex);
        ki.flip();
        return establishmentEvent.toByteString().concat(ByteString.copyFrom(ki));
    }

    @Override
    public String toString() {
        return this.establishmentEvent + ":" + this.keyIndex;
    }
}
