/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.joou.ULong;

import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * Coordinates that precisely locate a unique event in the KEL
 *
 * @author hal.hildebrand
 */
public class EventCoordinates {

    public static EventCoordinates NONE = new EventCoordinates();
    private final Digest     digest;
    private final Identifier identifier;
    private final String     ilk;
    private final ULong      sequenceNumber;

    public EventCoordinates(EventCoords coordinates) {
        digest = Digest.from(coordinates.getDigest());
        ilk = coordinates.getIlk();
        identifier = Identifier.from(coordinates.getIdentifier());
        sequenceNumber = ULong.valueOf(coordinates.getSequenceNumber());
    }

    public EventCoordinates(Identifier identifier, ULong sequenceNumber, Digest digest, String ilk) {
        this.identifier = requireNonNull(identifier, "identifier");
        this.sequenceNumber = sequenceNumber;
        this.digest = requireNonNull(digest, "digest");
        this.ilk = ilk;
    }
    private EventCoordinates() {
        identifier = Identifier.NONE;
        digest = Digest.NONE;
        sequenceNumber = ULong.valueOf(-1);
        ilk = KeyEvent.NONE;
    }

    public static EventCoordinates from(EventCoords coordinates) {
        if (EventCoords.getDefaultInstance().equals(coordinates)) {
            return NONE;
        }
        return new EventCoordinates(coordinates);
    }

    public static EventCoordinates of(EventCoordinates event, Digest digest) {
        return new EventCoordinates(event.getIdentifier(), event.getSequenceNumber(), digest, event.getIlk());
    }

    public static EventCoordinates of(KeyEvent event) {
        requireNonNull(event, "event");
        var algorithm = event.getPrevious().equals(EventCoordinates.NONE) ? DigestAlgorithm.DEFAULT
                                                                          : event.getPrevious()
                                                                                 .getDigest()
                                                                                 .getAlgorithm();
        if (algorithm.equals(DigestAlgorithm.NONE)) {
            algorithm = DigestAlgorithm.DEFAULT;
        }
        return of(event, algorithm);
    }

    public static EventCoordinates of(KeyEvent event, Digest digest) {
        return new EventCoordinates(event.getIdentifier(), event.getSequenceNumber(), digest, event.getIlk());
    }

    public static EventCoordinates of(KeyEvent event, DigestAlgorithm algorithm) {
        requireNonNull(event, "event");
        requireNonNull(algorithm, "algorithm");
        var digest = event.hash(algorithm);
        return of(event, digest);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof EventCoordinates other) {
            return Objects.equals(sequenceNumber, other.sequenceNumber) && Objects.equals(ilk, other.ilk)
            && Objects.equals(identifier, other.identifier) && Objects.equals(digest, other.digest);
        }
        return false;
    }

    public Digest getDigest() {
        return this.digest;
    }

    public Identifier getIdentifier() {
        return this.identifier;
    }

    public String getIlk() {
        return ilk;
    }

    public ULong getSequenceNumber() {
        return this.sequenceNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(digest, identifier, sequenceNumber);
    }

    public EventCoords toEventCoords() {
        return EventCoords.newBuilder()
                          .setSequenceNumber(sequenceNumber.longValue())
                          .setIdentifier(identifier.toIdent())
                          .setIlk(ilk)
                          .setDigest(digest.toDigeste())
                          .build();
    }

    @Override
    public String toString() {
        return "[" + identifier + ":" + this.sequenceNumber + ":" + this.getDigest() + ":" + ilk + "]";
    }
}
