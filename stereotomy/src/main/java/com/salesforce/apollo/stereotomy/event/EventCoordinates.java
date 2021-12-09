/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import static com.salesforce.apollo.stereotomy.event.KeyEvent.INCEPTION_TYPE;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class EventCoordinates {

    public static EventCoordinates NONE = new EventCoordinates();

    public static EventCoordinates from(EventCoords coordinates) {
        if (EventCoords.getDefaultInstance().equals(coordinates)) {
            return NONE;
        }
        return new EventCoordinates(coordinates);
    }

    public static EventCoordinates of(EventCoordinates event, Digest digest) {
        return new EventCoordinates(event.getIlk(), event.getIdentifier(), digest, event.getSequenceNumber());
    }

    public static EventCoordinates of(Identifier identifier) {
        return new EventCoordinates(INCEPTION_TYPE, identifier, Digest.NONE, 0);
    }

    public static EventCoordinates of(KeyEvent event) {
        requireNonNull(event, "event");
        var algorithm = event.getPrevious().equals(EventCoordinates.NONE) ? DigestAlgorithm.DEFAULT
                                                                          : event.getPrevious().getDigest()
                                                                                 .getAlgorithm();
        if (algorithm.equals(DigestAlgorithm.NONE)) {
            algorithm = DigestAlgorithm.DEFAULT;
        }
        return of(event, algorithm);
    }

    public static EventCoordinates of(KeyEvent event, Digest digest) {
        return new EventCoordinates(event.getIlk(), event.getIdentifier(), digest, event.getSequenceNumber());
    }

    public static EventCoordinates of(KeyEvent event, DigestAlgorithm algorithm) {
        requireNonNull(event, "event");
        requireNonNull(algorithm, "algorithm");
        var digest = event.hash(algorithm);
        return of(event, digest);
    }

    private final Digest digest;

    private final Identifier identifier;

    private final String ilk;

    private final long sequenceNumber;

    public EventCoordinates(EventCoordinates event, Digest digest) {
        this(event.getIlk(), event.getIdentifier(), digest, event.getSequenceNumber());
    }

    public EventCoordinates(EventCoords coordinates) {
        digest = Digest.from(coordinates.getDigest());
        ilk = coordinates.getIlk();
        identifier = Identifier.from(coordinates.getIdentifier());
        sequenceNumber = coordinates.getSequenceNumber();
    }

    public EventCoordinates(String ilk, BasicIdentifier identifier) {
        this(ilk, identifier, Digest.NONE, 0);
    }

    public EventCoordinates(String ilk, Identifier identifier, Digest digest, long sequenceNumber) {
        this.identifier = requireNonNull(identifier, "identifier");
        this.sequenceNumber = sequenceNumber;
        this.digest = requireNonNull(digest, "digest");
        this.ilk = ilk;
    }

    private EventCoordinates() {
        identifier = Identifier.NONE;
        digest = Digest.NONE;
        sequenceNumber = -1;
        ilk = KeyEvent.NONE;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof EventCoordinates)) {
            return false;
        }
        EventCoordinates other = (EventCoordinates) obj;
        return Objects.equals(digest, other.digest) && Objects.equals(identifier, other.identifier) &&
               sequenceNumber == other.sequenceNumber;
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

    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(digest, identifier, sequenceNumber);
    }

    public EventCoords toEventCoords() {
        return EventCoords.newBuilder().setSequenceNumber(sequenceNumber).setIdentifier(identifier.toIdent())
                          .setIlk(ilk).setDigest(digest.toDigeste()).build();
    }

    @Override
    public String toString() {
        return "[" + identifier + ":" + this.sequenceNumber + ":" + this.getDigest() + ":" + ilk + "]";
    }
};
