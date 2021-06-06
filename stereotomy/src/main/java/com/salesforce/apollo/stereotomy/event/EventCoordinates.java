/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.shortQb64;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

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

    public static EventCoordinates of(Identifier identifier) {
        return new EventCoordinates(identifier, 0, Digest.NONE);
    }

    public static EventCoordinates of(EventCoordinates event, Digest digest) {
        return new EventCoordinates(event.getIdentifier(), event.getSequenceNumber(), digest);
    }

    public static EventCoordinates of(KeyEvent event) {
        requireNonNull(event, "event");
        var algorithm = event.getPrevious().equals(EventCoordinates.NONE) ? DigestAlgorithm.DEFAULT
                : event.getPrevious().getDigest().getAlgorithm();
        if (algorithm.equals(DigestAlgorithm.NONE)) {
            algorithm = DigestAlgorithm.DEFAULT;
        }
        return of(event, algorithm);
    }

    public static EventCoordinates of(KeyEvent event, Digest digest) {
        return new EventCoordinates(event.getIdentifier(), event.getSequenceNumber(), digest);
    }

    public static EventCoordinates of(KeyEvent event, DigestAlgorithm algorithm) {
        requireNonNull(event, "event");
        requireNonNull(algorithm, "algorithm");
        var digest = algorithm.digest(event.getBytes());
        return of(event, digest);
    }

    private final Digest     digest;
    private final Identifier identifier;

    private final long sequenceNumber;

    public EventCoordinates(BasicIdentifier identifier) {
        this(identifier, 0, Digest.NONE);
    }

    public EventCoordinates(EventCoordinates event, Digest digest) {
        this(event.getIdentifier(), event.getSequenceNumber(), digest);
    }

    private EventCoordinates() {
        identifier = Identifier.NONE;
        digest = Digest.NONE;
        sequenceNumber = -1;
    }

    public EventCoordinates(Identifier identifier, long sequenceNumber, Digest digest) {
        this.identifier = requireNonNull(identifier, "identifier");
        this.sequenceNumber = sequenceNumber;
        this.digest = requireNonNull(digest, "digest");
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
        return Objects.equals(digest, other.digest) && Objects.equals(identifier, other.identifier)
                && sequenceNumber == other.sequenceNumber;
    }

    public Digest getDigest() {
        return this.digest;
    }

    public Identifier getIdentifier() {
        return this.identifier;
    }

    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(digest, identifier, sequenceNumber);
    }

    @Override
    public String toString() {
        return "[" + shortQb64(identifier) + ":" + this.sequenceNumber + ":" + shortQb64(this.getDigest()) + "]";
    }
};
