/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.sterotomy.controller;

import static com.salesforce.apollo.sterotomy.identifier.QualifiedBase64.qb64;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.salesforce.apollo.sterotomy.crypto.Digest;
import com.salesforce.apollo.sterotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.sterotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class Coordinates {

    public static Coordinates NONE = new Coordinates(Identifier.NONE, -1, Digest.NONE);

    private final Digest     digest;
    private final Identifier identifier;
    private final long       sequenceNumber;

    public Coordinates(BasicIdentifier identifier) {
        this(identifier, 0, Digest.NONE);
    }

    public Coordinates(Coordinates event, Digest digest) {
        this(event.identifier(), event.sequenceNumber(), digest);
    }

    public Coordinates(Identifier identifier, long sequenceNumber, Digest digest) {
        if (sequenceNumber < 0) {
            throw new IllegalArgumentException("sequenceNumber must be >= 0");
        }

        this.identifier = requireNonNull(identifier, "identifier");
        this.sequenceNumber = sequenceNumber;

        if ((!(identifier instanceof BasicIdentifier) || sequenceNumber != 0) && Digest.NONE.equals(digest)) {
            // Digest isn't required for BasicIdentifiers or for inception events
            throw new IllegalArgumentException("digest is required");
        }

        this.digest = requireNonNull(digest, "digest");
    }

    public Digest digest() {
        return this.digest;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Coordinates)) {
            return false;
        }
        Coordinates other = (Coordinates) obj;
        return Objects.equals(digest, other.digest) && Objects.equals(identifier, other.identifier)
                && sequenceNumber == other.sequenceNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(digest, identifier, sequenceNumber);
    }

    public Identifier identifier() {
        return this.identifier;
    }

    public long sequenceNumber() {
        return this.sequenceNumber;
    }

    @Override
    public String toString() {
        return this.identifier + ":" + this.sequenceNumber + ":" + qb64(this.digest());
    }

}
