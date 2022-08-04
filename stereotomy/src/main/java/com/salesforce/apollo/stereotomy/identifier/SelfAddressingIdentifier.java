/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import java.util.Objects;

import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public class SelfAddressingIdentifier implements Identifier, Comparable<SelfAddressingIdentifier> {

    private final Digest digest;

    public SelfAddressingIdentifier(Digest digest) {
        this.digest = digest;
    }

    @Override
    public int compareTo(SelfAddressingIdentifier o) {
        return digest.compareTo(o.digest);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SelfAddressingIdentifier)) {
            return false;
        }
        SelfAddressingIdentifier other = (SelfAddressingIdentifier) obj;
        return Objects.equals(digest, other.digest);
    }

    public Digest getDigest() {
        return digest;
    }

    @Override
    public int hashCode() {
        return Objects.hash(digest);
    }

    @Override
    public byte identifierCode() {
        return 1;
    }

    @Override
    public boolean isTransferable() {
        return true;
    }

    @Override
    public Ident toIdent() {
        return Ident.newBuilder().setSelfAddressing(digest.toDigeste()).build();
    }

    @Override
    public String toString() {
        return "SA" + digest;
    }
}
