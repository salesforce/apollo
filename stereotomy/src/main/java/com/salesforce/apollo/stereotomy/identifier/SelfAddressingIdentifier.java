/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.event.proto.Ident;

import java.util.Objects;

/**
 * @author hal.hildebrand
 */
public class SelfAddressingIdentifier implements Identifier, Comparable<SelfAddressingIdentifier> {

    private final Digest digest;

    public SelfAddressingIdentifier(Digest digest) {
        assert digest != null : "Digest cannot be null";
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
        if (!(obj instanceof SelfAddressingIdentifier other)) {
            return false;
        }
        return Objects.equals(digest, other.digest);
    }

    public Digest getDigest() {
        return digest;
    }

    @Override
    public Digest getDigest(DigestAlgorithm algo) {
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
