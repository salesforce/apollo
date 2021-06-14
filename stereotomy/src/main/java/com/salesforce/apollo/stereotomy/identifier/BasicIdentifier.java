/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import static com.salesforce.apollo.crypto.QualifiedBase64.bs;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;

import java.nio.ByteBuffer;
import java.security.PublicKey;
import java.util.Objects;

import com.google.protobuf.ByteString;

/**
 * @author hal.hildebrand
 *
 */
public class BasicIdentifier implements Identifier {

    private static final ByteString IDENTIFIER = ByteString.copyFrom(new byte[2]);
    private final PublicKey         publicKey;

    public BasicIdentifier(ByteBuffer buff) {
        this(publicKey(buff));
    }

    public BasicIdentifier(PublicKey publicKey) {
        this.publicKey = publicKey;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BasicIdentifier)) {
            return false;
        }
        BasicIdentifier other = (BasicIdentifier) obj;
        return Objects.equals(publicKey, other.publicKey);
    }

    public PublicKey getPublicKey() {
        return publicKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(publicKey);
    }

    @Override
    public byte identifierCode() {
        return 2;
    }

    @Override
    public boolean isTransferable() {
        return false;
    }

    @Override
    public ByteString toByteString() {
        return IDENTIFIER.concat(bs(publicKey));
    }

    @Override
    public String toString() {
        return "B[" + publicKey + "]";
    }
}
