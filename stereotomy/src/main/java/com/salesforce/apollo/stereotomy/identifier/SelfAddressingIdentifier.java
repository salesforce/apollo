/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;

import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public class SelfAddressingIdentifier implements Identifier {

    private static final ByteString IDENTIFIER = ByteString.copyFrom(new byte[] { 1 });
    private final Digest            digest;

    public SelfAddressingIdentifier(Digest digest) {
        this.digest = digest;
    }

    public SelfAddressingIdentifier(ByteBuffer buff) {
        this(digest(buff));
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
    public Ident toIdent() {
        return Ident.newBuilder().setSelfAddressing(digest.toDigeste()).build();
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
    public String toString() {
        return "SA[" + digest + "]";
    }

    @Override
    public ByteString toByteString() {
        return IDENTIFIER.concat(digest.toByteString());
    }
}
