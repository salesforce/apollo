/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.crypto.JohnHancock;

/**
 * @author hal.hildebrand
 *
 */
public class SelfSigningIdentifier implements Identifier {
    private static final ByteString IDENTIFIER = ByteString.copyFrom(new byte[] { 3 });

    private final JohnHancock signature;

    public SelfSigningIdentifier(ByteBuffer buff) {
        this(signature(buff));
    }

    public SelfSigningIdentifier(JohnHancock signature) {
        this.signature = signature;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SelfSigningIdentifier)) {
            return false;
        }
        SelfSigningIdentifier other = (SelfSigningIdentifier) obj;
        return Objects.equals(signature, other.signature);
    }

    public JohnHancock getSignature() {
        return signature;
    }

    @Override
    public int hashCode() {
        return Objects.hash(signature);
    }

    @Override
    public byte identifierCode() {
        return 3;
    }

    @Override
    public boolean isTransferable() {
        return true;
    }

    @Override
    public ByteString toByteString() {
        return IDENTIFIER.concat(signature.toByteString());
    }

    @Override
    public Ident toIdent() {
        return Ident.newBuilder().setSelfSigning(signature.toSig()).build();
    }

    @Override
    public String toString() {
        return "SS[" + signature + "]";
    }

}
