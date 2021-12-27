/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.nio.ByteBuffer;
import java.security.PublicKey;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;

/**
 * @author hal.hildebrand
 *
 */
public interface Identifier {
    final ByteString          EMPTY      = ByteString.copyFrom(new byte[] { 0 });
    Identifier                NONE       = new Identifier() {

                                             @Override
                                             public byte identifierCode() {
                                                 return 0;
                                             }

                                             @Override
                                             public boolean isNone() {
                                                 return true;
                                             }

                                             @Override
                                             public boolean isTransferable() {
                                                 return false;
                                             }

                                             @Override
                                             public Ident toIdent() {
                                                 return NONE_IDENT;
                                             }

                                             @Override
                                             public String toString() {
                                                 return "<NONE>";
                                             }
                                         };
    public static final Ident NONE_IDENT = Ident.newBuilder().setNONE(true).build();

    public static Identifier from(Ident identifier) {
        if (identifier.hasBasic()) {
            return new BasicIdentifier(identifier.getBasic());
        }
        if (identifier.hasSelfAddressing()) {
            return new SelfAddressingIdentifier(digest(identifier.getSelfAddressing()));
        }
        if (identifier.hasSelfSigning()) {
            return new SelfSigningIdentifier(signature(identifier.getSelfSigning()));
        }
        return Identifier.NONE;
    }

    public static Identifier identifier(IdentifierSpecification spec, ByteBuffer inceptionStatement) {
        var derivation = spec.getDerivation();
        if (derivation.isAssignableFrom(BasicIdentifier.class)) {
            return basic(spec.getKeys().get(0));
        } else if (derivation.isAssignableFrom(SelfAddressingIdentifier.class)) {
            return selfAddressing(inceptionStatement, spec.getSelfAddressingDigestAlgorithm());
        } else if (derivation.isAssignableFrom(SelfSigningIdentifier.class)) {
            return selfSigning(inceptionStatement, spec.getSigner());
        } else {
            throw new IllegalArgumentException("unknown prefix type: " + derivation.getCanonicalName());
        }
    }

    static BasicIdentifier basic(PublicKey key) {
        return new BasicIdentifier(key);
    }

    static SelfAddressingIdentifier selfAddressing(ByteBuffer inceptionStatement, DigestAlgorithm digestAlgorithm) {
        var digest = digestAlgorithm.digest(inceptionStatement);
        return new SelfAddressingIdentifier(digest);
    }

    static SelfSigningIdentifier selfSigning(ByteBuffer inceptionStatement, Signer signer) {
        var signature = signer.sign(inceptionStatement);
        return new SelfSigningIdentifier(signature);
    }

    @Override
    boolean equals(Object obj);

    @Override
    int hashCode();

    byte identifierCode();

    default boolean isNone() {
        return false;
    }

    boolean isTransferable();

    Ident toIdent();
}
