/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.nio.ByteBuffer;
import java.security.PublicKey;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.stereotomy.EventCoordinates;
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

    /**
     * Ordering by
     * 
     * <pre>
     * <coords.identifier, coords.sequenceNumber, coords.digest>
     * </pre>
     */
    static String coordinateOrdering(EventCoordinates coords) {
        return qb64(coords.getIdentifier()) + ':' + coords.getSequenceNumber() + ':' + qb64(coords.getDigest());
    }

    static String receiptDigestSuffix(EventCoordinates event, EventCoordinates signer) {
        return qb64(event.getDigest()) + ':' + qb64(signer.getDigest());
    }

    /**
     * Ordering by
     * 
     * <pre>
     * <event.identifier, signer.identifier, event.sequenceNumber, signer.sequenceNumber, event.digest, signer.digest>
     * </pre>
     */
    static String receiptOrdering(EventCoordinates event, EventCoordinates signer) {
        return receiptPrefix(event, signer) + receiptSequence(event, signer) + receiptDigestSuffix(event, signer);
    }

    static String receiptPrefix(EventCoordinates event, EventCoordinates signer) {
        return receiptPrefix(event.getIdentifier(), signer.getIdentifier());
    }

    static String receiptPrefix(Identifier forIdentifier, Identifier forIdentifier2) {
        return qb64(forIdentifier) + ':' + qb64(forIdentifier2) + '.';
    }

    static String receiptSequence(EventCoordinates event, EventCoordinates signer) {
        return Long.toString(event.getSequenceNumber()) + ':' + signer.getSequenceNumber() + '.';
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
