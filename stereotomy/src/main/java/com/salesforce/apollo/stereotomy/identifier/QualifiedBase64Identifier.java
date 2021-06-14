/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import static com.salesforce.apollo.crypto.DigestAlgorithm.BLAKE2B_256;
import static com.salesforce.apollo.crypto.DigestAlgorithm.BLAKE2B_512;
import static com.salesforce.apollo.crypto.DigestAlgorithm.BLAKE2S_256;
import static com.salesforce.apollo.crypto.DigestAlgorithm.BLAKE3_256;
import static com.salesforce.apollo.crypto.DigestAlgorithm.BLAKE3_512;
import static com.salesforce.apollo.crypto.DigestAlgorithm.SHA2_256;
import static com.salesforce.apollo.crypto.DigestAlgorithm.SHA2_512;
import static com.salesforce.apollo.crypto.DigestAlgorithm.SHA3_256;
import static com.salesforce.apollo.crypto.DigestAlgorithm.SHA3_512;
import static com.salesforce.apollo.crypto.SignatureAlgorithm.EC_SECP256K1;
import static com.salesforce.apollo.crypto.SignatureAlgorithm.ED_25519;
import static com.salesforce.apollo.crypto.SignatureAlgorithm.ED_448;
import static com.salesforce.apollo.crypto.SignatureAlgorithm.lookup;

import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.QualifiedBase64;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.stereotomy.event.DelegatingEventCoordinates;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;

/**
 * @author hal.hildebrand
 *
 */
public class QualifiedBase64Identifier extends QualifiedBase64 {

    public static String basicIdentifierPlaceholder(SignatureAlgorithm signatureAlgorithm) {
        var placeholderLength = qb64Length(signatureAlgorithm.publicKeyLength());
        return "#".repeat(placeholderLength);
    }

    public static SignatureAlgorithm basicIdentifierSignatureAlgorithm(String code) {
        return switch (code) {
        case "1AAA" -> SignatureAlgorithm.EC_SECP256K1;
        case "1AAC" -> SignatureAlgorithm.ED_25519;
        case "B" -> SignatureAlgorithm.ED_448;
        default -> throw new IllegalArgumentException("unknown code: " + code);
        };
    }

    public static Identifier identifier(ByteBuffer buff) {
        return switch (buff.get()) {
        case 0 -> Identifier.NONE;
        case 1 -> new SelfSigningIdentifier(buff);
        case 2 -> new BasicIdentifier(buff);
        case 3 -> new SelfAddressingIdentifier(buff);
        default -> throw new IllegalArgumentException("Unexpected value: " + buff.get());
        };
    }

    public static Identifier identifier(ByteString bs) {
        return identifier(bs.asReadOnlyByteBuffer());
    }

    public static Identifier identifier(String qb64) {
        if (qb64.isEmpty()) {
            return Identifier.NONE;
        }
        if (qb64.startsWith("0")) {
            var bytes = unbase64(qb64.substring(2));
            return switch (qb64.substring(1, 2)) {
            // case "A" -> null; // Random seed or private key of length 128 bits
            case "B" -> new SelfSigningIdentifier(ED_25519.signature(bytes));
            case "C" -> new SelfSigningIdentifier(EC_SECP256K1.signature(bytes));
            case "D" -> new SelfAddressingIdentifier(new Digest(BLAKE3_512, bytes));
            case "E" -> new SelfAddressingIdentifier(new Digest(SHA3_512, bytes));
            case "F" -> new SelfAddressingIdentifier(new Digest(BLAKE2B_512, bytes));
            case "G" -> new SelfAddressingIdentifier(new Digest(SHA2_512, bytes));
            default -> throw new IllegalArgumentException("Unrecognized identifier: " + qb64);
            };
        } else if (qb64.startsWith("1")) {
            var bytes = unbase64(qb64.substring(4));
            return switch (qb64.substring(1, 4)) {
            case "AAA" -> new BasicIdentifier(EC_SECP256K1.publicKey(bytes));
            // case "AAB" -> null; // EC SECP256K1 public key
            case "AAC" -> new BasicIdentifier(ED_448.publicKey(bytes));
            // case "AAD" -> null; // Ed448 public key
            case "AAE" -> new SelfSigningIdentifier(ED_25519.signature(bytes));
            default -> throw new IllegalArgumentException("Unrecognized identifier: " + qb64);
            };
        } else if (!qb64.matches("^[0-6-]")) {
            var bytes = unbase64(qb64.substring(1));
            return switch (qb64.substring(0, 1)) {
            // case "A" -> null; // Random seed of Ed25519 private key of length 256 bits
            case "B" -> new BasicIdentifier(ED_25519.publicKey(bytes));
            // case "C" -> null; // X25519 public encryption key
            // case "D" -> null; // Ed25519 public signing verification key.
            case "E" -> new SelfAddressingIdentifier(new Digest(BLAKE3_256, bytes));
            case "F" -> new SelfAddressingIdentifier(new Digest(BLAKE2B_256, bytes));
            case "G" -> new SelfAddressingIdentifier(new Digest(BLAKE2S_256, bytes));
            case "H" -> new SelfAddressingIdentifier(new Digest(SHA3_256, bytes));
            case "I" -> new SelfAddressingIdentifier(new Digest(SHA2_256, bytes));
            default -> throw new IllegalArgumentException("Unrecognized identifier: " + qb64);
            };
        } else {
            throw new IllegalArgumentException("Unrecognized identifier: " + qb64);
        }
    }

    public static String identifierPlaceholder(Identifier identifier) {
        if (identifier instanceof BasicIdentifier) {
            var bp = (BasicIdentifier) identifier;
            var signatureAlgorithm = SignatureAlgorithm.lookup(bp.getPublicKey());
            return basicIdentifierPlaceholder(signatureAlgorithm);
        } else if (identifier instanceof SelfAddressingIdentifier) {
            var sap = (SelfAddressingIdentifier) identifier;
            return selfAddressingIdentifierPlaceholder(sap.getDigest().getAlgorithm());
        } else if (identifier instanceof SelfSigningIdentifier) {
            var ssp = (SelfSigningIdentifier) identifier;
            return selfSigningIdentifierPlaceholder(ssp.getSignature().getAlgorithm());
        } else {
            throw new IllegalArgumentException("unknown prefix type: " + identifier.getClass().getCanonicalName());
        }
    }

    public static String qb64(BasicIdentifier identifier) {
        var stdAlgo = SignatureAlgorithm.lookup(identifier.getPublicKey());
        var sigOps = lookup(identifier.getPublicKey());
        return nonTransferrableIdentifierCode(stdAlgo) + base64(sigOps.encode(identifier.getPublicKey()));
    }

    public static String qb64(Identifier identifier) {
        if (identifier instanceof BasicIdentifier) {
            return qb64((BasicIdentifier) identifier);
        } else if ((identifier instanceof SelfAddressingIdentifier)) {
            return qb64((SelfAddressingIdentifier) identifier);
        } else if (identifier instanceof SelfSigningIdentifier) {
            return qb64((SelfSigningIdentifier) identifier);
        } else if (identifier == Identifier.NONE) {
            return "";
        }

        throw new IllegalStateException("Unrecognized identifier: " + identifier.getClass());
    }

    public static String qb64(SelfAddressingIdentifier identifier) {
        return qb64(identifier.getDigest());
    }

    public static String qb64(SelfSigningIdentifier identifier) {
        return qb64(identifier.getSignature());
    }

    public static String selfAddressingIdentifierPlaceholder(DigestAlgorithm digestAlgorithm) {
        var placeholderLength = qb64Length(digestAlgorithm.digestLength());
        return "#".repeat(placeholderLength);
    }

    public static String selfSigningIdentifierPlaceholder(SignatureAlgorithm signatureAlgorithm) {
        var placeholderLength = qb64Length(signatureAlgorithm.signatureLength());
        return "#".repeat(placeholderLength);
    }

    public static String shortQb64(DelegatingEventCoordinates c) {
        var p = c.getPreviousEvent();
        return shortQb64(p.getIdentifier()) + ":" + p.getSequenceNumber() + ":" + shortQb64(p.getDigest());
    }

    public static String shortQb64(EventCoordinates c) {
        return shortQb64(c.getIdentifier()) + ":" + c.getSequenceNumber() + ":" + shortQb64(c.getDigest());
    }

    public static String shortQb64(Identifier identifier) {
        return qb64(identifier).substring(0, SHORTENED_LENGTH);
    }

    public static String transferrableIdentifierCode(SignatureAlgorithm a) {
        return publicKeyCode(a);
    }
}
