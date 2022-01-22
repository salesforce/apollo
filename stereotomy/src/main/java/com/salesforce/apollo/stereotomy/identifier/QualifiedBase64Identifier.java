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

import java.math.BigInteger;

import org.joou.ULong;

import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.QualifiedBase64;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KeyCoordinates;

/**
 * QB64 DSL for identifier conversion
 * 
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

    public static EventCoordinates eventCoordinates(String qb64) {
        String[] split = qb64.split(":");
        if (split.length != 4) {
            throw new IllegalArgumentException("Invalid event coordinates: " + qb64);
        }
        return new EventCoordinates(identifier(split[0]), ULong.valueOf(new BigInteger(split[1])), digest(split[2]),
                                    split[3]);
    }

    public static Identifier identifier(Ident identifier) {
        return Identifier.from(identifier);
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

    public static KeyCoordinates keyCoordinates(String qb64) {
        int last = qb64.lastIndexOf(':');
        if (last <= 0) {
            throw new IllegalArgumentException("Invalid key coordinates: " + qb64);
        }
        EventCoordinates coordinates = eventCoordinates(qb64.substring(0, last));

        int index = Integer.parseInt(qb64.substring(last + 1));
        return new KeyCoordinates(coordinates, index);
    }

    public static String qb64(BasicIdentifier identifier) {
        var stdAlgo = SignatureAlgorithm.lookup(identifier.getPublicKey());
        var sigOps = lookup(identifier.getPublicKey());
        return nonTransferrableIdentifierCode(stdAlgo) + base64(sigOps.encode(identifier.getPublicKey()));
    }

    /**
     * identifier:sequence number:digest:ilk
     */
    public static String qb64(EventCoordinates c) {
        return qb64(c.getIdentifier()) + ":" + c.getSequenceNumber() + ":" + qb64(c.getDigest()) + ":" + c.getIlk();
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

    /**
     * identifier:sequence number:digest:ilk:keyIndex
     */
    public static String qb64(KeyCoordinates c) {
        return qb64(c.getEstablishmentEvent()) + ":" + c.getKeyIndex();
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

    public static String shortQb64(EventCoordinates c) {
        return shortQb64(c.getIdentifier()) + ":" + c.getSequenceNumber() + ":" + shortQb64(c.getDigest()) + c.getIlk();
    }

    public static String shortQb64(Identifier identifier) {
        return identifier == Identifier.NONE ? "<none>" : qb64(identifier).substring(0, SHORTENED_LENGTH);
    }

    public static String shortQb64(KeyCoordinates c) {
        var coordinates = c.getEstablishmentEvent();
        return shortQb64(coordinates.getIdentifier()) + ":" + coordinates.getSequenceNumber() + ":"
        + shortQb64(coordinates.getDigest()) + ":" + c.getKeyIndex();
    }

    public static String transferrableIdentifierCode(SignatureAlgorithm a) {
        return publicKeyCode(a);
    }
}
