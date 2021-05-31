package com.salesforce.apollo.sterotomy.identifier;

import static com.salesforce.apollo.sterotomy.crypto.DigestAlgorithm.BLAKE2B_256;
import static com.salesforce.apollo.sterotomy.crypto.DigestAlgorithm.BLAKE2B_512;
import static com.salesforce.apollo.sterotomy.crypto.DigestAlgorithm.BLAKE2S_256;
import static com.salesforce.apollo.sterotomy.crypto.DigestAlgorithm.BLAKE3_256;
import static com.salesforce.apollo.sterotomy.crypto.DigestAlgorithm.BLAKE3_512;
import static com.salesforce.apollo.sterotomy.crypto.DigestAlgorithm.SHA2_256;
import static com.salesforce.apollo.sterotomy.crypto.DigestAlgorithm.SHA2_512;
import static com.salesforce.apollo.sterotomy.crypto.DigestAlgorithm.SHA3_256;
import static com.salesforce.apollo.sterotomy.crypto.DigestAlgorithm.SHA3_512;
import static com.salesforce.apollo.sterotomy.crypto.SignatureAlgorithm.EC_SECP256K1;
import static com.salesforce.apollo.sterotomy.crypto.SignatureAlgorithm.ED_25519;
import static com.salesforce.apollo.sterotomy.crypto.SignatureAlgorithm.ED_448;
import static com.salesforce.apollo.sterotomy.crypto.SignatureAlgorithm.lookup;

import java.security.PublicKey;
import java.util.Base64;

import org.bouncycastle.util.Arrays;

import com.salesforce.apollo.sterotomy.crypto.Digest;
import com.salesforce.apollo.sterotomy.crypto.DigestAlgorithm;
import com.salesforce.apollo.sterotomy.crypto.JohnHancock;
import com.salesforce.apollo.sterotomy.crypto.SignatureAlgorithm;

public final class QualifiedBase64 {

    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

    private static final char[] LOOKUP = { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
                                           'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
                                           'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
                                           't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7',
                                           '8', '9', '-', '_' };

    private static final int[] REVERSE_LOOKUP;

    static {
        REVERSE_LOOKUP = new int[128];
        Arrays.fill(REVERSE_LOOKUP, 0xff);
        for (var i = 0; i < LOOKUP.length; i++) {
            REVERSE_LOOKUP[LOOKUP[i]] = i;
        }
    }

    public static SignatureAlgorithm attachedSignatureAlgorithm(String code) {
        return switch (code.charAt(0)) {
        case 'A' -> SignatureAlgorithm.ED_25519;
        case 'B' -> SignatureAlgorithm.EC_SECP256K1;
        case '0' -> switch (code.charAt(1)) {
            case 'A' -> SignatureAlgorithm.EC_SECP256K1;
            default -> throw new IllegalArgumentException("unknown code: " + code);
            };
        default -> throw new IllegalArgumentException("unknown code: " + code);
        };
    }

    public static String attachedSignatureCode(SignatureAlgorithm algorithm, int index) {
        return switch (algorithm) {
        case ED_25519 -> "A" + base64(index, 1);
        case EC_SECP256K1 -> "B" + base64(index, 1);
        case ED_448 -> "0A" + base64(index, 2);
        };
    }

    public static String base64(byte[] b) {
        return ENCODER.encodeToString(b);
    }

    public static String base64(int i) {
        return base64(i, 0);
    }

    public static String base64(int i, int paddedLength) {
        var base64 = new StringBuilder(6);

        do {
            base64.append(LOOKUP[i % 64]);
            i /= 64;
            paddedLength--;
        } while (i > 0 || paddedLength > 0);

        return base64.reverse().toString();
    }

    public static int base64Length(int bytesLength) {
        var bits = bytesLength * 8;
        return bits / 6 + (bits % 6 != 0 ? 1 : 0);
    }

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

    public static Digest digest(String qb64) {
        if (qb64.startsWith("0")) {
            var bytes = unbase64(qb64.substring(2));
            return switch (qb64.substring(1, 2)) {
            case "D" -> new Digest(BLAKE3_512, bytes);
            case "E" -> new Digest(SHA3_512, bytes);
            case "F" -> new Digest(BLAKE2B_512, bytes);
            case "G" -> new Digest(SHA2_512, bytes);
            default -> throw new IllegalStateException("Unrecognized digest: " + qb64);
            };
        } else if (!qb64.matches("^[0-6-]")) {
            var bytes = unbase64(qb64.substring(1));
            return switch (qb64.substring(0, 1)) {
            case "E" -> new Digest(BLAKE3_256, bytes);
            case "F" -> new Digest(BLAKE2B_256, bytes);
            case "G" -> new Digest(BLAKE2S_256, bytes);
            case "H" -> new Digest(SHA3_256, bytes);
            case "I" -> new Digest(SHA2_256, bytes);
            default -> throw new IllegalStateException("Unrecognized digest: " + qb64);
            };
        } else {
            throw new IllegalStateException("Unrecognized digest: " + qb64);
        }
    }

    public static String digestCode(DigestAlgorithm algorithm) {
        return switch (algorithm) {
        case BLAKE2B_256 -> "F";
        case BLAKE2B_512 -> "0F";
        case BLAKE2S_256 -> "G";
        case BLAKE3_256 -> "E";
        case BLAKE3_512 -> "0D";
        case SHA2_256 -> "I";
        case SHA2_512 -> "0G";
        case SHA3_256 -> "H";
        case SHA3_512 -> "0E";
        default -> throw new IllegalArgumentException("Unexpected value: " + algorithm);
        };
    }

    public static Identifier identifier(String qb64) {
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

    public static String nonTransferrableIdentifierCode(SignatureAlgorithm a) {
        return switch (a) {
        case EC_SECP256K1 -> "1AAA";
        case ED_25519 -> "B";
        case ED_448 -> "1AAC";
        };
    }

    public static PublicKey publicKey(String qb64) {
        if (qb64.startsWith("1")) {
            var bytes = unbase64(qb64.substring(4));
            return switch (qb64.substring(1, 4)) {
            case "AAB" -> EC_SECP256K1.publicKey(bytes);
            case "AAD" -> ED_448.publicKey(bytes);
            default -> throw new IllegalStateException("Unrecognized public key: " + qb64);
            };
        } else if (!qb64.matches("^[0-6-]")) {
            var bytes = unbase64(qb64.substring(1));
            return switch (qb64.substring(0, 1)) {
            case "D" -> ED_25519.publicKey(bytes);
            default -> throw new IllegalStateException("Unrecognized public key: " + qb64);
            };
        } else {
            throw new IllegalStateException("Unrecognized public key: " + qb64);
        }
    }

    public static SignatureAlgorithm publicKeyAlgorithm(String code) {
        return switch (code) {
        case "1AAB" -> SignatureAlgorithm.EC_SECP256K1;
        case "D" -> SignatureAlgorithm.ED_25519;
        case "1AAD" -> SignatureAlgorithm.ED_448;
        default -> throw new IllegalArgumentException("unknown code: " + code);
        };
    }

    public static String publicKeyCode(SignatureAlgorithm a) {
        return switch (a) {
        case EC_SECP256K1 -> "1AAB";
        case ED_25519 -> "D";
        case ED_448 -> "1AAD";
        default -> throw new IllegalArgumentException("Unexpected value: " + a);
        };
    }

    public static String qb64(BasicIdentifier identifier) {
        var stdAlgo = SignatureAlgorithm.lookup(identifier.getPublicKey());
        var sigOps = lookup(identifier.getPublicKey());
        return nonTransferrableIdentifierCode(stdAlgo) + base64(sigOps.encode(identifier.getPublicKey()));
    }

    public static String qb64(Digest d) {
        return digestCode(d.getAlgorithm()) + base64(d.getBytes());
    }

    public static String qb64(Identifier identifier) {
        if (identifier instanceof BasicIdentifier) {
            return qb64((BasicIdentifier) identifier);
        } else if ((identifier instanceof SelfAddressingIdentifier)) {
            return qb64((SelfAddressingIdentifier) identifier);
        } else if (identifier instanceof SelfSigningIdentifier) {
            return qb64((SelfSigningIdentifier) identifier);
        }

        throw new IllegalStateException("Unrecognized identifier: " + identifier.getClass());
    }

    public static String qb64(JohnHancock s) {
        return signatureCode(s.getAlgorithm()) + base64(s.getBytes());
    }

    public static String qb64(PublicKey publicKey) {
        var stdAlgo = SignatureAlgorithm.lookup(publicKey);
        var sigOps = lookup(publicKey);
        return publicKeyCode(stdAlgo) + base64(sigOps.encode(publicKey));
    }

    public static String qb64(SelfAddressingIdentifier identifier) {
        return qb64(identifier.getDigest());
    }

    public static String qb64(SelfSigningIdentifier identifier) {
        return qb64(identifier.getSignature());
    }

    public static int qb64Length(int materialLength) {
        var bits = materialLength * 8;
        return bits / 6 + (bits % 6 != 0 ? 1 : 0) + (bits % 6 != 0 ? (6 - bits % 6) / 2 : 0)
        // if no padding, then we add 4 to accomodate code
                + (bits % 6 == 0 ? 4 : 0);
    }

    public static String selfAddressingIdentifierPlaceholder(DigestAlgorithm digestAlgorithm) {
        var placeholderLength = qb64Length(digestAlgorithm.digestLength());
        return "#".repeat(placeholderLength);
    }

    public static String selfSigningIdentifierPlaceholder(SignatureAlgorithm signatureAlgorithm) {
        var placeholderLength = qb64Length(signatureAlgorithm.signatureLength());
        return "#".repeat(placeholderLength);
    }

    public static JohnHancock signature(String qb64) {
        if (qb64.startsWith("0")) {
            var bytes = unbase64(qb64.substring(2));
            return switch (qb64.substring(1, 2)) {
            case "B" -> ED_25519.signature(bytes);
            case "C" -> EC_SECP256K1.signature(bytes);
            default -> throw new IllegalStateException("Unrecognized signature: " + qb64);
            };
        } else if (qb64.startsWith("1")) {
            var bytes = unbase64(qb64.substring(4));
            return switch (qb64.substring(1, 4)) {
            case "AAE" -> ED_448.signature(bytes);
            default -> throw new IllegalStateException("Unrecognized signature: " + qb64);
            };
        } else {
            throw new IllegalStateException("Unrecognized signature: " + qb64);
        }
    }

    public static SignatureAlgorithm signatureAlgorithm(String code) {
        return switch (code) {
        case "0B" -> SignatureAlgorithm.ED_25519;
        case "0C" -> SignatureAlgorithm.EC_SECP256K1;
        case "1AAE" -> SignatureAlgorithm.ED_448;
        default -> throw new IllegalArgumentException("unknown code: " + code);
        };
    }

    public static String signatureCode(SignatureAlgorithm algorithm) {
        return switch (algorithm) {
        case ED_25519 -> "0B";
        case EC_SECP256K1 -> "0C";
        case ED_448 -> "1AAE";
        };
    }

    public static String transferrableIdentifierCode(SignatureAlgorithm a) {
        return publicKeyCode(a);
    }

    public static byte[] unbase64(String base64) {
        return DECODER.decode(base64);
    }

    public static int unbase64Int(String base64) {
        var chars = base64.toCharArray();
        var result = 0;
        for (var i = 0; i < chars.length; i++) {
            result += REVERSE_LOOKUP[chars[i]] << (6 * (chars.length - i - 1));
        }
        return result;
    }

    private QualifiedBase64() {
        throw new IllegalStateException("Do not instantiate.");
    }

}
