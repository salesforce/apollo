package com.salesforce.apollo.crypto;

import static com.salesforce.apollo.crypto.DigestAlgorithm.BLAKE2B_256;
import static com.salesforce.apollo.crypto.DigestAlgorithm.BLAKE2B_512;
import static com.salesforce.apollo.crypto.DigestAlgorithm.BLAKE2S_256;
import static com.salesforce.apollo.crypto.DigestAlgorithm.BLAKE3_256;
import static com.salesforce.apollo.crypto.DigestAlgorithm.BLAKE3_512;
import static com.salesforce.apollo.crypto.DigestAlgorithm.SHA2_256;
import static com.salesforce.apollo.crypto.DigestAlgorithm.SHA2_512;
import static com.salesforce.apollo.crypto.DigestAlgorithm.SHA3_256;
import static com.salesforce.apollo.crypto.DigestAlgorithm.SHA3_512;
import static com.salesforce.apollo.crypto.SignatureAlgorithm.ED_25519;
import static com.salesforce.apollo.crypto.SignatureAlgorithm.ED_448;

import java.security.PublicKey;
import java.util.Arrays;
import java.util.Base64;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.cryptography.proto.Digeste;
import com.salesfoce.apollo.cryptography.proto.PubKey;
import com.salesfoce.apollo.cryptography.proto.Sig;

/**
 * Qualifieed Base 64 KERI conversion for core crypto interop
 * 
 * @author hal.hildebrand
 *
 */
public class QualifiedBase64 {

    public final static int SHORTENED_LENGTH = 12;

    protected static final Base64.Decoder DECODER = Base64.getUrlDecoder();
    protected static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

    private static final char[] LOOKUP = { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
                                           'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
                                           'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
                                           't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7',
                                           '8', '9', '-', '_' };
    private static final int[]  REVERSE_LOOKUP;

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
        case '0' -> switch (code.charAt(1)) {
        case 'A' -> SignatureAlgorithm.ED_448;
        default -> throw new IllegalArgumentException("unknown code: " + code);
        };
        default -> throw new IllegalArgumentException("unknown code: " + code);
        };
    }

    public static String attachedSignatureCode(SignatureAlgorithm algorithm, int index) {
        return switch (algorithm) {
        case ED_25519 -> "A" + base64(index, 1);
        case ED_448 -> "0A" + base64(index, 2);
        case NULL_SIGNATURE -> throw new UnsupportedOperationException("Unimplemented case: " + algorithm);
        default -> throw new IllegalArgumentException("Unexpected value: " + algorithm);
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

    public static PubKey bs(PublicKey publicKey) {
        SignatureAlgorithm algo = SignatureAlgorithm.lookup(publicKey);
        return PubKey.newBuilder()
                     .setCode(algo.signatureCode())
                     .setEncoded(ByteString.copyFrom(algo.encode(publicKey)))
                     .build();
    }

    public static Digest digest(Digeste d) {
        return new Digest(d);
    }

    public static Digest digest(String qb64) {
        if (qb64.isEmpty()) {
            return Digest.NONE;
        }
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

    public static String nonTransferrableIdentifierCode(SignatureAlgorithm a) {
        return switch (a) {
        case ED_25519 -> "B";
        case ED_448 -> "1AAC";
        case NULL_SIGNATURE -> throw new UnsupportedOperationException("Unimplemented case: " + a);
        default -> throw new IllegalArgumentException("Unexpected value: " + a);
        };
    }

    public static PublicKey publicKey(PubKey pk) {
        var algo = SignatureAlgorithm.fromSignatureCode(pk.getCode());
        var bytes = pk.getEncoded().toByteArray();
        return algo.publicKey(bytes);
    }

    public static PublicKey publicKey(String qb64) {
        if (qb64.startsWith("1")) {
            var bytes = unbase64(qb64.substring(4));
            return switch (qb64.substring(1, 4)) {
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
        case "D" -> SignatureAlgorithm.ED_25519;
        case "1AAD" -> SignatureAlgorithm.ED_448;
        default -> throw new IllegalArgumentException("unknown code: " + code);
        };
    }

    public static String publicKeyCode(SignatureAlgorithm a) {
        return switch (a) {
        case ED_25519 -> "D";
        case ED_448 -> "1AAD";
        default -> throw new IllegalArgumentException("Unexpected value: " + a);
        };
    }

    public static String qb64(Digest d) {
        return Digest.NONE.equals(d) ? "" : digestCode(d.getAlgorithm()) + base64(d.getBytes());
    }

    public static String qb64(JohnHancock s) {
        final var bytes = s.getBytes();
        var builder = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != null) {
                builder.append(base64(bytes[i]));
            }
            if (i < bytes.length - 1) {
                builder.append(':');
            }
        }
        return signatureCode(s.getAlgorithm()) + builder.toString();
    }

    public static String qb64(PublicKey publicKey) {
        var stdAlgo = SignatureAlgorithm.lookup(publicKey);
        return publicKeyCode(stdAlgo) + base64(stdAlgo.encode(publicKey));
    }

    public static int qb64Length(int materialLength) {
        var bits = materialLength * 8;
        return bits / 6 + (bits % 6 != 0 ? 1 : 0) + (bits % 6 != 0 ? (6 - bits % 6) / 2 : 0)
        // if no padding, then we add 4 to accomodate code
        + (bits % 6 == 0 ? 4 : 0);
    }

    public static String shortQb64(Digest digest) {
        return qb64(digest).substring(0, SHORTENED_LENGTH);
    }

    public static String shortQb64(JohnHancock signature) {
        return qb64(signature).substring(0, SHORTENED_LENGTH);
    }

    public static String shortQb64(PublicKey publicKey) {
        return qb64(publicKey).substring(0, SHORTENED_LENGTH);
    }

    public static JohnHancock signature(Sig s) {
        return new JohnHancock(s);
    }

    public static JohnHancock signature(String qb64) {
        if (qb64.startsWith("0")) {
            var bytes = unbase64(qb64.substring(2));
            return switch (qb64.substring(1, 2)) {
            case "B" -> ED_25519.signature(bytes);
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
        case "1AAE" -> SignatureAlgorithm.ED_448;
        default -> throw new IllegalArgumentException("unknown code: " + code);
        };
    }

    public static String signatureCode(SignatureAlgorithm algorithm) {
        return switch (algorithm) {
        case ED_25519 -> "0B";
        case ED_448 -> "1AAE";
        case NULL_SIGNATURE -> throw new UnsupportedOperationException("Unimplemented case: " + algorithm);
        default -> throw new IllegalArgumentException("Unexpected value: " + algorithm);
        };
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

    protected QualifiedBase64() {
        throw new IllegalStateException("Do not instantiate.");
    }

}
