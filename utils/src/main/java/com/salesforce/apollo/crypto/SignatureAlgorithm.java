/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import java.math.BigInteger;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.EdECPrivateKey;
import java.security.interfaces.EdECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPrivateKeySpec;
import java.security.spec.ECPublicKeySpec;
import java.security.spec.EdECPoint;
import java.security.spec.EdECPrivateKeySpec;
import java.security.spec.EdECPublicKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.security.spec.NamedParameterSpec;
import java.util.Arrays;

import org.bouncycastle.crypto.params.ECPublicKeyParameters;
import org.bouncycastle.jcajce.provider.asymmetric.util.ECUtil;
import org.bouncycastle.jce.ECPointUtil;

/**
 * 
 * Ye Enumeration of ye olde thyme Signature alorithms.
 * 
 * @author hal.hildebrand
 *
 */
public enum SignatureAlgorithm {

    EC_SECP256K1 {

        private final KeyFactory       keyFactory;
        private final KeyPairGenerator keyPairGenerator;
        private final ECParameterSpec  parameterSpec;
        {
            // secp256k1 is considered "unsecure" so you have enable it like this:
            System.setProperty("jdk.sunec.disableNative", "false");
            try {
                var ap = AlgorithmParameters.getInstance(ECDSA_ALGORITHM_NAME);
                ap.init(new ECGenParameterSpec(curveName()));
                parameterSpec = ap.getParameterSpec(ECParameterSpec.class);
                keyPairGenerator = KeyPairGenerator.getInstance(ECDSA_ALGORITHM_NAME);
                keyPairGenerator.initialize(parameterSpec);
                keyFactory = KeyFactory.getInstance(ECDSA_ALGORITHM_NAME);
            } catch (NoSuchAlgorithmException | InvalidParameterSpecException | InvalidAlgorithmParameterException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String algorithmName() {
            return ECDSA_ALGORITHM_NAME;
        }

        @Override
        public String curveName() {
            return "secp256k1";
        }

        @Override
        public byte[] encode(PublicKey publicKey) {
            try {
                var publicKeyParameter = (ECPublicKeyParameters) ECUtil.generatePublicKeyParameter(publicKey);
                return publicKeyParameter.getQ().getEncoded(true);
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException("Cannot encode public key", e);
            }
        }

        @Override
        public KeyPair generateKeyPair() {
            return keyPairGenerator.generateKeyPair();
        }

        @Override
        public KeyPair generateKeyPair(SecureRandom secureRandom) {
            try {
                var keyPairGenerator = KeyPairGenerator.getInstance(ECDSA_ALGORITHM_NAME);
                keyPairGenerator.initialize(parameterSpec, secureRandom);
                return keyPairGenerator.generateKeyPair();
            } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public PrivateKey privateKey(byte[] bytes) {
            try {
                var spec = new ECPrivateKeySpec(new BigInteger(1, bytes), parameterSpec);

                return keyFactory.generatePrivate(spec);
            } catch (InvalidKeySpecException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int privateKeyLength() {
            return 32;
        }

        @Override
        public PublicKey publicKey(byte[] bytes) {
            try {
                var ecPoint = ECPointUtil.decodePoint(parameterSpec.getCurve(), bytes);
                var spec = new ECPublicKeySpec(ecPoint, parameterSpec);

                return keyFactory.generatePublic(spec);
            } catch (GeneralSecurityException e) {
                throw new IllegalArgumentException("Cannot decode public key", e);
            }
        }

        @Override
        public int publicKeyLength() {
            return 33;
        }

        @Override
        public JohnHancock sign(byte[] message, PrivateKey privateKey) {
            try {
                var sig = java.security.Signature.getInstance(this.signatureInstanceName());
                sig.initSign(privateKey);
                sig.update(message);
                var bytes = sig.sign();

                return new JohnHancock(this, bytes);
            } catch (GeneralSecurityException e) {
                throw new IllegalArgumentException("Cannot sign", e);
            }
        }

        @Override
        public JohnHancock signature(byte[] signatureBytes) {
            return new JohnHancock(this, signatureBytes);
        }

        @Override
        public String signatureInstanceName() {
            return "SHA256" + ECDSA_SIGNATURE_ALGORITHM_SUFFIX;
        }

        @Override
        public int signatureLength() {
            return 64;
        }

        @Override
        public boolean verify(byte[] message, JohnHancock signature, PublicKey publicKey) {
            try {
                var sig = Signature.getInstance(signatureInstanceName());
                sig.initVerify(publicKey);
                sig.update(message);
                return sig.verify(signature.bytes);
            } catch (GeneralSecurityException e) {
                // TODO handle better
                throw new RuntimeException(e);
            }
        }
    },
    ED_25519 {
        private final EdDSAOperations ops = new EdDSAOperations(this);

        @Override
        public String algorithmName() {
            return ECDSA_ALGORITHM_NAME;
        }

        @Override
        public String curveName() {
            return "ed25519";
        }

        @Override
        public byte[] encode(PublicKey publicKey) {
            return ops.encode(publicKey);
        }

        @Override
        public KeyPair generateKeyPair() {
            return ops.generateKeyPair();
        }

        @Override
        public KeyPair generateKeyPair(SecureRandom secureRandom) {
            return ops.generateKeyPair(secureRandom);
        }

        @Override
        public PrivateKey privateKey(byte[] bytes) {
            return ops.privateKey(bytes);
        }

        @Override
        public int privateKeyLength() {
            return 32;
        }

        @Override
        public PublicKey publicKey(byte[] bytes) {
            return ops.publicKey(bytes);
        }

        @Override
        public int publicKeyLength() {
            return 32;
        }

        @Override
        public JohnHancock sign(byte[] message, PrivateKey privateKey) {
            return ops.sign(message, privateKey);
        }

        @Override
        public JohnHancock signature(byte[] signatureBytes) {
            return ops.signature(signatureBytes);
        }

        @Override
        public String signatureInstanceName() {
            return "ED25519";
        }

        @Override
        public int signatureLength() {
            return 64;
        }

        @Override
        public String toString() {
            return ops.toString();
        }

        @Override
        public boolean verify(byte[] message, JohnHancock signature, PublicKey publicKey) {
            return ops.verify(message, signature, publicKey);
        }
    },
    ED_448 {
        private final EdDSAOperations ops = new EdDSAOperations(this);

        @Override
        public String algorithmName() {
            return ECDSA_ALGORITHM_NAME;
        }

        @Override
        public String curveName() {
            return "ed448";
        }

        @Override
        public byte[] encode(PublicKey publicKey) {
            return ops.encode(publicKey);
        }

        @Override
        public KeyPair generateKeyPair() {
            return ops.generateKeyPair();
        }

        @Override
        public KeyPair generateKeyPair(SecureRandom secureRandom) {
            return ops.generateKeyPair(secureRandom);
        }

        @Override
        public PrivateKey privateKey(byte[] bytes) {
            return ops.privateKey(bytes);
        }

        @Override
        public int privateKeyLength() {
            return 56;
        }

        @Override
        public PublicKey publicKey(byte[] bytes) {
            return ops.publicKey(bytes);
        }

        @Override
        public int publicKeyLength() {
            return 57;
        }

        @Override
        public JohnHancock sign(byte[] message, PrivateKey privateKey) {
            return ops.sign(message, privateKey);
        }

        @Override
        public JohnHancock signature(byte[] signatureBytes) {
            return ops.signature(signatureBytes);
        }

        @Override
        public String signatureInstanceName() {
            return "ED448";
        }

        @Override
        public int signatureLength() {
            return 114;
        }

        @Override
        public String toString() {
            return ops.toString();
        }

        @Override
        public boolean verify(byte[] message, JohnHancock signature, PublicKey publicKey) {
            return ops.verify(message, signature, publicKey);
        }
    };

    public static class EdDSAOperations {

        public static final String EDDSA_ALGORITHM_NAME = "EdDSA";

        private static EdECPoint decodeEdPoint(byte[] in) {
            var arr = in.clone();
            var msb = arr[arr.length - 1];
            arr[arr.length - 1] &= (byte) 0x7F;
            var xOdd = (msb & 0x80) != 0;
            reverse(arr);
            var y = new BigInteger(1, arr);
            return new EdECPoint(xOdd, y);
        }

        private static void reverse(byte[] arr) {
            var i = 0;
            var j = arr.length - 1;

            while (i < j) {
                swap(arr, i, j);
                i++;
                j--;
            }
        }

        private static void swap(byte[] arr, int i, int j) {
            var tmp = arr[i];
            arr[i] = arr[j];
            arr[j] = tmp;
        }

        final KeyFactory         keyFactory;
        final KeyPairGenerator   keyPairGenerator;
        final NamedParameterSpec parameterSpec;
        final SignatureAlgorithm signatureAlgorithm;

        public EdDSAOperations(SignatureAlgorithm signatureAlgorithm) {
            try {
                this.signatureAlgorithm = signatureAlgorithm;

                var curveName = signatureAlgorithm.curveName().toLowerCase();
                parameterSpec = switch (curveName) {
                case "ed25519" -> NamedParameterSpec.ED25519;
                case "ed448" -> NamedParameterSpec.ED448;
                default -> throw new RuntimeException("Unknown Edwards curve: " + curveName);
                };

                keyPairGenerator = KeyPairGenerator.getInstance(EDDSA_ALGORITHM_NAME);
                keyPairGenerator.initialize(parameterSpec);
                keyFactory = KeyFactory.getInstance(EDDSA_ALGORITHM_NAME);
            } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
                throw new IllegalStateException("Unable to initialize", e);
            }
        }

        public byte[] encode(PublicKey publicKey) {
            var point = ((EdECPublicKey) publicKey).getPoint();
            var encodedPoint = point.getY().toByteArray();

            reverse(encodedPoint);
            encodedPoint = Arrays.copyOf(encodedPoint, publicKeyLength());
            var msb = (byte) (point.isXOdd() ? 0x80 : 0);
            encodedPoint[encodedPoint.length - 1] |= msb;

            return encodedPoint;
        }

        public KeyPair generateKeyPair() {
            return keyPairGenerator.generateKeyPair();
        }

        public KeyPair generateKeyPair(SecureRandom secureRandom) {
            try {
                var kpg = KeyPairGenerator.getInstance(EDDSA_ALGORITHM_NAME);
                kpg.initialize(parameterSpec, secureRandom);
                return kpg.generateKeyPair();
            } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
                throw new IllegalArgumentException("Cannot generate key pair", e);
            }
        }

        public PrivateKey privateKey(byte[] bytes) {
            try {

                return keyFactory.generatePrivate(new EdECPrivateKeySpec(parameterSpec, bytes));
            } catch (GeneralSecurityException e) {
                throw new IllegalArgumentException("Cannot decode private key", e);
            }
        }

        public PublicKey publicKey(byte[] bytes) {
            try {
                if (bytes.length != publicKeyLength()) {
                    throw new RuntimeException(new InvalidKeyException(
                            "key length is " + bytes.length + ", key length must be " + publicKeyLength()));
                }

                var point = decodeEdPoint(bytes);

                return keyFactory.generatePublic(new EdECPublicKeySpec(parameterSpec, point));
            } catch (GeneralSecurityException e) {
                throw new IllegalArgumentException("Cannot decode public key", e);
            }
        }

        public JohnHancock sign(byte[] message, PrivateKey privateKey) {
            try {
                var sig = Signature.getInstance(EDDSA_ALGORITHM_NAME);
                sig.initSign(privateKey);
                sig.update(message);
                var bytes = sig.sign();

                return new JohnHancock(signatureAlgorithm, bytes);
            } catch (GeneralSecurityException e) {
                throw new IllegalArgumentException("Cannot sign", e);
            }
        }

        public JohnHancock signature(byte[] signatureBytes) {
            return new JohnHancock(signatureAlgorithm, signatureBytes);
        }

        public boolean verify(byte[] message, JohnHancock signature, PublicKey publicKey) {
            try {
                var sig = Signature.getInstance(EDDSA_ALGORITHM_NAME);
                sig.initVerify(publicKey);
                sig.update(message);
                return sig.verify(signature.bytes);
            } catch (GeneralSecurityException e) {
                throw new IllegalArgumentException("Cannot verify", e);
            }
        }

        private int publicKeyLength() {
            return signatureAlgorithm.publicKeyLength();
        }

    }

    public static final SignatureAlgorithm DEFAULT = ED_25519;

    private static final String ECDSA_ALGORITHM_NAME             = "EC";
    private static final String ECDSA_SIGNATURE_ALGORITHM_SUFFIX = "withECDSA";
    @SuppressWarnings("unused")
    private static final String EDDSA_ALGORITHM_NAME             = "EdDSA";

    public static SignatureAlgorithm lookup(PrivateKey privateKey) {
        return switch (privateKey.getAlgorithm()) {
        case "EC" -> lookupEc(((ECPrivateKey) privateKey).getParams());
        case "EdDSA" -> lookupEd(((EdECPrivateKey) privateKey).getParams());
        case "Ed25519" -> ED_25519;
        case "Ed448" -> ED_448;
        default -> throw new IllegalArgumentException("Unknown algorithm: " + privateKey.getAlgorithm());
        };
    }

    public static SignatureAlgorithm lookup(PublicKey publicKey) {
        return switch (publicKey.getAlgorithm()) {
        case "EC" -> lookupEc(((ECPublicKey) publicKey).getParams());
        case "EdDSA" -> lookupEd(((EdECPublicKey) publicKey).getParams());
        case "Ed25519" -> ED_25519;
        case "Ed448" -> ED_448;
        default -> throw new IllegalArgumentException("Unknown algorithm: " + publicKey.getAlgorithm());
        };
    }

    private static SignatureAlgorithm lookupEc(ECParameterSpec params) {
        try {
            var algorithmParameters = AlgorithmParameters.getInstance("EC");
            algorithmParameters.init(params);
            var genParamSpec = algorithmParameters.getParameterSpec(ECGenParameterSpec.class);
            var curveName = genParamSpec.getName();
            return switch (curveName.toLowerCase()) {
            case "1.3.132.0.10":
            case "secp256k1":
                yield EC_SECP256K1;
            default:
                throw new IllegalArgumentException("Unknown EC curve: " + curveName);
            };
        } catch (NoSuchAlgorithmException | InvalidParameterSpecException e) {
            throw new IllegalStateException("EC algorithm or needed curves unavailable.", e);
        }
    }

    private static SignatureAlgorithm lookupEd(NamedParameterSpec params) {
        var curveName = params.getName();
        return switch (curveName.toLowerCase()) {
        case "ed25519" -> ED_25519;
        case "ed448" -> ED_448;
        default -> throw new IllegalArgumentException("Unknown edwards curve: " + curveName);
        };
    }

    abstract public String algorithmName();

    abstract public String curveName();

    abstract public byte[] encode(PublicKey publicKey);

    abstract public KeyPair generateKeyPair();

    abstract public KeyPair generateKeyPair(SecureRandom secureRandom);

    public KeyPair keyPair(byte[] bytes, byte[] publicKey) {
        return new KeyPair(publicKey(publicKey), privateKey(bytes));
    }

    abstract public PrivateKey privateKey(byte[] bytes);

    abstract public int privateKeyLength();

    abstract public PublicKey publicKey(byte[] bytes);

    abstract public int publicKeyLength();

    abstract public JohnHancock sign(byte[] message, PrivateKey privateKey);

    abstract public JohnHancock signature(byte[] signatureBytes);

    abstract public String signatureInstanceName();

    abstract public int signatureLength();

    abstract public boolean verify(byte[] message, JohnHancock signature, PublicKey publicKey);
}
