/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
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
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import java.security.spec.NamedParameterSpec;

import org.bouncycastle.crypto.params.ECPublicKeyParameters;
import org.bouncycastle.jcajce.provider.asymmetric.util.ECUtil;
import org.bouncycastle.jce.ECPointUtil;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.Verifier.DefaultVerifier;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * 
 * Ye Enumeration of ye olde thyme Signature alorithms.
 * 
 * @author hal.hildebrand
 *
 */
public enum SignatureAlgorithm {

    NULL_SIGNATURE {

        @Override
        public String algorithmName() {
            return "Null Algorithm";
        }

        @Override
        public String curveName() {
            return "Null";
        }

        @Override
        public byte[] encode(PublicKey publicKey) {
            return new byte[0];
        }

        @Override
        public KeyPair generateKeyPair() {
            return null;
        }

        @Override
        public KeyPair generateKeyPair(SecureRandom secureRandom) {
            return null;
        }

        @Override
        public PrivateKey privateKey(byte[] bytes) {
            return null;
        }

        @Override
        public int privateKeyLength() {
            return 0;
        }

        @Override
        public PublicKey publicKey(byte[] bytes) {
            return null;
        }

        @Override
        public int publicKeyLength() {
            return 0;
        }

        @Override
        public JohnHancock signature(byte[] signatureBytes) {
            return new JohnHancock(NULL_SIGNATURE, signatureBytes);
        }

        @Override
        public byte signatureCode() {
            return 1;
        }

        @Override
        public String signatureInstanceName() {
            return "Null Algorithm";
        }

        @Override
        public int signatureLength() {
            return 64;
        }

        @Override
        protected boolean verify(PublicKey publicKey, byte[] signature, InputStream message) {
            return false;
        }

        @Override
        JohnHancock sign(PrivateKey[] privateKeys, InputStream message) {
            return new JohnHancock(NULL_SIGNATURE, new byte[64]);
        }

    },

    EC_SECP256K1 {

        private final KeyFactory       keyFactory;
        private final KeyPairGenerator keyPairGenerator;
        private final ECParameterSpec  parameterSpec;
        {
            try {
                var ap = AlgorithmParameters.getInstance(ECDSA_ALGORITHM_NAME, ProviderUtils.getProviderBC());
                ap.init(new ECGenParameterSpec(curveName()));
                parameterSpec = ap.getParameterSpec(ECParameterSpec.class);
                keyPairGenerator = KeyPairGenerator.getInstance(ECDSA_ALGORITHM_NAME, ProviderUtils.getProviderBC());
                keyPairGenerator.initialize(parameterSpec);
                keyFactory = KeyFactory.getInstance(ECDSA_ALGORITHM_NAME, ProviderUtils.getProviderBC());
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
                var keyPairGenerator = KeyPairGenerator.getInstance(ECDSA_ALGORITHM_NAME,
                                                                    ProviderUtils.getProviderBC());
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
        public JohnHancock sign(PrivateKey[] privateKeys, InputStream is) {
            byte[][] signatures = new byte[privateKeys.length][];
            try {
                int i = 0;
                for (PrivateKey privateKey : privateKeys) {
                    if (privateKey != null) {
                        var sig = Signature.getInstance(this.signatureInstanceName(), ProviderUtils.getProviderBC());
                        sig.initSign(privateKey);
                        byte[] buf = new byte[1024];
                        try {
                            for (int read = is.read(buf); read > 0; read = is.read(buf)) {
                                sig.update(buf, 0, read);
                            }
                        } catch (IOException e) {
                            throw new IllegalStateException("Io error", e);
                        }
                        signatures[i] = sig.sign();
                    } else {
                        signatures[i] = null;
                    }
                    i++;
                }
                return new JohnHancock(this, signatures);
            } catch (GeneralSecurityException e) {
                throw new IllegalArgumentException("Cannot sign", e);
            }
        }

        @Override
        public JohnHancock signature(byte[] signatureBytes) {
            return new JohnHancock(this, signatureBytes);
        }

        @Override
        public byte signatureCode() {
            return 2;
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
        protected boolean verify(PublicKey publicKey, final byte[] bytes, InputStream is) {
            try {
                var sig = Signature.getInstance(signatureInstanceName(), ProviderUtils.getProviderBC());
                sig.initVerify(publicKey);
                byte[] buf = new byte[1024];
                try {
                    for (int read = is.read(buf); read > 0; read = is.read(buf)) {
                        sig.update(buf, 0, read);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Io error", e);
                }
                return sig.verify(bytes);
            } catch (GeneralSecurityException e) {
                throw new IllegalArgumentException("Unable to verify", e);
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
        public JohnHancock sign(PrivateKey[] privateKeys, InputStream is) {
            return ops.sign(privateKeys, is);
        }

        @Override
        public JohnHancock signature(byte[] signatureBytes) {
            return ops.signature(signatureBytes);
        }

        @Override
        public byte signatureCode() {
            return 3;
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
        protected boolean verify(PublicKey publicKey, byte[] bytes, InputStream message) {
            return ops.verify(publicKey, bytes, message);
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
        public JohnHancock sign(PrivateKey[] privateKeys, InputStream is) {
            return ops.sign(privateKeys, is);
        }

        @Override
        public JohnHancock signature(byte[] signatureBytes) {
            return ops.signature(signatureBytes);
        }

        @Override
        public byte signatureCode() {
            return 4;
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
        protected boolean verify(PublicKey publicKey, byte[] bytes, InputStream message) {
            return ops.verify(publicKey, bytes, message);
        }

    };

    public static final SignatureAlgorithm DEFAULT = ED_25519;

    private static final String ECDSA_ALGORITHM_NAME             = "EC";
    private static final String ECDSA_SIGNATURE_ALGORITHM_SUFFIX = "withECDSA";
    @SuppressWarnings("unused")
    private static final String EDDSA_ALGORITHM_NAME             = "EdDSA";

    public static SignatureAlgorithm fromSignatureCode(int i) {
        return switch (i) {
        case 0:
            throw new IllegalArgumentException("Unknown signature code: " + i);
        case 1:
            yield NULL_SIGNATURE;
        case 2:
            yield EC_SECP256K1;
        case 3:
            yield ED_25519;
        case 4:
            yield ED_448;
        default:
            throw new IllegalArgumentException("Unknown signature code: " + i);
        };
    }

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
            var algorithmParameters = AlgorithmParameters.getInstance("EC", ProviderUtils.getProviderBC());
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

    final public JohnHancock sign(PrivateKey privateKey, byte[]... message) {
        return sign(new PrivateKey[] { privateKey }, BbBackedInputStream.aggregate(message));
    }

    final public JohnHancock sign(PrivateKey privateKey, ByteBuffer... buffers) {
        return sign(new PrivateKey[] { privateKey }, BbBackedInputStream.aggregate(buffers));
    }

    final public JohnHancock sign(PrivateKey privateKey, ByteString... buffers) {
        return sign(new PrivateKey[] { privateKey }, BbBackedInputStream.aggregate(buffers));
    }

    abstract public JohnHancock signature(byte[] signatureBytes);

    abstract public byte signatureCode();

    abstract public String signatureInstanceName();

    abstract public int signatureLength();

    final public boolean verify(PublicKey publicKey, JohnHancock signature, byte[]... message) {
        return verify(publicKey, signature, BbBackedInputStream.aggregate(message));
    }

    final public boolean verify(PublicKey publicKey, JohnHancock signature, ByteBuffer... message) {
        return verify(publicKey, signature, BbBackedInputStream.aggregate(message));
    }

    final public boolean verify(PublicKey publicKey, JohnHancock signature, ByteString... message) {
        return verify(publicKey, signature, BbBackedInputStream.aggregate(message));
    }

    abstract protected boolean verify(PublicKey publicKey, byte[] signature, InputStream message);

    final boolean verify(PublicKey publicKey, JohnHancock signature, InputStream message) {
        return new DefaultVerifier(new PublicKey[] { publicKey }).verify(SigningThreshold.unweighted(1), signature,
                                                                         message);
    }

    abstract JohnHancock sign(PrivateKey[] privateKeys, InputStream message);
}
