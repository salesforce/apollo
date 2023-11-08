/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.interfaces.EdECPrivateKey;
import java.security.interfaces.EdECPublicKey;
import java.security.spec.NamedParameterSpec;
import java.security.spec.XECPrivateKeySpec;
import java.security.spec.XECPublicKeySpec;

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

    ED_25519 {
        private final EdDSAOperations ops = new EdDSAOperations(this);

        @Override
        public String algorithmName() {
            return EDDSA_ALGORITHM_NAME;
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
            return 2;
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
        public PrivateKey toEncryption(PrivateKey edPrivateKey) {
            try {
                KeyPairGenerator kpg = KeyPairGenerator.getInstance(XDH);
                NamedParameterSpec paramSpec = new NamedParameterSpec(X25519);
                kpg.initialize(paramSpec);
                final var edECPrivateKey = (EdECPrivateKey) edPrivateKey;
                var preHash = DigestAlgorithm.SHA2_256.digest(edECPrivateKey.getBytes().get());
                var kf = KeyFactory.getInstance(XDH);
                var privSpec = new XECPrivateKeySpec(paramSpec, preHash.getBytes());
                return kf.generatePrivate(privSpec);
            } catch (Exception e) {
                throw new IllegalStateException("Unable to convert", e);
            }
        }

        @Override
        public PublicKey toEncryption(PublicKey edPublicKey) {
            try {
                KeyPairGenerator kpg = KeyPairGenerator.getInstance(XDH);
                NamedParameterSpec paramSpec = new NamedParameterSpec(X25519);
                kpg.initialize(paramSpec);
                var point = ((EdECPublicKey) edPublicKey).getPoint();
                var y = point.getY();
                var one = BigInteger.valueOf(1);
                var u = one.add(y).divide(one.subtract(y));
                var kf = KeyFactory.getInstance(XDH);
                var pubSpec = new XECPublicKeySpec(paramSpec, u);
                return kf.generatePublic(pubSpec);
            } catch (Exception e) {
                throw new IllegalStateException("Unable to convert", e);
            }
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
            return EDDSA_ALGORITHM_NAME;
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
            return 3;
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
        public PrivateKey toEncryption(PrivateKey edPrivateKey) {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public PublicKey toEncryption(PublicKey edPublicKey) {
            throw new UnsupportedOperationException("Not yet implemented");
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
        public PrivateKey toEncryption(PrivateKey edPrivateKey) {
            throw new UnsupportedOperationException("Not valid for NULL signature algorithm");
        }

        @Override
        public PublicKey toEncryption(PublicKey edPublicKey) {
            throw new UnsupportedOperationException("Not valid for NULL signature algorithm");
        }

        @Override
        protected boolean verify(PublicKey publicKey, byte[] signature, InputStream message) {
            return false;
        }

        @Override
        JohnHancock sign(PrivateKey[] privateKeys, InputStream message) {
            throw new UnsupportedOperationException("Not valid for NULL signature algorithm");
        }

    };

    public static final SignatureAlgorithm DEFAULT              = ED_25519;
    private static final String            EDDSA_ALGORITHM_NAME = "EdDSA";
    private static final String            X25519               = "X25519";
    private static final String            XDH                  = "XDH";

    public static SignatureAlgorithm fromSignatureCode(int i) {
        return switch (i) {
        case 0:
            throw new IllegalArgumentException("Unknown signature code: " + i);
        case 1:
            yield NULL_SIGNATURE;
        case 2:
            yield ED_25519;
        case 3:
            yield ED_448;
        default:
            throw new IllegalArgumentException("Unknown signature code: " + i);
        };
    }

    public static SignatureAlgorithm lookup(PrivateKey privateKey) {
        return switch (privateKey.getAlgorithm()) {
        case "EdDSA" -> lookupEd(((EdECPrivateKey) privateKey).getParams());
        case "Ed25519" -> ED_25519;
        case "Ed448" -> ED_448;
        default -> throw new IllegalArgumentException("Unknown algorithm: " + privateKey.getAlgorithm());
        };
    }

    public static SignatureAlgorithm lookup(PublicKey publicKey) {
        return switch (publicKey.getAlgorithm()) {
        case "EdDSA" -> lookupEd(((EdECPublicKey) publicKey).getParams());
        case "Ed25519" -> ED_25519;
        case "Ed448" -> ED_448;
        default -> throw new IllegalArgumentException("Unknown algorithm: " + publicKey.getAlgorithm());
        };
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

    /**
     * Convert the Ed* public/private signature keys into the equivalent X*
     * public/private encryption key. See
     * <a href="https://eprint.iacr.org/2021/509.pdf">On using the same key pair for
     * Ed25519 and an X25519 based KEM</a>.
     *
     * @param edKeyPair
     * @return
     */
    public KeyPair toEncryption(KeyPair edKeyPair) {
        return new KeyPair(toEncryption(edKeyPair.getPublic()), toEncryption(edKeyPair.getPrivate()));
    }

    abstract public PrivateKey toEncryption(PrivateKey edPrivateKey);

    abstract public PublicKey toEncryption(PublicKey edPublicKey);

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

    abstract JohnHancock sign(PrivateKey[] privateKeys, InputStream message);

    final boolean verify(PublicKey publicKey, JohnHancock signature, InputStream message) {
        return new DefaultVerifier(new PublicKey[] { publicKey }).verify(SigningThreshold.unweighted(1), signature,
                                                                         message);
    }
}
