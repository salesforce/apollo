/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.Verifier.DefaultVerifier;
import com.salesforce.apollo.utils.BbBackedInputStream;
import org.joou.ULong;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.interfaces.EdECPrivateKey;
import java.security.interfaces.EdECPublicKey;
import java.security.spec.NamedParameterSpec;

/**
 * Ye Enumeration of ye olde thyme Signature alorithms.
 *
 * @author hal.hildebrand
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
        public JohnHancock sign(ULong sequenceNumber, PrivateKey[] privateKeys, InputStream is) {
            return ops.sign(privateKeys, is, sequenceNumber);
        }

        @Override
        public JohnHancock signature(ULong sequenceNumber, byte[] signatureBytes) {
            return ops.signature(signatureBytes, sequenceNumber);
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
        public JohnHancock sign(ULong sequenceNumber, PrivateKey[] privateKeys, InputStream is) {
            return ops.sign(privateKeys, is, sequenceNumber);
        }

        @Override
        public JohnHancock signature(ULong sequenceNumber, byte[] signatureBytes) {
            return ops.signature(signatureBytes, sequenceNumber);
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
        public String toString() {
            return ops.toString();
        }

        @Override
        protected boolean verify(PublicKey publicKey, byte[] bytes, InputStream message) {
            return ops.verify(publicKey, bytes, message);
        }

    }, NULL_SIGNATURE {
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
        public JohnHancock signature(ULong sequenceNumber, byte[] signatureBytes) {
            return new JohnHancock(NULL_SIGNATURE, signatureBytes, sequenceNumber);
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
        JohnHancock sign(ULong sequenceNumber, PrivateKey[] privateKeys, InputStream message) {
            return new JohnHancock(NULL_SIGNATURE, new byte[64], sequenceNumber);
        }

    };

    public static final SignatureAlgorithm DEFAULT = ED_25519;

    private static final String EDDSA_ALGORITHM_NAME = "EdDSA";

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

    final public JohnHancock sign(ULong sequenceNumber, PrivateKey privateKey, byte[]... message) {
        return sign(sequenceNumber, new PrivateKey[] { privateKey }, BbBackedInputStream.aggregate(message));
    }

    final public JohnHancock sign(ULong sequenceNumber, PrivateKey privateKey, ByteBuffer... buffers) {
        return sign(sequenceNumber, new PrivateKey[] { privateKey }, BbBackedInputStream.aggregate(buffers));
    }

    final public JohnHancock sign(ULong sequenceNumber, PrivateKey privateKey, ByteString... buffers) {
        return sign(sequenceNumber, new PrivateKey[] { privateKey }, BbBackedInputStream.aggregate(buffers));
    }

    abstract public JohnHancock signature(ULong sequenceNumber, byte[] signatureBytes);

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

    abstract JohnHancock sign(ULong sequenceNumber, PrivateKey[] privateKeys, InputStream message);

    final boolean verify(PublicKey publicKey, JohnHancock signature, InputStream message) {
        return new DefaultVerifier(new PublicKey[] { publicKey }).verify(SigningThreshold.unweighted(1), signature,
                                                                         message);
    }
}
