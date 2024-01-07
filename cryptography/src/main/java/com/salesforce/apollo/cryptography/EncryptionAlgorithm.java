package com.salesforce.apollo.cryptography;

import java.math.BigInteger;
import java.security.*;
import java.security.interfaces.EdECPrivateKey;
import java.security.interfaces.EdECPublicKey;
import java.security.interfaces.XECPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.NamedParameterSpec;
import java.security.spec.XECPublicKeySpec;

public enum EncryptionAlgorithm {
    X_25519 {
        @Override
        public String algorithmName() {
            return "X25519";
        }

        @Override
        public String curveName() {
            return "Curve25519";
        }

        @Override
        public int publicKeyLength() {
            return 32;
        }
    }, X_448 {
        @Override
        public String algorithmName() {
            return "X448";
        }

        @Override
        public String curveName() {
            return "Curve448";
        }

        @Override
        public int publicKeyLength() {
            return 57;
        }
    };

    public static EncryptionAlgorithm lookup(PrivateKey privateKey) {
        return switch (privateKey.getAlgorithm()) {
            case "XDH" -> lookupX(((EdECPrivateKey) privateKey).getParams());
            case "x25519" -> X_25519;
            case "x448" -> X_448;
            default -> throw new IllegalArgumentException("Unknown algorithm: " + privateKey.getAlgorithm());
        };
    }

    public static EncryptionAlgorithm lookup(PublicKey publicKey) {
        return switch (publicKey.getAlgorithm()) {
            case "XDH" -> lookupX(((EdECPublicKey) publicKey).getParams());
            case "X25519" -> X_25519;
            case "X448" -> X_448;
            default -> throw new IllegalArgumentException("Unknown algorithm: " + publicKey.getAlgorithm());
        };
    }

    private static EncryptionAlgorithm lookupX(NamedParameterSpec params) {
        var curveName = params.getName();
        return switch (curveName.toLowerCase()) {
            case "x25519" -> X_25519;
            case "x448" -> X_448;
            default -> throw new IllegalArgumentException("Unknown edwards curve: " + curveName);
        };
    }

    abstract public String algorithmName();

    abstract public String curveName();

    final public byte[] encode(PublicKey publicKey) {
        return ((XECPublicKey) publicKey).getU().toByteArray();
    }

    final public KeyPair generateKeyPair() {
        try {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("XDH");
            kpg.initialize(getParamSpec());
            return kpg.generateKeyPair();
        } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
            throw new IllegalArgumentException("Cannot generate key pair", e);
        }
    }

    final public KeyPair generateKeyPair(SecureRandom secureRandom) {
        try {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("XDH");
            kpg.initialize(getParamSpec(), secureRandom);
            return kpg.generateKeyPair();
        } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
            throw new IllegalArgumentException("Cannot generate key pair", e);
        }
    }

    final public PublicKey publicKey(byte[] bytes) {
        try {
            KeyFactory kf = KeyFactory.getInstance("XDH");
            BigInteger u = new BigInteger(bytes);
            XECPublicKeySpec pubSpec = new XECPublicKeySpec(getParamSpec(), u);
            return kf.generatePublic(pubSpec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalArgumentException("Cannot create public key", e);
        }
    }

    abstract public int publicKeyLength();

    private NamedParameterSpec getParamSpec() {
        return new NamedParameterSpec(algorithmName());
    }
}
