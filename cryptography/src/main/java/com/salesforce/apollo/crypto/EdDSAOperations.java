/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.joou.ULong;

import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.interfaces.EdECPublicKey;
import java.security.spec.EdECPrivateKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.NamedParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;

/**
 * common operations and state per algorithm.
 *
 * @author hal.hildebrand
 */
public class EdDSAOperations {

    public static final String EDDSA_ALGORITHM_NAME = "EdDSA";

    private static final ThreadLocal<Signature> SIGNATURE_CACHE = new ThreadLocal<>() {

        @Override
        protected Signature initialValue() {
            try {
                return Signature.getInstance(EDDSA_ALGORITHM_NAME);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("Unable to retrieve sig algo: " + EDDSA_ALGORITHM_NAME, e);
            }
        }
    };
    private final        ASN1ObjectIdentifier   curveId;
    private final        KeyFactory             keyFactory;
    private final        KeyPairGenerator       keyPairGenerator;
    private final        NamedParameterSpec     parameterSpec;
    private final        SignatureAlgorithm     signatureAlgorithm;

    public EdDSAOperations(SignatureAlgorithm signatureAlgorithm) {
        try {
            this.signatureAlgorithm = signatureAlgorithm;

            var curveName = signatureAlgorithm.curveName().toLowerCase();
            parameterSpec = switch (curveName) {
                case "ed25519" -> NamedParameterSpec.ED25519;
                case "ed448" -> NamedParameterSpec.ED448;
                default -> throw new RuntimeException("Unknown Edwards curve: " + curveName);
            };
            curveId = switch (curveName) {
                case "ed25519" -> EdECObjectIdentifiers.id_Ed25519;
                case "ed448" -> EdECObjectIdentifiers.id_Ed448;
                default -> throw new RuntimeException("Unknown Edwards curve: " + signatureAlgorithm);
            };

            keyPairGenerator = KeyPairGenerator.getInstance(EDDSA_ALGORITHM_NAME);
            keyPairGenerator.initialize(parameterSpec);
            keyFactory = KeyFactory.getInstance(EDDSA_ALGORITHM_NAME);
        } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
            throw new IllegalStateException("Unable to initialize", e);
        }
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
        var pubKeyInfo = new SubjectPublicKeyInfo(new AlgorithmIdentifier(curveId), bytes);
        X509EncodedKeySpec x509KeySpec;
        try {
            x509KeySpec = new X509EncodedKeySpec(pubKeyInfo.getEncoded());
        } catch (IOException e1) {
            throw new IllegalArgumentException(e1);
        }

        try {
            return keyFactory.generatePublic(x509KeySpec);
        } catch (InvalidKeySpecException e1) {
            throw new IllegalArgumentException(e1);
        }
    }

    public JohnHancock sign(PrivateKey[] privateKeys, InputStream is, ULong sequenceNumber) {
        byte[][] signatures = new byte[privateKeys.length][];
        try {
            int i = 0;
            var sig = SIGNATURE_CACHE.get();
            for (PrivateKey privateKey : privateKeys) {
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
                i++;
            }
            return new JohnHancock(signatureAlgorithm, signatures, sequenceNumber);
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("Cannot sign", e);
        }
    }

    public JohnHancock signature(byte[] signatureBytes, ULong sequenceNumber) {
        return new JohnHancock(signatureAlgorithm, signatureBytes, sequenceNumber);
    }

    public boolean verify(PublicKey publicKey, byte[] bytes, InputStream is) {
        try {
            var sig = SIGNATURE_CACHE.get();
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
            // TODO handle better
            throw new RuntimeException(e);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private int publicKeyLength() {
        return signatureAlgorithm.publicKeyLength();
    }

}
