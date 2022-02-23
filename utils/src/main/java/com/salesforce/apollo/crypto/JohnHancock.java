/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import java.io.InputStream;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.utils.proto.Sig;
import com.salesforce.apollo.crypto.Verifier.Filtered;
import com.salesforce.apollo.utils.Hex;

/**
 * A signature
 * 
 * @author hal.hildebrand
 *
 */
public class JohnHancock {

    public static JohnHancock from(Sig signature) {
        return new JohnHancock(signature);
    }

    public static JohnHancock of(Sig signature) {
        return new JohnHancock(signature);
    }

    private final byte[][]           bytes;
    private final SignatureAlgorithm algorithm;

    public JohnHancock(Sig sig) {
        this.algorithm = SignatureAlgorithm.fromSignatureCode(sig.getCode());
        bytes = new byte[sig.getSignaturesCount()][];
        int i = 0;
        sig.getSignaturesList().forEach(bs -> bytes[i] = bs.toByteArray());
    }

    public JohnHancock(SignatureAlgorithm algorithm, byte[] bytes) {
        this(algorithm, new byte[][] { bytes });
    }

    public JohnHancock(SignatureAlgorithm algorithm, byte[][] bytes) {
        this.algorithm = algorithm;
        this.bytes = bytes;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof JohnHancock)) {
            return false;
        }
        JohnHancock other = (JohnHancock) obj;
        return algorithm == other.algorithm && Arrays.equals(bytes, other.bytes);
    }

    public SignatureAlgorithm getAlgorithm() {
        return algorithm;
    }

    public byte[][] getBytes() {
        return bytes;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bytes);
        result = prime * result + Objects.hash(algorithm);
        return result;
    }

    public Digest toDigest(DigestAlgorithm digestAlgorithm) {
        if (digestAlgorithm.digestLength() * 2 != algorithm.signatureLength()) {
            throw new IllegalArgumentException("Cannot convert to a hash, as digest and signature length are not compatible");
        }
        Digest combined = digestAlgorithm.getOrigin();
        for (byte[] segment : bytes) {
            combined = combined.xor(new Digest(digestAlgorithm,
                                               Arrays.copyOf(segment, digestAlgorithm.digestLength())));
            combined = combined.xor(new Digest(digestAlgorithm,
                                               Arrays.copyOfRange(segment, digestAlgorithm.digestLength(),
                                                                  segment.length)));
        }
        return combined;
    }

    public Sig toSig() {
        return Sig.newBuilder()
                  .setCode(algorithm.signatureCode())
                  .addAllSignatures(Arrays.asList(bytes).stream().map(b -> ByteString.copyFrom(b)).toList())
                  .build();
    }

    @Override
    public String toString() {
        return "Sig[" + (bytes.length == 0 ? "<null>"
                                           : (bytes.length == 1 ? Hex.hexSubString(bytes[0], 12)
                                                                : Arrays.asList(bytes)
                                                                        .stream()
                                                                        .map(e -> "|" + Hex.hexSubString(e, 12))
                                                                + ":" + algorithm.signatureCode()))
        + "]";
    }

    public boolean verify(SigningThreshold threshold, PublicKey[] keys, InputStream message) {
        if (keys.length != bytes.length) {
            throw new IllegalArgumentException(String.format("Have %s signatures and provided %s keys", bytes.length,
                                                             keys.length));
        }

        var verifiedSignatures = new ArrayList<Integer>();
        var keyIndex = 0;
        for (var signature : bytes) {

            var publicKey = keys[keyIndex];

            var ops = SignatureAlgorithm.lookup(publicKey);
            if (ops.verify(publicKey, signature, message)) {
                verifiedSignatures.add(keyIndex);
            }
        }

        int[] arrIndexes = verifiedSignatures.stream().mapToInt(i -> i.intValue()).toArray();
        return SigningThreshold.thresholdMet(threshold, arrIndexes);
    }

    public Filtered filter(SigningThreshold threshold, PublicKey[] keys, InputStream message) {
        if (keys.length != bytes.length) {
            throw new IllegalArgumentException(String.format("Have %s signatures and provided %s keys", bytes.length,
                                                             keys.length));
        }

        var verifiedSignatures = new ArrayList<Integer>();
        byte[][] filtered = new byte[bytes.length][];
        var keyIndex = 0;
        for (byte[] signature : bytes) {
            var publicKey = keys[keyIndex];
            var ops = SignatureAlgorithm.lookup(publicKey);
            if (ops.verify(publicKey, signature, message)) {
                verifiedSignatures.add(keyIndex);
                filtered[keyIndex] = signature;
            }
            keyIndex++;
        }

        int[] arrIndexes = verifiedSignatures.stream().mapToInt(i -> i.intValue()).toArray();
        return new Filtered(SigningThreshold.thresholdMet(threshold, arrIndexes), new JohnHancock(algorithm, filtered));
    }
}
