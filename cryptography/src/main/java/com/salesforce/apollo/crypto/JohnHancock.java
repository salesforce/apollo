/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.cryptography.proto.Sig;
import com.salesforce.apollo.crypto.Verifier.Filtered;
import com.salesforce.apollo.utils.Hex;
import org.joou.ULong;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * A signature
 *
 * @author hal.hildebrand
 */
public class JohnHancock {

    private final ULong              sequenceNumber;
    private final SignatureAlgorithm algorithm;
    private final byte[][]           bytes;

    public JohnHancock(Sig sig) {
        this.algorithm = SignatureAlgorithm.fromSignatureCode(sig.getCode());
        bytes = new byte[sig.getSignaturesCount()][];
        sequenceNumber = ULong.valueOf(sig.getSequenceNumber());
        int i = 0;
        sig.getSignaturesList().forEach(bs -> bytes[i] = bs.toByteArray());
    }

    public JohnHancock(SignatureAlgorithm algorithm, byte[] bytes, ULong sequenceNumber) {
        this(algorithm, new byte[][] { bytes }, sequenceNumber);
    }

    public JohnHancock(SignatureAlgorithm algorithm, byte[][] bytes, ULong sequenceNumber) {
        this.algorithm = algorithm;
        this.bytes = bytes;
        this.sequenceNumber = sequenceNumber;
    }

    public static JohnHancock from(Sig signature) {
        return new JohnHancock(signature);
    }

    public static JohnHancock of(Sig signature) {
        return new JohnHancock(signature);
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

    public Filtered filter(SigningThreshold threshold, Map<Integer, PublicKey> keys, InputStream message) {
        var verifiedSignatures = new ArrayList<Integer>();
        byte[][] filtered = new byte[bytes.length][];
        var keyIndex = 0;
        for (byte[] signature : bytes) {
            var publicKey = keys.get(keyIndex);
            if (publicKey != null) {
                var ops = SignatureAlgorithm.lookup(publicKey);
                if (ops.verify(publicKey, signature, message)) {
                    verifiedSignatures.add(keyIndex);
                    filtered[keyIndex] = signature;
                }
            }
            keyIndex++;
        }

        int[] arrIndexes = verifiedSignatures.stream().mapToInt(i -> i.intValue()).toArray();
        return new Filtered(SigningThreshold.thresholdMet(threshold, arrIndexes), arrIndexes.length,
                            new JohnHancock(algorithm, filtered, sequenceNumber));
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

    public int signatureCount() {
        return bytes.length;
    }

    public Digest toDigest(DigestAlgorithm digestAlgorithm) {
        if (digestAlgorithm.digestLength() * 2 != algorithm.signatureLength()) {
            throw new IllegalArgumentException(
            "Cannot convert to a hash, as digest and signature length are not compatible");
        }
        Digest combined = digestAlgorithm.getOrigin();
        for (byte[] segment : bytes) {
            combined = combined.xor(
            new Digest(digestAlgorithm, Arrays.copyOf(segment, digestAlgorithm.digestLength())));
            combined = combined.xor(
            new Digest(digestAlgorithm, Arrays.copyOfRange(segment, digestAlgorithm.digestLength(), segment.length)));
        }
        return combined;
    }

    public Sig toSig() {
        return Sig.newBuilder()
                  .setCode(algorithm.signatureCode())
                  .setSequenceNumber(sequenceNumber.longValue())
                  .addAllSignatures(Arrays.asList(bytes).stream().map(b -> ByteString.copyFrom(b)).toList())
                  .build();
    }

    @Override
    public String toString() {
        return "Sig[" + Arrays.asList(bytes).stream().map(b -> Hex.hexSubString(b, 12)).toList() + ":"
        + algorithm.signatureCode() + "]";
    }

    public boolean verify(SigningThreshold threshold, Map<Integer, PublicKey> keys, InputStream input) {

        var message = new BufferedInputStream(input);
        message.mark(Integer.MAX_VALUE);
        var verifiedSignatures = new ArrayList<Integer>();
        var keyIndex = 0;
        for (var signature : bytes) {
            var publicKey = keys.get(keyIndex);
            if (publicKey != null) {
                try {
                    message.reset();
                } catch (IOException e) {
                    LoggerFactory.getLogger(JohnHancock.class).error("Cannot reset message input", e);
                    return false;
                }
                if (algorithm.verify(publicKey, signature, message)) {
                    verifiedSignatures.add(keyIndex);
                }
            }
            keyIndex++;
        }

        int[] arrIndexes = verifiedSignatures.stream().mapToInt(i -> i.intValue()).toArray();
        return SigningThreshold.thresholdMet(threshold, arrIndexes);
    }
}
