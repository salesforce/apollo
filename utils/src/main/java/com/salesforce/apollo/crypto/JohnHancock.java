/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import org.bouncycastle.util.encoders.Hex;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * A signature
 * 
 * @author hal.hildebrand
 *
 */
public class JohnHancock {
    public static JohnHancock from(ByteString bs) {
        return new JohnHancock(bs);
    }

    final byte[]                     bytes;
    private final SignatureAlgorithm algorithm;

    public JohnHancock(ByteBuffer buff) {
        this.algorithm = SignatureAlgorithm.fromSignatureCode(buff.get());
        bytes = new byte[algorithm.signatureLength()];
        buff.get(bytes);
    }

    public JohnHancock(ByteString bs) {
        this(bs.asReadOnlyByteBuffer());
    }

    public JohnHancock(SignatureAlgorithm algorithm, byte[] bytes) {
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

    public byte[] getBytes() {
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

    public ByteString toByteString() {
        try {
            return ByteString.readFrom(BbBackedInputStream.aggregate(new byte[] { algorithm.signatureCode() }, bytes));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot serialize to ByteString", e);
        }
    }

    @Override
    public String toString() {
        return "Sig[" + Hex.toHexString(bytes).substring(0, 12) + ":" + algorithm.signatureCode() + "]";
    }
}
