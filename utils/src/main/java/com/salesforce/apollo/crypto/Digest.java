/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import java.util.Arrays;
import java.util.Objects;

import org.bouncycastle.util.encoders.Hex;

/**
 * A computed digest
 * 
 * @author hal.hildebrand
 *
 */
public class Digest implements Comparable<Digest> {
    public static final Digest NONE = new Digest(DigestAlgorithm.NONE, new byte[0]);

    public static int compare(byte[] o1, byte[] o2) {
        if (o1 == null) {
            return o2 == null ? 0 : -1;
        } else if (o2 == null) {
            return 1;
        }
        if (o1.length != o2.length) {
            return o1.length - o2.length;
        }
        for (int i = 0; i < o1.length; i++) {
            final int diff = (o1[i] & 0xFF) - (o2[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }

    public static boolean matches(byte[] bytes, Digest d1) {
        return Arrays.equals(d1.getBytes(), d1.getAlgorithm().digest(bytes).getBytes());
    }

    private final DigestAlgorithm algorithm;
    private final byte[]          bytes;
    private int                   hashCode;

    public Digest(DigestAlgorithm algorithm, byte[] bytes) {
        assert bytes != null && algorithm != null;

        if (bytes.length != algorithm.digestLength()) {
            throw new IllegalArgumentException(
                    "Invalid bytes length.  Require: " + algorithm.digestLength() + " found: " + bytes.length);
        }
        this.algorithm = algorithm;
        this.bytes = bytes;
    }

    @Override
    public int compareTo(Digest id) {
        if (id == null) {
            return 1;
        }
        if (id.algorithm != algorithm) {
            throw new IllegalArgumentException("Cannot compare digests of different algorithm. this: " + algorithm
                    + " is not: " + id.getAlgorithm());
        }
        return compare(bytes, id.bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Digest)) {
            return false;
        }
        Digest other = (Digest) obj;
        return algorithm == other.algorithm && Arrays.equals(bytes, other.bytes);
    }

    public DigestAlgorithm getAlgorithm() {
        return algorithm;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public int hashCode() {
        if (hashCode < 0) {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(bytes);
            result = prime * result + Objects.hash(algorithm);
            hashCode = result;
        }
        return hashCode;
    }

    @Override
    public String toString() {
        return "[" + algorithm + ":" + Hex.toHexString(bytes).substring(0, 12) + "]";
    }
}
