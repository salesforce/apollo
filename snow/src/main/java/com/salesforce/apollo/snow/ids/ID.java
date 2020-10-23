/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.ids;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class ID implements Comparable<ID> {
    public static final ID LAST;
    public static final ID ORIGIN;

    private static final int ADDRESS_BITS_PER_WORD = 6;
    private static final int BYTE_SIZE;
    private static final int LONG_SIZE             = 4;

    // NumBits is the number of bits this patricia tree manages
    public static final int NumBits = 256;

    // EqualSubset takes in two indices and two ids and returns if the ids are
    // equal from bit start to bit end (non-inclusive). Bit indices are defined as:
    // [7 6 5 4 3 2 1 0] [15 14 13 12 11 10 9 8] ... [255 254 253 252 251 250 249
    // 248]
    // Where index 7 is the MSB of byte 0.
    static boolean equalSubset(int start, int stop, ID id1, ID id2) {
        return true; // TODO
    }

    static {
        BYTE_SIZE = LONG_SIZE * 8;
        long[] o = new long[LONG_SIZE];
        Arrays.fill(o, 0);
        ORIGIN = new ID(o);
        long[] l = new long[LONG_SIZE];
        Arrays.fill(l, 0xffffffffffffffffL);
        LAST = new ID(l);
    }

    public static byte[] bytes(UUID uuid) {
        byte[] bytes = new byte[32];
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.putLong(uuid.getLeastSignificantBits());
        buf.putLong(uuid.getMostSignificantBits());
        return bytes;
    }

    /**
     * Given a bit index, return word index containing it.
     */
    private static int wordIndex(int bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }

    protected final long[] itself;

    public ID(BigInteger i) {
        this(i.toByteArray());
    }

    public ID(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("Cannot be null");
        } else if (key.length < BYTE_SIZE) {
            byte[] normalized = new byte[BYTE_SIZE];
            int start = BYTE_SIZE - key.length;
            for (int i = 0; i < key.length; i++) {
                normalized[i + start] = key[i];
            }
            key = normalized;
        } else if (key.length > BYTE_SIZE) {
            throw new IllegalArgumentException("Cannot be larger than " + BYTE_SIZE + " bytes: " + key.length);
        }

        itself = new long[4];
        ByteBuffer buff = ByteBuffer.wrap(key);
        for (int i = 0; i < 4; i++) {
            itself[i] = buff.getLong();
        }
    }

    /**
     * @param itself
     */
    public ID(long[] itself) {
        assert itself.length == LONG_SIZE;
        this.itself = itself;
    }

    public ID(String b64Encoded) {
        this(Base64.getUrlDecoder().decode(b64Encoded));
    }

    public ID(UUID uuid) {
        this(bytes(uuid));
    }

    public String b64Encoded() {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes());
    }

    public boolean bit(int bitIndex) {
        if (bitIndex < 0)
            throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
        int wordIndex = wordIndex(bitIndex);
        return (wordIndex < LONG_SIZE) && ((itself[wordIndex] & (1L << bitIndex)) != 0);
    }

    public byte[] bytes() {
        byte[] bytes = new byte[32];
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        for (int i = 0; i < itself.length; i++) {
            buff.putLong(itself[i]);
        }
        return bytes;
    }

    @Override
    public int compareTo(ID o) {
        for (int i = 0; i < 4; i++) {
            int compare = Long.compareUnsigned(itself[i], o.itself[i]);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        long[] other = ((ID) obj).itself;
        for (int i = 0; i < 4; i++) {
            if (itself[i] != other[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return (int) (itself[0] & 0xFFFFFFFF);
    }

    public long[] longs() {
        return itself;
    }

    @Override
    public String toString() {
        return "[" + b64Encoded() + "]";
    }

    public void write(ByteBuffer dest) {
        for (long l : itself) {
            dest.putLong(l);
        }
    }
}
