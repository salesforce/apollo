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

    // NumBits is the number of bits this patricia tree manages
    public static final short NumBits = 256;
    public static final ID  ORIGIN;

    private static final int ADDRESS_BITS_PER_WORD = 6;
    private static final int BitsPerByte           = 8;
    private static final int BYTE_SIZE;
    private static final int LONG_SIZE             = 4;

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

    // EqualSubset takes in two indices and two ids and returns if the ids are
    // equal from bit start to bit end (non-inclusive). Bit indices are defined as:
    // [7 6 5 4 3 2 1 0] [15 14 13 12 11 10 9 8] ... [255 254 253 252 251 250 249
    // 248]
    // Where index 7 is the MSB of byte 0.
    public static boolean equalSubset(int start, int stop, ID id1, ID id2) {
        stop--;
        if (start > stop || stop < 0) {
            return true;
        }
        if (stop >= NumBits) {
            return false;
        }

        byte[] id1Bytes = id1.bytes();
        byte[] id2Bytes = id2.bytes();

        int startIndex = start / BitsPerByte;
        int stopIndex = stop / BitsPerByte;

        // If there is a series of bytes between the first byte and the last byte, they
        // must be equal
        if (startIndex + 1 < stopIndex && !equals(id1Bytes, id2Bytes, startIndex + 1, stopIndex)) {
            return false;
        }

        int startBit = start % BitsPerByte; // Index in the byte that the first bit is at
        int stopBit = stop % BitsPerByte; // Index in the byte that the last bit is at

        int startMask = -1 << startBit; // 111...0... The number of 0s is equal to startBit
        int stopMask = (1 << (stopBit + 1)) - 1; // 000...1... The number of 1s is equal to stopBit+1

        if (startIndex == stopIndex) {
            // If we are looking at the same byte, both masks need to be applied
            int mask = startMask & stopMask;

            // The index here could be startIndex or stopIndex, as they are equal
            int b1 = mask & id1Bytes[startIndex];
            int b2 = mask & id2Bytes[startIndex];

            return b1 == b2;
        }

        int start1 = startMask & id1Bytes[startIndex];
        int start2 = startMask & id2Bytes[startIndex];

        int stop1 = stopMask & id1Bytes[stopIndex];
        int stop2 = stopMask & id2Bytes[stopIndex];

        return start1 == start2 && stop1 == stop2;
    }

    // firstDifferenceSubset takes in two indices and two ids and returns the index
    // of the first difference between the ids inside bit start to bit end
    // (non-inclusive). Bit indices are defined above
    // @returns null, if not found
    public static Integer firstDifferenceSubset(int start, int stop, ID id1, ID id2) {
        stop--;
        if (start > stop || stop < 0 || stop >= NumBits) {
            return null;
        }

        byte[] id1Bytes = id1.bytes();
        byte[] id2Bytes = id2.bytes();

        int startIndex = start / BitsPerByte;
        int stopIndex = stop / BitsPerByte;

        int startBit = start % BitsPerByte; // Index in the byte that the first bit is at
        int stopBit = stop % BitsPerByte; // Index in the byte that the last bit is at

        int startMask = -1 << startBit; // 111...0... The number of 0s is equal to startBit
        int stopMask = (1 << (stopBit + 1)) - 1; // 000...1... The number of 1s is equal to stopBit+1

        if (startIndex == stopIndex) {
            // If we are looking at the same byte, both masks need to be applied
            int mask = startMask & stopMask;

            // The index here could be startIndex or stopIndex, as they are equal
            int b1 = mask & id1Bytes[startIndex];
            int b2 = mask & id2Bytes[startIndex];

            if (b1 == b2) {
                return null;
            }

            byte bd = (byte) (b1 ^ b2);
            return Integer.numberOfTrailingZeros(bd + startIndex * BitsPerByte);
        }

        // Check the first byte, may have some bits masked
        int start1 = startMask & id1Bytes[startIndex];
        int start2 = startMask & id2Bytes[startIndex];

        if (start1 != start2) {
            int bd = start1 ^ start2;
            return Integer.numberOfTrailingZeros(bd + startIndex * BitsPerByte);
        }

        // Check all the interior bits
        for (int i = startIndex + 1; i < stopIndex; i++) {
            byte b1 = id1Bytes[i];
            byte b2 = id2Bytes[i];
            if (b1 != b2) {
                byte bd = (byte) (b1 ^ b2);
                return Integer.numberOfTrailingZeros(bd + i * BitsPerByte);
            }
        }

        // Check the last byte, may have some bits masked
        int stop1 = stopMask & id1Bytes[stopIndex];
        int stop2 = stopMask & id2Bytes[stopIndex];

        if (stop1 != stop2) {
            int bd = stop1 ^ stop2;
            return Integer.numberOfTrailingZeros(bd + stopIndex * BitsPerByte);
        }

        // No difference was found
        return null;
    }

    private static boolean equals(byte[] a, byte[] b, int start, int stop) {
        for (int i = start; i < stop; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
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

    public ID(int i) {
        this(new long[] { 0, 0, 0, i });
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
