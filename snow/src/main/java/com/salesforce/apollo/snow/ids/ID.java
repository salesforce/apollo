/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.ids;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class ID implements Comparable<ID> {
    public static final int                        BitsPerByte = 8;
    public static final int                        BYTE_SIZE;
    public static final ID                         LAST;
    public static final int                        LONG_SIZE   = 4;
    public static final ThreadLocal<MessageDigest> MESSAGE_DIGEST;
    // NumBits is the number of bits this patricia tree manages
    public static final short   NumBits = 256;
    public static final ID      ORIGIN;
    public static final String  SHA_256 = "sha-256";
    private static final byte[] ntz8tab = new byte[] { 0x08, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x03, 0x00, 0x01,
                                                       0x00, 0x02, 0x00, 0x01, 0x00, 0x04, 0x00, 0x01, 0x00, 0x02, 0x00,
                                                       0x01, 0x00, 0x03, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x05,
                                                       0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x03, 0x00, 0x01, 0x00,
                                                       0x02, 0x00, 0x01, 0x00, 0x04, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01,
                                                       0x00, 0x03, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x06, 0x00,
                                                       0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x03, 0x00, 0x01, 0x00, 0x02,
                                                       0x00, 0x01, 0x00, 0x04, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00,
                                                       0x03, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x05, 0x00, 0x01,
                                                       0x00, 0x02, 0x00, 0x01, 0x00, 0x03, 0x00, 0x01, 0x00, 0x02, 0x00,
                                                       0x01, 0x00, 0x04, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x03,
                                                       0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x07, 0x00, 0x01, 0x00,
                                                       0x02, 0x00, 0x01, 0x00, 0x03, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01,
                                                       0x00, 0x04, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x03, 0x00,
                                                       0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x05, 0x00, 0x01, 0x00, 0x02,
                                                       0x00, 0x01, 0x00, 0x03, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00,
                                                       0x04, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x03, 0x00, 0x01,
                                                       0x00, 0x02, 0x00, 0x01, 0x00, 0x06, 0x00, 0x01, 0x00, 0x02, 0x00,
                                                       0x01, 0x00, 0x03, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x04,
                                                       0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x03, 0x00, 0x01, 0x00,
                                                       0x02, 0x00, 0x01, 0x00, 0x05, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01,
                                                       0x00, 0x03, 0x00, 0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x04, 0x00,
                                                       0x01, 0x00, 0x02, 0x00, 0x01, 0x00, 0x03, 0x00, 0x01, 0x00, 0x02,
                                                       0x00, 0x01, 0x00 };

    static {
        BYTE_SIZE = LONG_SIZE * 8;
        long[] o = new long[LONG_SIZE];
        Arrays.fill(o, 0);
        ORIGIN = new ID(o);
        long[] l = new long[LONG_SIZE];
        Arrays.fill(l, 0xffffffffffffffffL);
        LAST = new ID(l);
        MESSAGE_DIGEST = ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance(SHA_256);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("Unable to retrieve " + SHA_256 + " Message Digest instance", e);
            }
        });
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
            return trailingZeros(bd) + (startIndex * BitsPerByte);
        }

        // Check the first byte, may have some bits masked
        int start1 = startMask & id1Bytes[startIndex];
        int start2 = startMask & id2Bytes[startIndex];

        if (start1 != start2) {
            int bd = start1 ^ start2;
            return trailingZeros(bd) + (startIndex * BitsPerByte);
        }

        // Check all the interior bits
        for (int i = startIndex + 1; i < stopIndex; i++) {
            byte b1 = id1Bytes[i];
            byte b2 = id2Bytes[i];
            if (b1 != b2) {
                byte bd = (byte) (b1 ^ b2);
                return trailingZeros(bd) + (i * BitsPerByte);
            }
        }

        // Check the last byte, may have some bits masked
        int stop1 = stopMask & id1Bytes[stopIndex];
        int stop2 = stopMask & id2Bytes[stopIndex];

        if (stop1 != stop2) {
            int bd = stop1 ^ stop2;
            return trailingZeros(bd) + (stopIndex * BitsPerByte);
        }

        // No difference was found
        return null;
    }

    public static byte[] hashOf(byte[]... bytes) {
        MessageDigest md = MESSAGE_DIGEST.get();
        md.reset();
        for (byte[] entry : bytes) {
            md.update(entry);
        }
        return md.digest();
    }

    private static boolean equals(byte[] a, byte[] b, int start, int stop) {
        for (int i = start; i < stop; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    private static int trailingZeros(int x) {
        return (int) ntz8tab[x & 0xFF];
    }

    protected final long[] itself;

    public ID(BigInteger i) {
        this(i.toByteArray());
    }

    public ID(byte i) {
        this(new long[] { 0, 0, 0, i });
    }

    public ID(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("Cannot be null");
        } else if (key.length < BYTE_SIZE) {
            byte[] normalized = new byte[BYTE_SIZE];
            for (int i = 0; i < key.length; i++) {
                normalized[i] = key[i];
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
        this(new long[] { i, 0, 0, 0 });
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

    public int bit(int i) {
        int byteIndex = i / BitsPerByte;
        int bitIndex = i % BitsPerByte;

        byte[] bytes = bytes();
        byte b = bytes[byteIndex];

        // b = [7, 6, 5, 4, 3, 2, 1, 0]

        b = (byte) (b >> bitIndex);

        // b = [0, ..., bitIndex + 1, bitIndex]
        // 1 = [0, 0, 0, 0, 0, 0, 0, 1]

        b = (byte) (b & 1);

        // b = [0, 0, 0, 0, 0, 0, 0, bitIndex]

        return (int) b;
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
        long result = 0;
        for (long l : itself) {
            result ^= l;
        }
        return (int) (result & Integer.MAX_VALUE);
    }

    public boolean IsZero() {
        return equals(ORIGIN);
    }

    public long[] longs() {
        return itself;
    }

    public ID prefix(long... prefixes) {
        ByteBuffer buffer = ByteBuffer.allocate((itself.length + prefixes.length) * 8);
        for (long prefix : prefixes) {
            buffer.putLong(prefix);
        }
        for (long i : itself) {
            buffer.putLong(i);
        }
        return new ID(hashOf(buffer.array()));
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
