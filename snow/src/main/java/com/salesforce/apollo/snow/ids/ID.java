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
    public static final ID      LAST;
    public static final ID      ORIGIN;
    private static final int    BYTE_SIZE;
    private final static char[] hexArray  = "0123456789ABCDEF".toCharArray();
    private static final int    LONG_SIZE = 4;

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

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
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
