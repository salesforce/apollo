/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import static com.salesforce.apollo.protocols.Conversion.hashOf;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.ID;
import com.salesfoce.apollo.proto.ID.Builder;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class HashKey implements Comparable<HashKey> {
    public static final int     BYTE_SIZE;
    public static final HashKey LAST;
    public static final int     LONG_SIZE = 4;
    public static final HashKey ORIGIN;
    private final static char[] hexArray  = "0123456789ABCDEF".toCharArray();

    static {
        BYTE_SIZE = LONG_SIZE * 8;
        long[] o = new long[LONG_SIZE];
        Arrays.fill(o, 0);
        ORIGIN = new HashKey(o);
        long[] l = new long[LONG_SIZE];
        Arrays.fill(l, 0xffffffffffffffffL);
        LAST = new HashKey(l);
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

    protected final long[] itself;

    public HashKey(BigInteger i) {
        this(i.toByteArray());
    }

    public HashKey(byte[] key) {
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

    public HashKey(ByteString key) {
        this(key.toByteArray());
    }

    /**
     * @param description
     */
    public HashKey(ID description) {
        itself = new long[4];
        for (int i = 0; i < 4; i++) {
            itself[i] = description.getItself(i);
        }
    }

    /**
     * @param itself2
     */
    public HashKey(long[] itself) {
        assert itself.length == LONG_SIZE;
        this.itself = itself;
    }

    public HashKey(String b64Encoded) {
        this(Base64.getUrlDecoder().decode(b64Encoded));
    }

    public HashKey(UUID uuid) {
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
    public int compareTo(HashKey o) {
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
        long[] other = ((HashKey) obj).itself;
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

    public HashKey prefix(byte[]... prefixes) {
        ByteBuffer buffer = ByteBuffer.allocate((itself.length + prefixes.length) * 8);
        for (byte[] prefix : prefixes) {
            buffer.put(prefix);
        }
        for (long i : itself) {
            buffer.putLong(i);
        }
        return new HashKey(hashOf(buffer.array()));
    }

    public HashKey prefix(long... prefixes) {
        ByteBuffer buffer = ByteBuffer.allocate((itself.length + prefixes.length) * 8);
        for (long prefix : prefixes) {
            buffer.putLong(prefix);
        }
        for (long i : itself) {
            buffer.putLong(i);
        }
        return new HashKey(hashOf(buffer.array()));
    }

    public ByteString toByteString() {
        return ByteString.copyFrom(bytes());
    }

    public ID toID() {
        Builder builder = ID.newBuilder();
        for (long i : itself) {
            builder.addItself(i);
        }
        return builder.build();
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
