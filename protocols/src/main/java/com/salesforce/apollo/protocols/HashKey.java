/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

import com.google.protobuf.ByteString;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class HashKey implements Comparable<HashKey> {

    public static final HashKey LAST;
    public static final HashKey ORIGIN;

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    static {
        byte[] o = new byte[32];
        Arrays.fill(o, (byte) 0);
        ORIGIN = new HashKey(o);
        byte[] l = new byte[32];
        Arrays.fill(l, (byte) 255);
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

    protected final byte[] itself;
    private final int      hashCode;

    public HashKey(byte[] key) {
        itself = key;
        hashCode = ByteBuffer.wrap(itself).getInt();
    }

    public HashKey(ByteString key) {
        this(key.toByteArray());
    }

    public HashKey(String b64Encoded) {
        this(Base64.getUrlDecoder().decode(b64Encoded));
    }

    public HashKey(UUID uuid) {
        this(bytes(uuid));
    }

    public HashKey(BigInteger i) {
        this(i.toByteArray());
    }

    public String b64Encoded() {
        return Base64.getEncoder().withoutPadding().encodeToString(itself);
    }

    public byte[] bytes() {
        return itself;
    }

    @Override
    public int compareTo(HashKey o) {
        return compare(itself, o.itself);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        return Arrays.equals(itself, ((HashKey) obj).itself);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    public ByteString toByteString() {
        return ByteString.copyFrom(itself);
    }

    @Override
    public String toString() {
        return "[" + b64Encoded() + "]";
    }

    public void write(ByteBuffer dest) {
        dest.put(itself);
    }
}
