/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class HashKey implements Comparable<HashKey> {

    // Last ID on a 32 byte hash ring
    public static final HashKey LAST;
    // First ID on a 32 byte hash ring
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

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    protected final byte[] itself;
    protected final int    hashCode;

    public HashKey(byte[] key) {
        itself = key;
        hashCode = new String(itself).hashCode();
    }

    public HashKey(String b64Encoded) {
        this(Base64.getUrlDecoder().decode(b64Encoded));
    }

    public String b64Encoded() {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(itself);
    }

    public byte[] bytes() {
        return itself;
    }

    @Override
    public int compareTo(HashKey o) {
        byte[] buffer2 = o.itself;
        if (itself == buffer2) {
            return 0;
        }
        for (int i = 0, j = 0; i < itself.length && j < itself.length; i++, j++) {
            int a = (itself[i] & 0xff);
            int b = (buffer2[j] & 0xff);
            if (a != b) {
                return a - b;
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
        return Arrays.equals(itself, ((HashKey) obj).itself);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "HashKey[" + b64Encoded() + "]";
    }

    public void write(ByteBuffer dest) {
        dest.put(itself);
    }
}
