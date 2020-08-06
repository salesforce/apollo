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

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static int compare(byte[] buffer1, byte[] buffer2) {
        // Short circuit equal case
        if (buffer1 == buffer2) {
            return 0;
        }
        // Bring WritableComparator code local
        for (int i = 0, j = 0; i < buffer1.length && j < buffer1.length; i++, j++) {
            int a = (buffer1[i] & 0xff);
            int b = (buffer2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return 0;
    }

    protected final byte[] itself;

    public HashKey(byte[] key) {
        itself = key;
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
        HashKey other = (HashKey) obj;
        return this.compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
        return new String(itself).hashCode();
    }

    @Override
    public String toString() {
        return "HashKey[" + b64Encoded() + "]";
    }

    public void write(ByteBuffer dest) {
        dest.put(itself);
    }
}
