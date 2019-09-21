/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.protocols;

import static com.salesforce.apollo.protocols.Conversion.SHA_256;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.HASH;

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
        Arrays.fill(o, (byte)0);
        ORIGIN = new HashKey(o);
        byte[] l = new byte[32];
        Arrays.fill(l, (byte)255);
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

    /**
     * @param entry
     * @return the hash value of the entry
     */
    public static byte[] hashOf(Entry entry) {
        ByteBuffer buffer = entry.getData();
        buffer.mark();
        MessageDigest md;
        try {
            md = MessageDigest.getInstance(SHA_256);
        } catch (NoSuchAlgorithmException e1) {
            throw new IllegalStateException("Cannot get instance of message digest");
        }
        md.update((byte)entry.getType().ordinal());
        md.update(entry.getData().array());
        return md.digest();
    }

    protected final byte[] itself;

    public HashKey(byte[] key) {
        itself = key;
    }

    public HashKey(HASH key) {
        this(key.bytes());
    }

    public byte[] bytes() {
        return itself;
    }

    @Override
    public int compareTo(HashKey o) {
        return compare(itself, o.itself);
    }

    public int compareTo(HASH t) {
        return compare(itself, t.bytes());
    }

    public HASH toHash() {
        return new HASH(bytes());
    }

    @Override
    public String toString() {
        return "HashKey[" + Base64.getEncoder().withoutPadding().encodeToString(itself) + "]";
    }

    public void write(ByteBuffer dest) {
        dest.put(itself);
    }
}
