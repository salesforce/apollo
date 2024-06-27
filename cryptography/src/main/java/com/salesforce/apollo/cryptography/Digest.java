/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.cryptography;

import com.salesforce.apollo.bloomFilters.Hash;
import com.salesforce.apollo.cryptography.proto.Digeste;
import com.salesforce.apollo.cryptography.proto.Digeste.Builder;
import com.salesforce.apollo.utils.BUZ;
import com.salesforce.apollo.utils.Hex;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;

import static com.salesforce.apollo.cryptography.DigestAlgorithm.EMPTY;

/**
 * A computed digest
 *
 * @author hal.hildebrand
 */
public class Digest implements Comparable<Digest> {
    public static final Digest          NONE     = new Digest(DigestAlgorithm.NONE, new long[] { 0L }) {

        @Override
        public String toString() {
            return "[NONE]";
        }

    };
    private final       DigestAlgorithm algorithm;
    private final       long[]          hash;
    private volatile    int             hashCode = 0;

    public Digest(byte code, long[] hash) {
        algorithm = DigestAlgorithm.fromDigestCode(code);
        assert hash.length == algorithm.longLength();
        this.hash = hash;
    }

    public Digest(ByteBuffer buff) {
        algorithm = DigestAlgorithm.fromDigestCode(buff.get());
        hash = new long[algorithm.longLength()];
        for (int i = 0; i < hash.length; i++) {
            hash[i] = buff.getLong();
        }
    }

    public Digest(DigestAlgorithm algorithm, byte[] bytes) {
        assert bytes != null && algorithm != null;

        if (bytes.length != algorithm.digestLength()) {
            throw new IllegalArgumentException(
            "Invalid bytes length.  Require: " + algorithm.digestLength() + " found: " + bytes.length);
        }
        this.algorithm = algorithm;
        int length = algorithm.longLength();
        this.hash = new long[length];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        for (int i = 0; i < length; i++) {
            hash[i] = buffer.getLong();
        }
    }

    public Digest(DigestAlgorithm algo, long[] hash) {
        if (hash.length != algo.longLength()) {
            throw new IllegalArgumentException("hash length incorrect for algorithm");
        }
        algorithm = algo;
        this.hash = hash;
    }

    public Digest(Digeste d) {
        algorithm = DigestAlgorithm.fromDigestCode(d.getType());
        if (algorithm.equals(DigestAlgorithm.NONE)) {
            hash = EMPTY;
        } else {
            assert d.getHashCount() == algorithm.longLength();
            hash = new long[d.getHashCount()];
            int i = 0;
            for (long l : d.getHashList()) {
                hash[i++] = l;
            }
        }
    }

    public static Digest combine(DigestAlgorithm algo, Digest[] digests) {
        return algo.digest(
        Stream.of(digests).map(e -> e != null ? e : algo.getOrigin()).map(e -> ByteBuffer.wrap(e.getBytes())).toList());
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

    public static Digest from(Digeste d) {
        return new Digest(d);
    }

    public static Digest normalized(DigestAlgorithm digestAlgorithm, byte[] bs) {
        if (bs.length > digestAlgorithm.digestLength()) {
            throw new IllegalArgumentException();
        }
        byte[] hash = new byte[digestAlgorithm.digestLength()];
        System.arraycopy(bs, 0, hash, 0, bs.length);
        return new Digest(digestAlgorithm, hash);
    }

    @Override
    public int compareTo(Digest id) {
        if (id == this) {
            return 0;
        }
        if (hash.length != id.hash.length) {
            return -1;
        }
        for (int i = 0; i < hash.length; i++) {
            int compare = Long.compareUnsigned(hash[i], id.hash[i]);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    public int digestCode() {
        return algorithm.digestCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Digest other) {
            if (algorithm != other.algorithm) {
                return false;
            }

            for (int i = 0; i < hash.length; i++) {
                if (hash[i] != other.hash[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public long fold() {
        long folded = 0;
        for (var l : hash) {
            folded ^= BUZ.buzhash(l) % Hash.MERSENNE_31;
        }
        return folded;
    }

    public DigestAlgorithm getAlgorithm() {
        return algorithm;
    }

    public byte[] getBytes() {
        byte[] bytes = new byte[algorithm.digestLength()];
        if (bytes.length == 0) {
            return bytes;
        }
        ByteBuffer buff = ByteBuffer.wrap(bytes);
        for (int i = 0; i < hash.length; i++) {
            buff.putLong(hash[i]);
        }
        return bytes;
    }

    public long[] getLongs() {
        return hash;
    }

    @Override
    public int hashCode() {
        final int current = hashCode;
        if (current != 0) {
            return current;
        }
        for (long l : hash) {
            if (l != 0) {
                int proposed = (int) (BUZ.buzhash(l) % Hash.MERSENNE_31);
                if (proposed == 0) {
                    hashCode = 31;
                } else {
                    hashCode = proposed;
                }
                return proposed;
            }
        }
        return hashCode = 31;
    }

    public Digest prefix(byte[]... prefixes) {
        int prefixLength = 0;
        for (byte[] p : prefixes) {
            prefixLength += p.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(hash.length * 8 + prefixLength);
        for (byte[] prefix : prefixes) {
            buffer.put(prefix);
        }
        for (long h : hash) {
            buffer.putLong(h);
        }
        buffer.flip();
        Digest d = getAlgorithm().digest(buffer);
        return new Digest(getAlgorithm(), d.getBytes());
    }

    public Digest prefix(Digest base) {
        return base.prefix(hash);
    }

    public Digest prefix(Digest id, int ring) {
        ByteBuffer buffer = ByteBuffer.allocate(hash.length * 8 + (id.getLongs().length * 8) + 4);
        for (long prefix : id.getLongs()) {
            buffer.putLong(prefix);
        }
        buffer.putInt(ring);
        for (long h : hash) {
            buffer.putLong(h);
        }
        buffer.flip();
        Digest d = getAlgorithm().digest(buffer);
        return new Digest(getAlgorithm(), d.getBytes());
    }

    public Digest prefix(long... prefixes) {
        ByteBuffer buffer = ByteBuffer.allocate(hash.length * 8 + (prefixes.length * 8));
        for (long prefix : prefixes) {
            buffer.putLong(prefix);
        }
        for (long h : hash) {
            buffer.putLong(h);
        }
        buffer.flip();
        Digest d = getAlgorithm().digest(buffer);
        return new Digest(getAlgorithm(), d.getBytes());
    }

    public Digest prefix(String prefix) {
        return prefix(prefix.getBytes());
    }

    public Digest rehash() {
        ByteBuffer buffer = ByteBuffer.allocate(hash.length * 8);
        for (long h : hash) {
            buffer.putLong(h);
        }
        buffer.flip();
        Digest d = getAlgorithm().digest(buffer);
        return new Digest(getAlgorithm(), d.getBytes());
    }

    public String shortString() {
        String hexString = Hex.hex(getBytes());
        return hexString.substring(0, Math.min(hexString.length(), 16));
    }

    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(getBytes());
    }

    public Digeste toDigeste() {
        Builder builder = Digeste.newBuilder().setType(algorithm.digestCode());
        for (long l : hash) {
            builder.addHash(l);
        }
        return builder.build();
    }

    public long toLong() {
        return algorithm.toLong(hash);
    }

    @Override
    public String toString() {
        String hexString = Hex.hex(getBytes());
        return "[" + hexString.substring(0, Math.min(hexString.length(), 12)) + "]";
    }

    public UUID toUUID() {
        return algorithm.toUUID(hash);
    }

    public Digest xor(Digest b) {
        if (algorithm != b.algorithm) {
            throw new IllegalArgumentException("Cannot xor digests of different algorithms");
        }
        long[] xord = new long[hash.length];

        for (int i = 0; i < hash.length; i++) {
            xord[i] = hash[i] ^ b.hash[i];
        }
        return new Digest(algorithm, xord);
    }
}
