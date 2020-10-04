/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.collections4.trie.MerklePatriciaTrie;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class HashKeyAnalyzerTest {
    private static final int SIZE = 20000;

    @Test
    public void bitSet() {
        final byte[] key = toByteArray("10100110", 2);
        final ByteArrayKeyAnalyzer ka = new ByteArrayKeyAnalyzer(key.length * 8);
        final int length = ka.lengthInBits(key);
        assertTrue(ka.isBitSet(key, 0, length));
        assertFalse(ka.isBitSet(key, 1, length));
        assertTrue(ka.isBitSet(key, 2, length));
        assertFalse(ka.isBitSet(key, 3, length));
        assertFalse(ka.isBitSet(key, 4, length));
        assertTrue(ka.isBitSet(key, 5, length));
        assertTrue(ka.isBitSet(key, 6, length));
        assertFalse(ka.isBitSet(key, 7, length));
    }

    @Test
    public void keys() {
        final MerklePatriciaTrie<BigInteger> trie = new MerklePatriciaTrie<BigInteger>(4);
        final Map<HashKey, BigInteger> map = new TreeMap<HashKey, BigInteger>(new HashKeyAnalyzer());
        for (int i = 0; i < SIZE; i++) {
            BigInteger value = BigInteger.valueOf(i);
            final HashKey key = new HashKey(value);
            final BigInteger existing = trie.put(key, value);
            assertNull(existing, "Expected no prev value: " + key + " i=" + i);
            map.put(key, value);
        }
        assertEquals(map.size(), trie.size());
        for (HashKey key : map.keySet()) {
            final BigInteger expected = new BigInteger(1, key.bytes());
            final BigInteger value = trie.get(key);
            assertEquals(expected, value);
        }
    }

    @Test
    public void prefix() {
        final byte[] prefix = toByteArray("00001010", 2);
        final byte[] key1 = toByteArray("11001010", 2);
        final byte[] key2 = toByteArray("10101100", 2);
        final ByteArrayKeyAnalyzer keyAnalyzer = new ByteArrayKeyAnalyzer(key1.length * 8);
        final int prefixLength = keyAnalyzer.lengthInBits(prefix);
        assertFalse(keyAnalyzer.isPrefix(prefix, 4, prefixLength, key1));
        assertTrue(keyAnalyzer.isPrefix(prefix, 4, prefixLength, key2));
    }

    private static byte[] toByteArray(final String value, final int radix) {
        return toByteArray(Long.parseLong(value, radix));
    }

    private static byte[] toByteArray(final long value) {
        return toByteArray(BigInteger.valueOf(value));
    }

    private static byte[] toByteArray(final BigInteger value) {
        final byte[] src = value.toByteArray();
        if (src.length <= 1) {
            return src;
        }
        if (src[0] != 0) {
            return src;
        }
        final byte[] dst = new byte[src.length - 1];
        System.arraycopy(src, 1, dst, 0, dst.length);
        return dst;
    }
}
