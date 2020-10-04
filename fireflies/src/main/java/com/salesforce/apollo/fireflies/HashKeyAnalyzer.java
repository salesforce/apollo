/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import org.apache.commons.collections4.trie.KeyAnalyzer;
import org.bouncycastle.util.Arrays;

import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class HashKeyAnalyzer extends KeyAnalyzer<HashKey> {

    private static final long serialVersionUID = 1L;
    private static final int  LENGTH           = 8;   // byte
    /** A bit mask where the first bit is 1 and the others are zero. */
    private static final int  MSB              = 0x80;

    @Override
    public int bitsPerElement() {
        return LENGTH;
    }

    @Override
    public int lengthInBits(HashKey key) {
        return key.bytes().length * 8;
    }

    @Override
    public boolean isBitSet(HashKey key, int bitIndex, int lengthInBits) {
        if (key == null) {
            return false;
        }
        final int prefix = 32 - lengthInBits;
        final int keyBitIndex = bitIndex - prefix;
        if (keyBitIndex >= lengthInBits || keyBitIndex < 0) {
            return false;
        }
        final int index = keyBitIndex / LENGTH;
        final int bit = keyBitIndex % LENGTH;
        return (key.bytes()[index] & mask(bit)) != 0;
    }

    /** Returns a bit mask where the given bit is set. */
    private static int mask(final int bit) {
        return MSB >>> bit;
    }

    @Override
    public int bitIndex(HashKey key, int offsetInBits, int lengthInBits, HashKey other, int otherOffsetInBits,
                        int otherLengthInBits) {
        if (other == null) {
            other = HashKey.ORIGIN;
        }
        boolean allNull = true;
        final int length = Math.max(lengthInBits, otherLengthInBits);
        final int prefix = HashKey.ORIGIN.bytes().length - length;
        if (prefix < 0) {
            return KeyAnalyzer.OUT_OF_BOUNDS_BIT_KEY;
        }
        for (int i = 0; i < length; i++) {
            final int index = prefix + offsetInBits + i;
            final boolean value = isBitSet(key, index, lengthInBits);
            if (value) {
                allNull = false;
            }
            final int otherIndex = prefix + otherOffsetInBits + i;
            final boolean otherValue = isBitSet(other, otherIndex, otherLengthInBits);
            if (value != otherValue) {
                return index;
            }
        }
        if (allNull) {
            return KeyAnalyzer.NULL_BIT_KEY;
        }
        return KeyAnalyzer.EQUAL_BIT_KEY;
    }

    @Override
    public boolean isPrefix(HashKey prefix, int offsetInBits, int lengthInBits, HashKey key) {
        byte[] prefixBytes = Arrays.copyOfRange(prefix.bytes(), offsetInBits / LENGTH, lengthInBits / LENGTH);
        byte[] keyBytes = key.bytes();

        for (int i = 0; i < prefixBytes.length; i++) {
            if (prefixBytes[i] != keyBytes[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int compare(HashKey o1, HashKey o2) { 
        return o1.compareTo(o2);
    }

}
