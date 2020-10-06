/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import static com.salesforce.apollo.protocols.Conversion.hashOf;

import java.util.BitSet;

/**
 * @author hal.hildebrand
 *
 */
public class BloomFilter {

    public static class HashFunction {
        private final byte[][] h;
        private final int      keysize;
        private final int      m;

        public HashFunction(HashKey initial, long n, double p) {
            m = optimalM(n, p);
            int k = optimalK(n, m);
            h = generateSeeds(initial, k);
            keysize = initial.itself.length;
        }

        public byte[][] getH() {
            return h;
        }

        public int getK() {
            return h.length;
        }

        public int getKeysize() {
            return keysize;
        }

        public int getM() {
            return m;
        }

        public int[] hash(HashKey element) {
            int[] hashes = new int[h.length];
            for (int i = 0; i < h.length; i++) {
                byte[] h1 = xor(element, h[i]);
                hashes[i] = hashCode(h1);
            }
            return hashes;
        }

        @SuppressWarnings("unused")
        private int altHashCode(byte a[]) {
            if (a == null)
                return 0;

            int result = 1;
            for (byte element : a) {
                int unsigned = Byte.toUnsignedInt(element);
                result = 31 * result + unsigned;
            }
            if (result < 0) {
                result = -result;
            }
            return (result & 0x7fffffff) % m;
        }

        private int hashCode(byte[] result) {
            int p = 16777619;
            int hash = (int) 2166136261L;

            for (int i = 0; i < result.length; i++)
                hash = (hash ^ result[i]) * p;

            hash += hash << 13;
            hash ^= hash >> 7;
            hash += hash << 3;
            hash ^= hash >> 17;
            hash += hash << 5;
            if (hash < 0) {
                hash = -hash;
            }
            return hash % m;
        }

        private byte[] xor(HashKey a, byte[] b) {
            byte[] c = new byte[keysize];
            for (int i = 0; i < keysize; i++) {
                c[i] = (byte) (a.itself[i] ^ b[i]);
            }
            return c;
        }
    }

    public static byte[][] generateSeeds(HashKey initial, int k) {
        byte[][] h = new byte[k][];
        byte[] current = initial.bytes();
        for (int i = 0; i < k; i++) {
            current = hashOf(current);
            h[i] = current;
        }
        return h;
    }

    static double population(BitSet bitSet, int k, int m) {
        int oneBits = bitSet.cardinality();
        return -m / ((double) k) * Math.log(1 - oneBits / ((double) m));
    }

    /**
     * Computes the optimal k (number of hashes per element inserted in Bloom
     * filter), given the expected insertions and total number of bits in the Bloom
     * filter.
     *
     * See http://en.wikipedia.org/wiki/File:Bloom_filter_fp_probability.svg for the
     * formula.
     *
     * @param n expected insertions (must be positive)
     * @param m total number of bits in Bloom filter (must be positive)
     */
    private static int optimalK(long n, long m) {
        // (m / n) * log(2), but avoid truncation due to division!
        return Math.max(1, (int) Math.round(((double) m) / ((double) n) * Math.log(2)));
    }

    /**
     * Computes m (total bits of Bloom filter) which is expected to achieve, for the
     * specified expected insertions, the required false positive probability.
     *
     * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
     * for the formula.
     *
     * @param n expected insertions (must be positive)
     * @param p false positive rate (must be 0 < p < 1)
     */
    private static int optimalM(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private final BitSet       bits;
    private final HashFunction h;

    public BloomFilter(HashFunction h) {
        this.h = h;
        bits = new BitSet(h.getM());
    }

    public boolean add(HashKey element) {
        assert element.itself.length == h.getKeysize();
        boolean added = false;
        for (int position : h.hash(element)) {
            if (!bits.get(position)) {
                added = true;
                bits.set(position, true);
            }
        }
        return added;
    }

    public void clear() {
        bits.clear();
    }

    public boolean contains(HashKey element) {
        assert element.itself.length == h.getKeysize();
        for (int position : h.hash(element)) {
            if (!bits.get(position)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Estimates the current population of the Bloom filter (see:
     * http://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter
     * )
     *
     * @return the estimated amount of elements in the filter
     */
    public double getEstimatedPopulation() {
        return population(bits, h.getK(), h.getM());
    }
}
