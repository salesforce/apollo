/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.util.BitSet;

/**
 * @author hal.hildebrand
 *
 */
public class HashFunction {
    public static class Hasher {

        public static final int HASH_KEY_SIZE = 32;

        private static final long C1 = 0x87c37b91114253d5L;
        private static final long C2 = 0x4cf5ad432745937fL;

        private static long fmix64(long k) {
            k ^= k >>> 33;
            k *= 0xff51afd7ed558ccdL;
            k ^= k >>> 33;
            k *= 0xc4ceb9fe1a85ec53L;
            k ^= k >>> 33;
            return k;
        }

        private static long mixK1(long k1) {
            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            return k1;
        }

        private static long mixK2(long k2) {
            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            return k2;
        }

        private long h1;
        private long h2;

        public Hasher(HashKey key, int seed) {
            this.h1 = seed;
            this.h2 = seed;
            process(key);
        }

        public long getH1() {
            return h1;
        }

        public long getH2() {
            return h2;
        }

        private void bmix64(long k1, long k2) {
            h1 ^= mixK1(k1);

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            h2 ^= mixK2(k2);

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        private Hasher makeHash() {
            h1 ^= HASH_KEY_SIZE;
            h2 ^= HASH_KEY_SIZE;

            h1 += h2;
            h2 += h1;

            h1 = fmix64(h1);
            h2 = fmix64(h2);

            h1 += h2;
            h2 += h1;
            return this;
        }

        private void process(HashKey key) {
            bmix64(key.itself[0], key.itself[1]);
            bmix64(key.itself[2], key.itself[3]);
            makeHash();
        }

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

    private final int k;
    private final int m;
    private final int seed;

    public HashFunction(int seed, int m, int k) {
        this.m = m;
        this.k = k;
        this.seed = seed;
    }

    public HashFunction(int seed, long n, double p) {
        m = optimalM(n, p);
        k = optimalK(n, m);
        this.seed = seed;
    }

    public int getK() {
        return k;
    }

    public int getM() {
        return m;
    }

    public int getSeed() {
        return seed;
    }

    public boolean mightContain(HashKey key, BitSet bits) {
        Hasher hasher = new Hasher(key, seed);

        long combinedHash = hasher.h1;
        for (int i = 0; i < k; i++) {
            if (!bits.get(((int) (combinedHash & Integer.MAX_VALUE)) % m)) {
                return false;
            }
            combinedHash += hasher.h2;
        }
        return true;
    }

    public boolean put(HashKey key, BitSet bits) {
        Hasher hasher = new Hasher(key, seed);

        boolean bitsChanged = false;
        long combinedHash = hasher.h1;
        for (int i = 0; i < k; i++) {
            bits.set(((int) (combinedHash & Integer.MAX_VALUE)) % m);
            combinedHash += hasher.h2;
        }
        return bitsChanged;
    }

}
