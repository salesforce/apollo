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
abstract public class Hash<T> {

    abstract public static class Hasher<T> {

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

        long h1;
        long h2;

        public Hasher(T key, int seed) {
            this.h1 = seed;
            this.h2 = seed;
            processIt(key);
        }

        public Hasher(T key, long seed) {
            this.h1 = seed;
            this.h2 = seed;
            processIt(key);
        }

        public long getH1() {
            return h1;
        }

        public long getH2() {
            return h2;
        }

        protected void process(HashKey key) {
            bmix64(key.itself[0], key.itself[1]);
            bmix64(key.itself[2], key.itself[3]);
            makeHash();
        }

        protected void process(Integer i) {
            h1 ^= mixK1(0);
            h2 ^= mixK2(i.longValue());
        }

        protected void process(Long l) {
            h1 ^= mixK1(l.longValue());
            h2 ^= mixK2(0);
        }

        abstract void processIt(T it);

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

        private Hasher<T> makeHash() {
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

    }

    public static class HkHasher extends Hasher<HashKey> {

        public HkHasher(HashKey key, int seed) {
            super(key, seed);
        }

        public HkHasher(HashKey key, long seed) {
            super(key, seed);
        }

        @Override
        protected void processIt(HashKey key) {
            process(key);
        }

    }

    public static class IntHasher extends Hasher<Integer> {

        public IntHasher(Integer key, int seed) {
            super(key, seed);
        }

        public IntHasher(Integer key, long seed) {
            super(key, seed);
        }

        @Override
        protected void processIt(Integer key) {
            process(key);
        }

    }

    public static class LongHasher extends Hasher<Long> {

        public LongHasher(Long key, int seed) {
            super(key, seed);
        }

        public LongHasher(Long key, long seed) {
            super(key, seed);
        }

        @Override
        protected void processIt(Long key) {
            process(key);
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
    protected static int optimalK(long n, long m) {
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
    protected static int optimalM(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    protected final int k;
    protected final int m;
    protected final int seed;

    public Hash(int seed, int m, int k) {
        this.m = m;
        this.k = k;
        this.seed = seed;
    }

    public Hash(int seed, long n, double p) {
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

    public boolean mightContain(T key, BitSet bits) {
        Hasher<T> hasher = newHasher(key);

        long combinedHash = hasher.h1;
        for (int i = 0; i < k; i++) {
            if (!bits.get(((int) (combinedHash & Integer.MAX_VALUE)) % m)) {
                return false;
            }
            combinedHash += hasher.h2;
        }
        return true;
    }

    public boolean put(T key, BitSet bits) {
        Hasher<T> hasher = newHasher(key);

        boolean bitsChanged = false;
        long combinedHash = hasher.h1;
        for (int i = 0; i < k; i++) {
            int index = ((int) (combinedHash & Integer.MAX_VALUE)) % m;
            bitsChanged |= bits.get(index);
            bits.set(index);
            combinedHash += hasher.h2;
        }
        return bitsChanged;
    }

    abstract Hasher<T> newHasher(T key);

}
