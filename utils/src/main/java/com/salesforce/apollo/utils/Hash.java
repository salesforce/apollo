/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.util.function.Consumer;
import java.util.function.Function;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.utils.IBF.IntIBF;

/**
 * @author hal.hildebrand
 *
 */
public abstract class Hash<M> {
    abstract public static class Hasher<M> {
        public static class DigestHasher extends Hasher<Digest> {

            public DigestHasher(Digest key, long seed) {
                super(key, seed);
            }

            public long getH1() {
                return h1;
            }

            @Override
            protected void processIt(Digest key) {
                process(key);
            }

        }

        public static class IntHasher extends Hasher<Integer> {

            public IntHasher(Integer key, long seed) {
                super(key, seed);
            }

            @Override
            protected void processIt(Integer key) {
                process(key);
            }

        }

        public static class LongHasher extends Hasher<Long> {

            public LongHasher(Long key, long seed) {
                super(key, seed);
            }

            @Override
            protected void processIt(Long key) {
                process(key);
            }

        }

        private static final long C1         = 0x87c37b91114253d5L;
        private static final long C2         = 0x4cf5ad432745937fL;
        private static final long CHUNK_SIZE = 16;

        long h1;
        long h2;
        int  length;

        Hasher(M key, long seed) {
            this.h1 = seed;
            this.h2 = seed;
            processIt(key);
            makeHash();
        }

        public int identityHash() {
            process(207);
            makeHash();
            return (int) (h1 & Integer.MAX_VALUE);
        }

        public void process(int k, Consumer<Integer> processor, int m) {
            process(k, hash -> {
                processor.accept(hash);
                return true;
            }, m);
        }

        public boolean process(int k, Function<Integer, Boolean> processor, int m) {
            long combinedHash = h1;
            int[] locations = new int[k];
            int index = 0;
            while (index < k) {
                int location = (IntIBF.smear((int) combinedHash & Integer.MAX_VALUE) % m);
                if (!contains(locations, location)) {
                    if (!processor.apply(location)) {
                        return false;
                    }
                    locations[index] = location;
                    index++;
                }
                combinedHash += h2;
            }
            return true;
        }

        void process(Digest key) {
            long[] hash = key.getLongs();
            for (int i = 0; i < hash.length / 2; i += 2) {
                bmix64(hash[i], hash[i + 1]);
                length += CHUNK_SIZE;
            }
            if ((hash.length & 1) != 0) {
                process(hash[hash.length - 1]);
            }
        }

        void process(int i) {
            h1 ^= mixK1(0);
            h2 ^= mixK2(i);
            length += CHUNK_SIZE / 4;
        }

        void process(long l) {
            h1 ^= mixK1(l);
            h2 ^= mixK2(0);
            length += CHUNK_SIZE / 2;
        }

        abstract void processIt(M key);

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

        private boolean contains(int[] locations, int location) {
            for (int i = 0; i < locations.length; i++) {
                if (locations[i] == location) {
                    return true;
                }
            }
            return false;
        }

        private long fmix64(long k) {
            k ^= k >>> 33;
            k *= 0xff51afd7ed558ccdL;
            k ^= k >>> 33;
            k *= 0xc4ceb9fe1a85ec53L;
            k ^= k >>> 33;
            return k;
        }

        private void makeHash() {
            h1 ^= length;
            h2 ^= length;

            h1 += h2;
            h2 += h1;

            h1 = fmix64(h1);
            h2 = fmix64(h2);

            h1 += h2;
            h2 += h1;
        }

        private long mixK1(long k1) {
            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            return k1;
        }

        private long mixK2(long k2) {
            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            return k2;
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
        return Math.max(8, (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2))));
    }

    protected final int  k;
    protected final int  m;
    protected final long seed;

    public Hash(long seed, int m, int k) {
        this.seed = seed;
        this.k = k;
        this.m = m;
    }

    public Hash(long seed, int n, double p) {
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

    public long getSeed() {
        return seed;
    }

    public int identityHash(M key) {
        return newHasher(key).identityHash();
    }

    public void process(M key, Consumer<Integer> processor) {
        newHasher(key).process(k, processor, m);
    }

    public boolean process(M key, Function<Integer, Boolean> processor) {
        return newHasher(key).process(k, processor, m);
    }

    abstract Hasher<M> newHasher(M key);
}
