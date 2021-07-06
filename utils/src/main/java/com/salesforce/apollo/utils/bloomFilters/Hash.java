/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;

import static com.salesforce.apollo.utils.bloomFilters.Primes.PRIMES;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public abstract class Hash<M> {
//    public static int HITS = 0;
//    public static int MISSES = 0;

    public static class BytesHasher extends Hasher<byte[]> {

        public BytesHasher() {
        }

        public BytesHasher(byte[] key, long seed) {
            super(key, seed);
        }

        @Override
        protected BytesHasher clone() {
            return new BytesHasher();
        }

        @Override
        protected void processIt(byte[] key) {
            process(key);
        }

    }

    public static class DigestHasher extends Hasher<Digest> {

        public DigestHasher() {
            super();
        }

        public DigestHasher(Digest key, long seed) {
            super(key, seed);
        }

        @Override
        protected Hasher<Digest> clone() {
            return new DigestHasher();
        }

        @Override
        protected void processIt(Digest key) {
            process(key);
        }
    }

    abstract public static class Hasher<M> {

        private static final long C1                   = 0x87c37b91114253d5L;
        private static final long C2                   = 0x4cf5ad432745937fL;
        private static final long CHUNK_SIZE           = 16;
        private static final int  MAX_HASHING_ATTEMPTS = 500;

        static int toInt(byte value) {
            return value & 0xFF;
        }

        private static void throwMax(int k, int m, int[] hashes) {
            throw new IllegalStateException("Cannot find: " + k + " unique hashes for m: " + m + " after: "
            + MAX_HASHING_ATTEMPTS + " hashing attempts.  found: " + IntStream.of(hashes).mapToObj(e -> e).toList());
        }

        long h1;
        long h2;
        int  length;

        public Hasher() {
        }

        public Hasher(M key, long seed) {
            process(key, seed);
        }

        /**
         * Generate K unique hash locations for M elements, using the seed.
         */
        public int[] hashes(int k, M key, int m, long seed) {
            process(key, seed);
            long combinedHash = h1;
            int[] hashes = new int[k];
            int attempts = 0;
            int i = 0;
            int prime = 0;
            while (i < k) {
                if (attempts++ > MAX_HASHING_ATTEMPTS) { // limit the pain
                    throwMax(k, m, hashes);
                }
                int hash = (int) ((combinedHash ^ (combinedHash >> 32)) & Integer.MAX_VALUE) % m;
                boolean found = false;
                for (int j = 0; j < i; j++) {
                    if (hashes[j] == hash) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    hashes[i++] = hash;
//                    HITS++;
                } else {
                    h2 += PRIMES[prime];
                    prime = ++prime % PRIMES.length;
//                    MISSES++;
                }
                combinedHash += h2;
            }
            return hashes;
        }

        public long identityHash() {
            return h1;
        }

        public long identityHash(M key, long seed) {
            process(key, seed);
            return h1;
        }

        public void processAdditional(int value) {
            process(value);
            makeHash();
        }

        public void processAdditional(long value) {
            process(value);
            makeHash();
        }

        public void processAdditional(M value) {
            processIt(value);
            makeHash();
        }

        protected abstract Hasher<M> clone();

        IntStream locations(int k, M key, int m, long seed) {
            return IntStream.of(hashes(k, key, m, seed));
        }

        void process(byte[] key) {
            ByteBuffer buff = ByteBuffer.wrap(key);
            process(buff);
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
            int reversed = Integer.reverse(i);
            ByteBuffer bb = ByteBuffer.wrap(new byte[2 * 8]);
            bb.putInt(i);
            bb.putInt(i + PRIMES[(i & Integer.MAX_VALUE) % PRIMES.length]);
            bb.putInt(reversed);
            bb.putInt(reversed + PRIMES[((i + 1) & Integer.MAX_VALUE) % PRIMES.length]);
            bb.flip();
            process(bb);
        }

        void process(long l) {
            bmix64(l, Long.reverse(l));
            length += CHUNK_SIZE;
        }

        Hasher<M> process(M key, long seed) {
            h1 = seed;
            h2 = Long.reverse(seed);
            length = 0;
            processIt(key);
            makeHash();
            return this;
        }

        void process(String key) {
            process(key.getBytes());
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

        private void process(ByteBuffer buff) {
            while (buff.remaining() >= 16) {
                bmix64(buff.getLong(), buff.getLong());
                length += CHUNK_SIZE;
            }
            if (buff.hasRemaining()) {
                processRemaining(buff);
            }
        }

        private void processRemaining(ByteBuffer buff) {
            long k1 = 0;
            long k2 = 0;
            length += buff.remaining();
            switch (buff.remaining()) {
            case 15:
                k2 ^= (long) toInt(buff.get(14)) << 48; // fall through
            case 14:
                k2 ^= (long) toInt(buff.get(13)) << 40; // fall through
            case 13:
                k2 ^= (long) toInt(buff.get(12)) << 32; // fall through
            case 12:
                k2 ^= (long) toInt(buff.get(11)) << 24; // fall through
            case 11:
                k2 ^= (long) toInt(buff.get(10)) << 16; // fall through
            case 10:
                k2 ^= (long) toInt(buff.get(9)) << 8; // fall through
            case 9:
                k2 ^= (long) toInt(buff.get(8)); // fall through
            case 8:
                k1 ^= buff.getLong();
                break;
            case 7:
                k1 ^= (long) toInt(buff.get(6)) << 48; // fall through
            case 6:
                k1 ^= (long) toInt(buff.get(5)) << 40; // fall through
            case 5:
                k1 ^= (long) toInt(buff.get(4)) << 32; // fall through
            case 4:
                k1 ^= (long) toInt(buff.get(3)) << 24; // fall through
            case 3:
                k1 ^= (long) toInt(buff.get(2)) << 16; // fall through
            case 2:
                k1 ^= (long) toInt(buff.get(1)) << 8; // fall through
            case 1:
                k1 ^= (long) toInt(buff.get(0));
                break;
            default:
                throw new AssertionError("Should never get here.");
            }
            h1 ^= mixK1(k1);
            h2 ^= mixK2(k2);
        }
    }

    public static class IntHasher extends Hasher<Integer> {
        public IntHasher() {
            super();
        }

        public IntHasher(Integer key, long seed) {
            super(key, seed);
        }

        @Override
        protected Hasher<Integer> clone() {
            return new IntHasher();
        }

        @Override
        protected void processIt(Integer key) {
            process(key);
        }

    }

    public static class LongHasher extends Hasher<Long> {

        public LongHasher() {
        }

        public LongHasher(Long key, long seed) {
            super(key, seed);
        }

        @Override
        protected Hasher<Long> clone() {
            return new LongHasher();
        }

        @Override
        protected void processIt(Long key) {
            process(key);
        }

    }

    public static class StringHasher extends Hasher<String> {

        public StringHasher() {
        }

        public StringHasher(String key, long seed) {
            super(key, seed);
        }

        @Override
        protected StringHasher clone() {
            return new StringHasher();
        }

        @Override
        protected void processIt(String key) {
            process(key);
        }

    }

    public static final long MERSENNE_31 = (long) (Math.pow(2, 32) - 1); // 2147483647

    /**
     * @param k - the number of hashes
     * @param m - the number of entries, bits or counters that K hashes to
     * @param n - the number of elements in the set
     * 
     * @return the false positive probability for the specified number of hashes K,
     *         population M entries and N elements
     */
    public static double fpp(int k, int m, int n) {
        double Kd = (double) k;
        double Md = (double) m;
        double Nd = (double) n;
        return Math.pow(1 - Math.exp(-Kd / (Md / Nd)), Kd);
    }

    /**
     * @param m   - the number of entries (bits)
     * @param k   - the number of hahes
     * @param fpp - the false positive probability
     * @return the number of elements that a bloom filter can hold with M entries
     *         (bits) K hashes and the specified false positive rate
     */
    public static int n(int m, int k, double fpp) {
        double Kd = (double) k;
        double Md = (double) m;
        return (int) Math.ceil(Md / (-Kd / Math.log(1 - Math.exp(Math.log(fpp) / Kd))));
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
    public static int optimalK(long n, long m) {
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
    public static int optimalM(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return Math.max(8, (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2))));
    }

    public final int  k;
    public final int  m;
    public final long seed;

    protected final Hasher<M> hasher;

    public Hash(long seed, int n, double p) {
        m = optimalM(n, p);
        k = optimalK(n, m);
        this.seed = seed;
        hasher = newHasher();
    }

    public Hash(long seed, int k, int m) {
        this.seed = seed;
        this.k = k;
        this.m = m;
        hasher = newHasher();
    }

    public Hash<M> clone() {
        Hasher<M> clone = hasher.clone();
        return new Hash<M>(seed, k, m) {

            @Override
            protected Hasher<M> newHasher() {
                return clone;
            }
        };
    }

    public double fpp(int n) {
        return fpp(k, m, n);
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

    public int[] hashes(M key) {
        return hasher.hashes(k, key, m, seed);
    }

    public long identityHash(M key) {
        return hasher.identityHash(key, seed);
    }

    public IntStream locations(M key) {
        return hasher.locations(k, key, m, seed);
    }

    abstract protected Hasher<M> newHasher();
}
