/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;

import java.util.BitSet;

import org.joou.ULong;

import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.crypto.Digest;

/**
 * Simplified Bloom filter for multiple types, with setable seeds and other
 * parameters.
 * 
 * @author hal.hildebrand
 *
 */
abstract public class BloomFilter<T> {

    public static class BytesBloomFilter extends BloomFilter<byte[]> {

        public BytesBloomFilter(long seed, int n, double p) {
            super(new Hash<byte[]>(seed, n, p) {
                @Override
                protected Hasher<byte[]> newHasher() {
                    return new BytesHasher();
                }
            });
        }

        public BytesBloomFilter(long seed, int m, int k, long[] bytes) {
            super(new Hash<byte[]>(seed, k, m) {
                @Override
                protected Hasher<byte[]> newHasher() {
                    return new BytesHasher();
                }
            }, BitSet.valueOf(bytes));
        }

        @Override
        protected int getType() {
            return BYTES;
        }
    }

    public static class DigestBloomFilter extends BloomFilter<Digest> {

        public DigestBloomFilter(long seed, int n, double p) {
            super(new Hash<Digest>(seed, n, p) {
                @Override
                protected Hasher<Digest> newHasher() {
                    return new DigestHasher();
                }
            });
        }

        public DigestBloomFilter(long seed, int m, int k, long[] bytes) {
            super(new Hash<Digest>(seed, k, m) {
                @Override
                protected Hasher<Digest> newHasher() {
                    return new DigestHasher();
                }
            }, BitSet.valueOf(bytes));
        }

        @Override
        protected int getType() {
            return DIGEST;
        }

    }

    public static class IntBloomFilter extends BloomFilter<Integer> {

        public IntBloomFilter(long seed, int n, double p) {
            super(new Hash<Integer>(seed, n, p) {
                @Override
                protected Hasher<Integer> newHasher() {
                    return new IntHasher();
                }
            });
        }

        public IntBloomFilter(long seed, int m, int k, long[] bits) {
            super(new Hash<Integer>(seed, k, m) {
                @Override
                protected Hasher<Integer> newHasher() {
                    return new IntHasher();
                }
            }, BitSet.valueOf(bits));
        }

        @Override
        protected int getType() {
            return INT;
        }

    }

    public static class LongBloomFilter extends BloomFilter<Long> {
        public LongBloomFilter(long seed, int n, double p) {
            super(new Hash<Long>(seed, n, p) {
                @Override
                protected Hasher<Long> newHasher() {
                    return new LongHasher();
                }
            });
        }

        public LongBloomFilter(long seed, int m, int k, long[] bits) {
            super(new Hash<Long>(seed, k, m) {
                @Override
                protected Hasher<Long> newHasher() {
                    return new LongHasher();
                }
            }, BitSet.valueOf(bits));
        }

        @Override
        protected int getType() {
            return LONG;
        }

    }

    public static class StringBloomFilter extends BloomFilter<String> {

        public StringBloomFilter(long seed, int n, double p) {
            super(new Hash<String>(seed, n, p) {
                @Override
                protected Hasher<String> newHasher() {
                    return new StringHasher();
                }
            });
        }

        public StringBloomFilter(long seed, int m, int k, long[] bytes) {
            super(new Hash<String>(seed, k, m) {
                @Override
                protected Hasher<String> newHasher() {
                    return new StringHasher();
                }
            }, BitSet.valueOf(bytes));
        }

        @Override
        protected int getType() {
            return STRING;
        }
    }

    public static class ULongBloomFilter extends BloomFilter<ULong> {
        public ULongBloomFilter(long seed, int n, double p) {
            super(new Hash<ULong>(seed, n, p) {
                @Override
                protected Hasher<ULong> newHasher() {
                    return new ULongHasher();
                }
            });
        }

        public ULongBloomFilter(long seed, int m, int k, long[] bits) {
            super(new Hash<ULong>(seed, k, m) {
                @Override
                protected Hasher<ULong> newHasher() {
                    return new ULongHasher();
                }
            }, BitSet.valueOf(bits));
        }

        @Override
        protected int getType() {
            return ULONG;
        }

    }

    private static final int BYTES  = 3;
    private static final int DIGEST = 0;
    private static final int INT    = 1;
    private static final int LONG   = 2;
    private static final int STRING = 4;
    private static final int ULONG  = 5;

    @SuppressWarnings("unchecked")
    public static <Q> BloomFilter<Q> create(long seed, int n, double p, int type) {
        switch (type) {
        case DIGEST:
            return (BloomFilter<Q>) new DigestBloomFilter(seed, n, p);
        case INT:
            return (BloomFilter<Q>) new IntBloomFilter(seed, n, p);
        case LONG:
            return (BloomFilter<Q>) new LongBloomFilter(seed, n, p);
        case BYTES:
            return (BloomFilter<Q>) new BytesBloomFilter(seed, n, p);
        case STRING:
            return (BloomFilter<Q>) new StringBloomFilter(seed, n, p);
        case ULONG:
            return (BloomFilter<Q>) new ULongBloomFilter(seed, n, p);
        default:
            throw new IllegalArgumentException("Invalid type: " + type);
        }
    }

    @SuppressWarnings("unchecked")
    public static <Q> BloomFilter<Q> create(long seed, int m, int k, long[] bits, int type) {
        switch (type) {
        case DIGEST:
            return (BloomFilter<Q>) new DigestBloomFilter(seed, m, k, bits);
        case INT:
            return (BloomFilter<Q>) new IntBloomFilter(seed, m, k, bits);
        case LONG:
            return (BloomFilter<Q>) new LongBloomFilter(seed, m, k, bits);
        case BYTES:
            return (BloomFilter<Q>) new BytesBloomFilter(seed, m, k, bits);
        case STRING:
            return (BloomFilter<Q>) new StringBloomFilter(seed, m, k, bits);
        case ULONG:
            return (BloomFilter<Q>) new ULongBloomFilter(seed, m, k, bits);
        default:
            throw new IllegalArgumentException("Invalid type: " + type);
        }
    }

    public static <Q> BloomFilter<Q> from(Biff bff) {
        long[] bits = new long[bff.getBitsCount()];
        int i = 0;
        for (long l : bff.getBitsList()) {
            bits[i++] = l;
        }
        return create(bff.getSeed(), bff.getM(), bff.getK(), bits, bff.getType());
    }

    private static double population(BitSet bitSet, int k, int m) {
        int oneBits = bitSet.cardinality();
        return -m / ((double) k) * Math.log(1 - oneBits / ((double) m));
    }

    private final BitSet  bits;
    private final Hash<T> h;

    private BloomFilter(Hash<T> h) {
        this(h, new BitSet(h.getM()));
    }

    private BloomFilter(Hash<T> h, BitSet bits) {
        this.h = h;
        this.bits = bits;
    }

    public void add(T element) {
        for (int hash : h.hashes(element)) {
            bits.set(hash);
        }
    }

    public void clear() {
        bits.clear();
    }

    public boolean contains(T element) {
        for (int hash : h.hashes(element)) {
            if (!bits.get(hash)) {
                return false;
            }
        }
        return true;
    }

    public double fpp(int n) {
        return h.fpp(n);
    }

    /**
     * Estimates the current population of the Bloom filter (see:
     * http://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter
     *
     * @return the estimated amount of elements in the filter
     */
    public double getEstimatedPopulation() {
        return population(bits, h.getK(), h.getM());
    }

    public Biff toBff() {
        Biff.Builder builder = Biff.newBuilder().setSeed(h.getSeed()).setM(h.getM()).setK(h.getK()).setType(getType());

        for (long l : bits.toLongArray()) {
            builder.addBits(l);
        }
        return builder.build();
    }

    protected abstract int getType();
}
