/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.util.BitSet;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.utils.Hash.Hasher.DigestHasher;
import com.salesforce.apollo.utils.Hash.Hasher.IntHasher;
import com.salesforce.apollo.utils.Hash.Hasher.LongHasher;

/**
 * Simplified Bloom filter for multiple types, with setable seeds and other
 * parameters.
 * 
 * @author hal.hildebrand
 *
 */
abstract public class BloomFilter<T> {
    public static class DigestBloomFilter extends BloomFilter<Digest> {

        public DigestBloomFilter(long seed, int n, double p) {
            super(new Hash<Digest>(seed, n, p) {
                @Override
                Hasher<Digest> newHasher(Digest key) {
                    return new DigestHasher(key, seed);
                }
            });
        }

        public DigestBloomFilter(long seed, int m, int k, ByteString bits) {
            super(new Hash<Digest>(seed, m, k) {
                @Override
                Hasher<Digest> newHasher(Digest key) {
                    return new DigestHasher(key, seed);
                }
            }, BitSet.valueOf(bits.toByteArray()));
        }

        @Override
        protected int getType() {
            return 0;
        }

    }

    public static class IntBloomFilter extends BloomFilter<Integer> {

        public IntBloomFilter(long seed, int n, double p) {
            super(new Hash<Integer>(seed, n, p) {
                @Override
                Hasher<Integer> newHasher(Integer key) {
                    return new IntHasher(key, seed);
                }
            });
        }

        public IntBloomFilter(long seed, int m, int k, ByteString bits) {
            super(new Hash<Integer>(seed, m, k) {
                @Override
                Hasher<Integer> newHasher(Integer key) {
                    return new IntHasher(key, seed);
                }
            }, BitSet.valueOf(bits.toByteArray()));
        }

        @Override
        protected int getType() {
            return 1;
        }

    }

    public static class LongBloomFilter extends BloomFilter<Long> {
        public LongBloomFilter(long seed, int n, double p) {
            super(new Hash<Long>(seed, n, p) {
                @Override
                Hasher<Long> newHasher(Long key) {
                    return new LongHasher(key, seed);
                }
            });
        }

        public LongBloomFilter(long seed, int m, int k, ByteString bits) {
            super(new Hash<Long>(seed, m, k) {
                @Override
                Hasher<Long> newHasher(Long key) {
                    return new LongHasher(key, seed);
                }
            }, BitSet.valueOf(bits.toByteArray()));
        }

        @Override
        protected int getType() {
            return 2;
        }

    }

    @SuppressWarnings("unchecked")
    public static <Q> BloomFilter<Q> create(long seed, int n, double p, int type) {
        switch (type) {
        case 0:
            return (BloomFilter<Q>) new DigestBloomFilter(seed, n, p);
        case 1:
            return (BloomFilter<Q>) new IntBloomFilter(seed, n, p);
        case 2:
            return (BloomFilter<Q>) new LongBloomFilter(seed, n, p);
        default:
            throw new IllegalArgumentException("Invalid type: " + type);
        }
    }

    @SuppressWarnings("unchecked")
    public static <Q> BloomFilter<Q> create(long seed, int m, int k, ByteString bits, int type) {
        switch (type) {
        case 0:
            return (BloomFilter<Q>) new DigestBloomFilter(seed, m, k, bits);
        case 1:
            return (BloomFilter<Q>) new IntBloomFilter(seed, m, k, bits);
        case 2:
            return (BloomFilter<Q>) new LongBloomFilter(seed, m, k, bits);
        default:
            throw new IllegalArgumentException("Invalid type: " + type);
        }
    }

    public static <Q> BloomFilter<Q> from(Biff bff) {
        return create(bff.getSeed(), bff.getM(), bff.getK(), bff.getBits(), bff.getType());
    }

    public static <Q> BloomFilter<Q> from(ByteString encoded) {
        try {
            return from(Biff.parseFrom(encoded));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("invalid bloom filter serialization", e);
        }
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
        h.process(element, hash -> {
            bits.set(hash);
        });
    }

    public void clear() {
        bits.clear();
    }

    public boolean contains(T element) {
        return h.process(element, hash -> {
            return bits.get(hash);
        });
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
        return Biff.newBuilder()
                   .setSeed(h.getSeed())
                   .setM(h.getM())
                   .setK(h.getK())
                   .setBits(ByteString.copyFrom(bits.toByteArray()))
                   .setType(getType())
                   .build();
    }

    protected abstract int getType();
}
