/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.util.BitSet;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.Biff;
import com.salesfoce.apollo.proto.Biff.Builder;

/**
 * @author hal.hildebrand
 *
 */
public class BloomFilter {

    static double population(BitSet bitSet, int k, int m) {
        int oneBits = bitSet.cardinality();
        return -m / ((double) k) * Math.log(1 - oneBits / ((double) m));
    }

    private final BitSet       bits;
    private final HashFunction h;

    public BloomFilter(int seed, long n, double p) {
        this(new HashFunction(seed, n, p));
    }

    public BloomFilter(Biff bff) {
        bits = BitSet.valueOf(bff.getBits().toByteArray());
        h = new HashFunction(bff.getSeed(), bff.getM(), bff.getK());
    }

    public BloomFilter(HashFunction h) {
        this.h = h;
        bits = new BitSet(h.getM());
    }

    public void add(HashKey element) {
        h.put(element, bits);
    }

    public void clear() {
        bits.clear();
    }

    public boolean contains(HashKey element) {
        return h.mightContain(element, bits);
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

    public Biff toBff() {
        Builder builder = Biff.newBuilder()
                              .setSeed(h.getSeed())
                              .setM(h.getM())
                              .setK(h.getK())
                              .setBits(ByteString.copyFrom(bits.toByteArray()));
        return builder.build();
    }
}
