/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;

import static java.util.stream.IntStream.range;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.utils.proto.BC;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.utils.Utils;

/**
 *
 * The BloomClock implements the scheme outlined in the excellent paper By Lum
 * Ramabaja, <a href="https://arxiv.org/abs/1905.13064">The Bloom Clock</a>
 * <p>
 * The BloomClock represents a partial ordering that can be used in the same
 * fashion as Vector Clocks. The Bloom Clock tracks histories of observed
 * Digests through a counting bloom filter. Bloom clocks can be compared to see
 * if one preceded the other. Because the Bloom Clock is based on a
 * probabalistic data structure - i.e. the bloom filter - there are false
 * positives that can result when comparing two Bloom Clocks.
 * <p>
 * This implementation is based on a 4 bit counting bloom filter. To handle
 * overflow a Long prefix is kept. Periodically in insertion, the current count
 * will be renormalized, subtracting a common minimum and adding this to the
 * prefix. This allows the Bloom Clock to track 2^65 insertions (2^64 + one more
 * byte).
 *
 * @author hal.hildebrand
 *
 */
public class BloomClock {

    /**
     * A comparator for Bloom Clocks. Because the Bloom Clock is a probabalistic
     * data structure, this comparator requires a provided <b>false positive
     * rate</b>. This FPR applies when clock A is compared to clock B and the
     * determination is that A proceeds B. This determination is, however,
     * probabalistic in that there is still a possibility this is a false positive
     * (the past is "included" in the present and future, so the FPR applies to the
     * "proceeded" relationship). The supplied FPR must be greater than or equal to
     * the calculated FPR of the comparison.
     *
     */
    public static class BcComparator implements Comparator<BloomClock> {
        private final double fpr;

        /**
         *
         * @param fpr - the False Positive Rate. Acceptable probability from 0.0 -> 1.0
         *            of a false positive when determining precidence.
         */
        public BcComparator(double fpr) {
            this.fpr = fpr;
        }

        /**
         * Provides comparison between two Bloom Clocks. The comparator has a false
         * positive threshold that determines the acceptable threshold of assurance that
         * clock A proceeds clock B.
         * <p>
         * If clock A is ordered after clock B, then this function returns 1
         * <p>
         * If clocks A and B are not comparable, i.e. they are "simultaneous", then this
         * function returns 0.
         * <p>
         * If clock A proceeds B within this comparator's false positive rate, then this
         * function returns -1.
         * <p>
         *
         */
        @Override
        public int compare(BloomClock a, BloomClock b) {
            var comparison = a.happenedBefore(b);
            if (comparison.comparison >= 0) {
                return comparison.comparison;
            }
            return comparison.fpr <= fpr ? -1 : 0;
        }

    }

    record Comparison(int compared, int sumA, int sumB) {
    }

    record ComparisonResult(int comparison, double fpr) {
    }

    public static long      DEFAULT_GOOD_SEED = Utils.bitStreamEntropy().nextLong();
    public final static int DEFAULT_K         = 4;
    public static final int DEFAULT_M         = 107;

    private final static Logger log  = LoggerFactory.getLogger(BloomClock.class);
    private static int          MASK = 0x0F;

    static Hash<Digest> newHash(long seed, int k, int m) {
        return new Hash<>(seed, k, m) {
            @Override
            Hasher<Digest> newHasher() {
                return new DigestHasher();
            }
        };
    }

    private final byte[] counts; // two cells per byte, giving 4 bits per cell

    private final Hash<Digest> hash;

    private long prefix = 0;

    public BloomClock() {
        this(DEFAULT_GOOD_SEED, DEFAULT_K, DEFAULT_M);
    }

    public BloomClock(BC bc) {
        hash = newHash(bc.getSeed(), bc.getK(), bc.getM());
        counts = bc.toByteArray();
        prefix = bc.getPrefix();
    }

    public BloomClock(int[] initialValues) {
        this(DEFAULT_GOOD_SEED, initialValues, DEFAULT_K);
    }

    public BloomClock(long seed) {
        this(seed, DEFAULT_K, DEFAULT_M);
    }

    public BloomClock(long seed, int k, int m) {
        counts = new byte[m];
        hash = newHash(seed, k, m);
    }

    /**
     *
     * @param seed          - the seed for the Hash function
     * @param initialValues - initial values of the clock
     * @param k             - number of hashes
     */
    public BloomClock(long seed, int[] initialValues, int k) {
        if (IntStream.of(initialValues).max().getAsInt() > MASK) {
            throw new IllegalArgumentException("initial values contain values > " + MASK);
        }
        counts = new byte[initialValues.length];
        hash = newHash(seed, k, initialValues.length);
        int min = 0;
        prefix += min;
        if (IntStream.of(initialValues).map(i -> i - min).max().getAsInt() > MASK) {
            throw new IllegalArgumentException("Cannot represent with a valid clock value.  Overflow.");
        }
        for (int i = 0; i < initialValues.length; i = i + 1) {
            set(i, initialValues[i] - min);
        }
    }

    private BloomClock(long prefix, Hash<Digest> hash, byte[] counts) {
        this.hash = hash;
        this.counts = counts;
        this.prefix = prefix;
    }

    /**
     * Add a digest to this clock. This should be done only once per unique digest.
     */
    public void add(Digest digest) {
        boolean roll = false;
        for (int hash : hash.hashes(digest)) {
            int count = count(hash);
            if (count + 1 == MASK) {
                roll = true;
            }
            inc(hash);
        }
        if (roll) {
            rollPrefix();
        }
    }

    public void addAll(Collection<Digest> digests) {
        digests.forEach(d -> add(d));
    }

    @Override
    public BloomClock clone() {
        return new BloomClock(prefix, hash.clone(), Arrays.copyOf(counts, counts.length));
    }

    public BloomClock construct(long seed) {
        return new BloomClock(seed, DEFAULT_K, DEFAULT_M);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BloomClock)) {
            return false;
        }
        BloomClock other = (BloomClock) obj;
        return Arrays.equals(counts, other.counts) && prefix == other.prefix;
    }

    public long getPrefix() {
        return prefix;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(counts);
        result = prime * result + Objects.hash(prefix);
        return result;
    }

    /**
     * @return a new Bloom Clock that represents the merged result of the receiver
     *         and the specified clock
     */
    public BloomClock merge(BloomClock bbc) {
        return clone().mergeWith(bbc);
    }

    /**
     * Destructively merge the specified clock with the receiver
     */
    public BloomClock mergeWith(BloomClock bbc) {
        if (counts.length != bbc.counts.length) {
            throw new IllegalArgumentException(
                    "Cannot merge as this clock has m: " + hash.m + " and B has m: " + bbc.hash.m);
        }
        // only one is > 0 if any
        int aBias = 0;
        int bBias = 0;

        // Merge prefixes
        int prefixCompare = Long.compareUnsigned(prefix, bbc.prefix);
        if (prefixCompare < 0) {
            long preDiff = bbc.prefix - prefix;
            prefix = bbc.prefix;
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                for (int i = 0; i < counts.length; i++) {
                    counts[i] = bbc.counts[i];
                }
                return this;
            }
            bBias = (int) (preDiff & MASK);
        } else if (prefixCompare > 0) {
            long preDiff = prefix - bbc.prefix;
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                return this;
            }
            aBias = (int) (preDiff & MASK);
        }

        int overall = 0;
        for (int i = 0; i < hash.m; i++) {
            int a = count(i) + aBias;
            int b = bbc.count(i) + bBias;

            int max = Math.max(a, b) - aBias - bBias;
            set(i, max);
            overall = Math.max(max, overall);
        }
        if (overall == 0xFF) {
            rollPrefix();
        }
        return this;
    }

    public BC toBC() {
        return BC.newBuilder()
                 .setSeed(hash.seed)
                 .setK(hash.k)
                 .setM(hash.m)
                 .setPrefix(prefix)
                 .setCounts(ByteString.copyFrom(counts))
                 .build();
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (Long.compareUnsigned(prefix, 0) > 0) {
            buff.append("(");
            buff.append(Long.toUnsignedString(prefix));
            buff.append(")");
        }
        buff.append("[");
        boolean comma = false;
        for (int i = 0; i < hash.m; i++) {
            if (comma) {
                buff.append(',');
            }
            buff.append(count(i));
            comma = true;
        }
        buff.append("]");
        return buff.toString();
    }

    private Comparison compareWith(BloomClock bbc) {

        int sumA = 0;
        int sumB = 0;
        int lessThan = 0;
        int greaterThan = 0;

        // only one is > 0 if any
        int aBias = 0;
        int bBias = 0;

        int prefixCompare = Long.compareUnsigned(prefix, bbc.prefix);
        if (prefixCompare < 0) {
            long preDiff = bbc.prefix - prefix;
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                return new Comparison(1, 0, MASK * hash.m);
            }
            bBias = (int) (preDiff & MASK);
        } else if (prefixCompare > 0) {
            long preDiff = prefix - bbc.prefix;
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                return new Comparison(-1, MASK * hash.m, 0);
            }
            aBias = (int) (preDiff & MASK);
        }

        for (int i = 0; i < hash.m; i++) {
            int a = count(i) + aBias;
            sumA += a;
            int b = bbc.count(i) + bBias;
            sumB += b;
            int diff = b - a;
            if (diff > 0) {
                lessThan++;
            } else if (diff < 0) {
                greaterThan++;
            }
            if (greaterThan != 0 && lessThan != 0) {
                int biasA = aBias;
                int biasB = bBias;
                return new Comparison(0, sumA + range(i + 1, hash.m).map(x -> x + biasA).sum(),
                        sumB + range(i + 1, hash.m).map(x -> x + biasB).sum());
            }
        }

        if (greaterThan != 0) {
            return new Comparison(1, sumA, sumB);
        }
        return new Comparison(-1, sumA, sumB);
    }

    private int count(int index) {
        return counts[index] & MASK;
    }

    private double falsePositiveRate(Comparison c) {
        double x = Math.min(c.sumA, c.sumB);
        double y = Math.max(c.sumA, c.sumB);
        return Math.pow(1 - Math.pow(1.0 - (1.0 / hash.m), x), y);
    }

    private ComparisonResult happenedBefore(BloomClock bbc) {
        if (counts.length != bbc.counts.length) {
            throw new IllegalArgumentException(
                    "Cannot compare as this clock has m: " + hash.m + " and B has m: " + bbc.hash.m);
        }
        Comparison c = compareWith(bbc);
        return switch (c.compared) {
        case 0 -> new ComparisonResult(0, falsePositiveRate(c));
        case 1 -> new ComparisonResult(1, falsePositiveRate(c));
        case -1 -> new ComparisonResult(-1, falsePositiveRate(c));
        default -> throw new IllegalArgumentException("Unexpected comparison value: " + c.compared);
        };
    }

    private void inc(int index) {
        counts[index] += 1;
    }

    private void rollPrefix() {
        int min = MASK;
        for (int i = 0; i < hash.m; i++) {
            int count = count(i);
            min = Math.min(min, count);
        }
        if (min == 0x00) {
            log.trace("Overflow");
            return;
        }
        if (prefix == -1L) {
            log.info("Prefix already at max, you win the internet");
            return;
        }
        prefix += min;
        for (int i = 0; i < counts.length; i++) {
            set(i, count(i) - min);
        }
    }

    private void set(int index, int value) {
        if (value < 0 || value > MASK) {
            throw new IllegalArgumentException();
        }
        counts[index] = (byte) (value & MASK);
    }
}
