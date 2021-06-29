/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;

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

    public static class ClockOverflowException extends Exception {
        private static final long serialVersionUID = 1L;

        public ClockOverflowException(String message) {
            super(message);
        }

    }

    record Comparison(int compared, int sumA, int sumB) {
    }

    record ComparisonResult(int comparison, double fpr) {
    }

    private final static Logger log = LoggerFactory.getLogger(BloomClock.class);

    static Hash<Digest> newHash(long seed, int m, int k) {
        return new Hash<Digest>(seed, m, k) {
            @Override
            Hasher<Digest> newHasher() {
                return new DigestHasher();
            }
        };
    }

    private final byte[]       counts;    // two cells per byte, giving 4 bits per cell
    private final Hash<Digest> hash;
    private long               prefix = 0;

    public BloomClock(long seed, int k, int m) {
        assert m % 2 == 0 : "must be an even number";
        counts = new byte[m / 2];
        hash = newHash(seed, m, k);
    }

    /**
     * 
     * @param seed          - the seed for the Hash function
     * @param initialValues - initial values of the clock
     * @param k             - number of hashes
     */
    public BloomClock(long seed, int[] initialValues, int k) {
        assert initialValues.length % 2 == 0 : "must be an even number";

        counts = new byte[initialValues.length / 2];
        hash = newHash(seed, initialValues.length, k);
        int cell = 0;
        int min = IntStream.of(initialValues).min().getAsInt();
        prefix += min;
        if (IntStream.of(initialValues).map(i -> i - min).max().getAsInt() > 0x0F) {
            throw new IllegalArgumentException("Cannot represent with a valid clock value.  Overflow.");
        }
        for (int i = 0; i < initialValues.length; i = i + 2) {
            counts[cell++] = (byte) ((initialValues[i] - min) << 4 | (initialValues[i + 1] - min));
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
    public void add(Digest digest) throws ClockOverflowException {
        for (int hash : hash.hashes(digest)) {
            int cell = hash / 2;
            boolean high = hash % 2 == 0;
            byte count;
            if (high) {
                count = (byte) (counts[cell] >> 4);
            } else {
                count = (byte) (counts[cell] & 0x0F);
            }
            if (count == 0x0F) {
                rollPrefix();
            }
            if (high) {
                counts[cell] += 0x10;
            } else {
                counts[cell] += 0x01;
            }
        }
    }

    public BloomClock clone() {
        return new BloomClock(prefix, hash.clone(), Arrays.copyOf(counts, counts.length));
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
    public BloomClock merge(BloomClock bbc) throws ClockOverflowException {
        return clone().mergeWith(bbc);
    }

    /**
     * Destructively merge the specified clock with the receiver
     */
    public BloomClock mergeWith(BloomClock bbc) throws ClockOverflowException {
        if (counts.length != bbc.counts.length) {
            throw new IllegalArgumentException(
                    "Cannot merge as this clock has m: " + hash.m + " and B has m: " + bbc.hash.m);
        }
        prefix += bbc.prefix;
        int overall = 0;
        for (int i = 0; i < counts.length; i++) {
            byte a = counts[i];
            byte b = bbc.counts[i];
            byte max = (byte) ((byte) Math.max(a >> 4, b >> 4) << 4);
            counts[i] = (byte) (max | Math.max((a & 0x0F), b & 0x0F));
            overall = Math.max(max, overall);
        }
        if (overall == 0xFF) {
            rollPrefix();
        }
        return this;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (Long.compareUnsigned(prefix, 0) > 0) {
            buff.append("(");
            buff.append(Long.toUnsignedString(prefix));
            buff.append(")");
        }
        buff.append("[");
        boolean comma = false;
        for (byte b : counts) {
            if (comma) {
                buff.append(',');
            }
            buff.append((b >> 4));
            buff.append(',');
            buff.append((b & 0x0F));
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
        int aBias = 0;
        int bBias = 0;

        int prefixCompare = Long.compareUnsigned(prefix, bbc.prefix);
        if (prefixCompare < 0) {
            long preDiff = bbc.prefix - prefix;
            if (Long.compareUnsigned(preDiff, 0x0F) > 0) {
                return new Comparison(1, 0, 0x0F * hash.m);
            }
            bBias = (int) (preDiff & Integer.MAX_VALUE);
        } else if (prefixCompare > 0) {
            long preDiff = prefix - bbc.prefix;
            if (Long.compareUnsigned(preDiff, 0x0F) > 0) {
                return new Comparison(-1, 0x0F * hash.m, 0);
            }
            aBias = (int) (preDiff & Integer.MAX_VALUE);
        }

        for (int i = 0; i < counts.length; i++) {
            int a;
            int b;
            // high nibble
            a = (counts[i] >> 4) + aBias;
            sumA += a;
            b = (bbc.counts[i] >> 4) + bBias;
            sumB += b;

            int diff = b - a;
            if (diff > 0) {
                lessThan++;
            } else if (diff < 0) {
                greaterThan++;
            }

            // low nibble
            a = (counts[i] & 0x0F) + aBias;
            sumA += a;
            b = (bbc.counts[i] & 0x0F) + bBias;
            sumB += b;

            diff = b - a;
            if (diff > 0) {
                lessThan++;
            } else if (diff < 0) {
                greaterThan++;
            }
        }
        if (lessThan != 0) {
            if (greaterThan != 0) {
                return new Comparison(0, sumA, sumB);
            }
            return new Comparison(-1, sumA, sumB);
        }
        if (greaterThan != 0) {
            return new Comparison(1, sumA, sumB);
        }
        return new Comparison(-1, sumA, sumB);
    }

    private double falsePositiveRate(Comparison c) {
        double x = Math.min(c.sumA, c.sumB);
        double y = Math.max(c.sumA, c.sumB);
        return Math.pow(1 - Math.pow(1.0 - (1.0 / (double) hash.m), x), y);
    }

    private ComparisonResult happenedBefore(BloomClock bbc) {
        if (counts.length != bbc.counts.length) {
            throw new IllegalArgumentException(
                    "Cannot compare as this clock has m: " + hash.m + " and B has m: " + bbc.hash.m);
        }
        Comparison c = compareWith(bbc);
        return switch (c.compared) {
        case 0 -> new ComparisonResult(0, 1.0);
        case 1 -> new ComparisonResult(1, 1.0);
        case -1 -> new ComparisonResult(-1, falsePositiveRate(c));
        default -> throw new IllegalArgumentException("Unexpected comparison value: " + c.compared);
        };
    }

    private void rollPrefix() throws ClockOverflowException {
        int min = 0x0F;
        for (byte cell : counts) {
            int tst = cell & 0x0F;
            min = Math.min(tst, min);
            tst = cell >> 4;
            min = Math.min(tst, min);
        }
        if (min == 0x00) {
            log.info("Cannot roll prefix");
            return;
        }
        if (prefix == -1L) {
            throw new ClockOverflowException("Prefix has reached maximum count (2^64)");
        }
        byte removed = (byte) (((byte) min << 4) | min);
        prefix -= min;
        for (int i = 0; i < counts.length; i++) {
            counts[i] -= removed;
        }
    }
}
