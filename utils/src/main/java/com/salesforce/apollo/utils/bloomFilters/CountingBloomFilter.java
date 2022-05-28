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
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.utils.Entropy;

/**
 * 
 * @author hal.hildebrand
 *
 */
public abstract class CountingBloomFilter<T extends Comparable<T>> {

    record ComparisonResult(int comparison, double fpr) {}

    record Comparison(int compared, int sumA, int sumB) {}

    public final static long DEFAULT_GOOD_SEED = Entropy.nextBitsStreamLong();
    public final static int  DEFAULT_K         = 3;
    public final static int  DEFAULT_M         = 200;
    public final static int  MASK              = 0x0F;

    private final static Logger log = LoggerFactory.getLogger(CountingBloomFilter.class);

    public static boolean validate(int m, byte[] counts) {
        return m == counts.length;
    }

    static Comparison compareWith(long abcPrefix, byte[] abcCounts, long bcbPrefix, byte[] bcbCounts) {
        int sumA = 0;
        int sumB = 0;
        int lessThan = 0;
        int greaterThan = 0;

        // only one is > 0 if any
        int aBias = 0;
        int bBias = 0;

        int prefixCompare = Long.compareUnsigned(abcPrefix, bcbPrefix);
        int m = m(abcCounts);
        if (prefixCompare < 0) {
            long preDiff = bcbPrefix - abcPrefix;
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                return new Comparison(1, 0, MASK * m);
            }
            bBias = (int) (preDiff & MASK);
        } else if (prefixCompare > 0) {
            long preDiff = abcPrefix - bcbPrefix;
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                return new Comparison(-1, MASK * m, 0);
            }
            aBias = (int) (preDiff & MASK);
        }

        for (int i = 0; i < m; i++) {
            int a = count(i, abcCounts) + aBias;
            sumA += a;
            int b = count(i, bcbCounts) + bBias;
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
                return new Comparison(0, sumA + range(i + 1, m).map(x -> x + biasA).sum(),
                                      sumB + range(i + 1, m).map(x -> x + biasB).sum());
            }
        }

        if (greaterThan != 0) {
            return new Comparison(1, sumA, sumB);
        }
        return lessThan == 0 ? new Comparison(0, sumA, sumB) : new Comparison(-1, sumA, sumB);
    }

    static int count(int index, byte[] counts) {
        return counts[index] & MASK;
    }

    static double falsePositiveRate(Comparison c, int m) {
        double x = Math.min(c.sumA(), c.sumB());
        double y = Math.max(c.sumA(), c.sumB());
        return Math.pow(1 - Math.pow(1.0 - (1.0 / m), x), y);
    }

    static ComparisonResult happenedBefore(long abcPrefix, byte[] abcCounts, long bcbPrefix, byte[] bcbCounts) {
        if (abcCounts.length != bcbCounts.length) {
            throw new IllegalArgumentException("Cannot compare as this cbf has a different count size than the specified cbf");
        }
        Comparison c = compareWith(abcPrefix, abcCounts, bcbPrefix, bcbCounts);
        int m = m(abcCounts);
        return switch (c.compared()) {
        case 0 -> new ComparisonResult(0, falsePositiveRate(c, m));
        case 1 -> new ComparisonResult(1, falsePositiveRate(c, m));
        case -1 -> new ComparisonResult(-1, falsePositiveRate(c, m));
        default -> throw new IllegalArgumentException("Unexpected comparison value: " + c.compared());
        };
    }

    static int m(byte[] counts) {
        return counts.length;
    }

    private final byte[] counts; // two cells per byte, giving 4 bits per cell

    private final Hash<T> hash;

    public CountingBloomFilter() {
        this(DEFAULT_GOOD_SEED, DEFAULT_K, DEFAULT_M);
    }

    public CountingBloomFilter(CountingBloomFilter<T> cbf, byte[] initialValues) {
        this(cbf.hash, initialValues);
    }

    public CountingBloomFilter(int[] initialValues) {
        this(DEFAULT_GOOD_SEED, initialValues, DEFAULT_K);
    }

    public CountingBloomFilter(long seed) {
        this(seed, DEFAULT_K, DEFAULT_M);
    }

    public CountingBloomFilter(long seed, byte[] counts, int k) {
        this.counts = counts;
        this.hash = newHash(seed, k, counts.length);
    }

    public CountingBloomFilter(long seed, int k, int m) {
        counts = new byte[m];
        hash = newHash(seed, k, m);
    }

    protected abstract Hash<T> newHash(long seed, int k, int m);

    /**
     *
     * @param seed          - the seed for the Hash function
     * @param initialValues - initial values of the cbf
     * @param k             - number of hashes
     */
    public CountingBloomFilter(long seed, int[] initialValues, int k) {
        if (IntStream.of(initialValues).max().getAsInt() > MASK) {
            throw new IllegalArgumentException("initial values contain values > " + MASK);
        }
        counts = new byte[initialValues.length];
        hash = newHash(seed, k, initialValues.length);
        int min = 0;
        if (IntStream.of(initialValues).map(i -> i - min).max().getAsInt() > MASK) {
            throw new IllegalArgumentException("Cannot represent with a valid cbf value.  Overflow.");
        }
        for (int i = 0; i < initialValues.length; i = i + 1) {
            set(i, initialValues[i] - min);
        }
    }

    private CountingBloomFilter(Hash<T> hash, byte[] counts) {
        this.hash = hash;
        this.counts = counts;
    }

    /**
     * Add a item to this cbf.
     */
    public void add(T item) {
        for (int h : hash.hashes(item)) {
            int count = count(h);
            if (count < MASK) {
                inc(h);
            } else {
                log.trace("Overflow");
            }
        }
    }

    public void addAll(Collection<T> digests) {
        digests.forEach(d -> add(d));
    }

    public double fpp(int n) {
        return hash.fpp(n);
    }

    public boolean isOrigin() {
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Destructively merge the specified filter with the receiver. The receiver's
     * new state is the max(receiver, bcb)
     */
    public void merge(CountingBloomFilter<T> bcb) {
        if (counts.length != bcb.counts.length) {
            throw new IllegalArgumentException("Cannot merge as this cbf has m: " + hash.m + " and B has m: "
            + bcb.hash.m);
        }

        int overall = 0;
        for (int i = 0; i < hash.m; i++) {
            int a = count(i);
            int b = count(i, bcb.counts);

            int max = Math.max(a, b);
            set(i, max);
            overall = Math.max(max, overall);
        }
    }

    public void reset() {
        Arrays.fill(counts, (byte) 0);
    }

    public int sum() {
        int sum = 0;
        for (int i = 0; i < hash.m; i++) {
            sum += count(i);
        }
        return sum;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
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

    private int count(int index) {
        return count(index, counts);
    }

    private void inc(int index) {
        counts[index] += 1;
    }

    private void set(int index, int value) {
        if (value < 0 || value > MASK) {
            throw new IllegalArgumentException();
        }
        counts[index] = (byte) (value & MASK);
    }
}
