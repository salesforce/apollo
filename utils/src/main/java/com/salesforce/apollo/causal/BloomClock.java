/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.causal;

import static java.util.stream.IntStream.range;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.utils.proto.BloomeClock;
import com.salesfoce.apollo.utils.proto.Clock;
import com.salesfoce.apollo.utils.proto.StampedBloomeClock;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.utils.BUZ;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.Hash;

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
 * overflow a Long prefix is kept. Periodically in insertion, the current counts
 * will be renormalized, subtracting a common minimum and adding this to the
 * prefix. This allows the Bloom Clock to track approximately 2^64 events
 *
 * @author hal.hildebrand
 *
 */
public class BloomClock implements ClockValue {

    record ComparisonResult(int comparison, double fpr) {}

    record Comparison(int compared, int sumA, int sumB) {}

    public static long      DEFAULT_GOOD_SEED = Utils.bitStreamEntropy().nextLong();
    public final static int DEFAULT_K         = 3;
    public static final int DEFAULT_M         = 200;
    public static int       MASK              = 0x0F;

    private final static Logger log = LoggerFactory.getLogger(BloomClock.class);

    public static String print(BloomClockValue clock) {
        StringBuilder buff = new StringBuilder();
        if (Long.compareUnsigned(clock.prefix(), 0) > 0) {
            buff.append("(");
            buff.append(Long.toUnsignedString(clock.prefix()));
            buff.append(")");
        }
        buff.append("[");
        boolean comma = false;
        byte[] counts = clock.counts();
        for (int i = 0; i < m(counts); i++) {
            if (comma) {
                buff.append(',');
            }
            buff.append(count(i, counts));
            comma = true;
        }
        buff.append("]");
        return buff.toString();
    }

    public static boolean validate(int m, byte[] counts) {
        return m == counts.length;
    }

    static Comparison compareWith(long abcPrefix, byte[] abcCounts, long bbcPrefix, byte[] bbcCounts) {
        int sumA = 0;
        int sumB = 0;
        int lessThan = 0;
        int greaterThan = 0;

        // only one is > 0 if any
        int aBias = 0;
        int bBias = 0;

        int prefixCompare = Long.compareUnsigned(abcPrefix, bbcPrefix);
        int m = m(abcCounts);
        if (prefixCompare < 0) {
            long preDiff = bbcPrefix - abcPrefix;
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                return new Comparison(1, 0, MASK * m);
            }
            bBias = (int) (preDiff & MASK);
        } else if (prefixCompare > 0) {
            long preDiff = abcPrefix - bbcPrefix;
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                return new Comparison(-1, MASK * m, 0);
            }
            aBias = (int) (preDiff & MASK);
        }

        for (int i = 0; i < m; i++) {
            int a = count(i, abcCounts) + aBias;
            sumA += a;
            int b = count(i, bbcCounts) + bBias;
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

    static ComparisonResult happenedBefore(long abcPrefix, byte[] abcCounts, long bbcPrefix, byte[] bbcCounts) {
        if (abcCounts.length != bbcCounts.length) {
            throw new IllegalArgumentException("Cannot compare as this clock has a different count size than the specified clock");
        }
        Comparison c = compareWith(abcPrefix, abcCounts, bbcPrefix, bbcCounts);
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

    static Hash<Digest> newHash(int k, int m) {
        return new Hash<>(BUZ.buzhash(0), k, m) {
            @Override
            protected Hasher<Digest> newHasher() {
                return new DigestHasher();
            }
        };
    }

    private final byte[] counts; // two cells per byte, giving 4 bits per cell

    private final Hash<Digest> hash;

    private long prefix = 0;

    public BloomClock() {
        this(DEFAULT_K, DEFAULT_M);
    }

    public BloomClock(BloomClock clock, byte[] initialValues) {
        this(clock.prefix, clock.hash, initialValues);
    }

    public BloomClock(BloomeClock clock) {
        this(clock.getPrefix(), newHash(clock.getK(), clock.getCounts().size()), clock.getCounts().toByteArray());
    }

    public BloomClock(int[] initialValues) {
        this(DEFAULT_GOOD_SEED, initialValues, DEFAULT_K);
    }

    public BloomClock(byte[] counts, int k) {
        this.counts = counts;
        this.hash = newHash(k, counts.length);
    }

    public BloomClock(Clock clock, int k, int m) {
        byte[] initialCounts = clock.getCounts().toByteArray();
        if (m(initialCounts) != m) {
            throw new IllegalArgumentException("invalid counts.length: " + m(initialCounts) + " expected: " + m);
        }
        prefix = clock.getPrefix();
        counts = initialCounts;
        hash = newHash(k, m);
    }

    public BloomClock(int k, int m) {
        counts = new byte[m];
        hash = newHash(k, m);
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
        hash = newHash(k, initialValues.length);
        int min = 0;
        prefix += min;
        if (IntStream.of(initialValues).map(i -> i - min).max().getAsInt() > MASK) {
            throw new IllegalArgumentException("Cannot represent with a valid clock value.  Overflow.");
        }
        for (int i = 0; i < initialValues.length; i = i + 1) {
            set(i, initialValues[i] - min);
        }
    }

    public BloomClock(StampedBloomeClock clock) {
        this(clock.getClock().getPrefix(), newHash(clock.getClock().getK(), clock.getClock().getCounts().size()),
             clock.getClock().getCounts().toByteArray());
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

    @Override
    public ComparisonResult compareTo(ClockValue b) {
        BloomClockValue bbc = b.toBloomClockValue();
        return happenedBefore(prefix, counts, bbc.prefix(), bbc.counts());
    }

    /**
     * Answer an immutable ClockValue of the current state of the receiver
     */
    public BloomClockValue current() {
        return new BloomClockValue(prefix, Arrays.copyOf(counts, counts.length));
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

    public double fpp(int n) {
        return hash.fpp(n);
    }

    public long getPrefix() {
        return prefix;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(counts);
        result = prime * result + Objects.hash(prefix);
        return result;
    }

    public boolean isOrigin() {
        if (prefix != 0) {
            return false;
        }
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Merge the specified clock with the receiver. The receiver's new state is the
     * max(receiver, clockB)
     * 
     * @return the immutable ClockValue representing the merged state of the
     *         receiver
     */
    public ClockValue merge(ClockValue clockB) {
        BloomClockValue bbc = clockB.toBloomClockValue();

        if (counts.length != bbc.counts().length) {
            throw new IllegalArgumentException("Cannot merge as this clock has m: " + hash.m + " and B has m: "
            + bbc.m());
        }

        // only one is > 0 if any
        int aBias = 0;
        int bBias = 0;

        // Merge prefixes
        int prefixCompare = Long.compareUnsigned(prefix, bbc.prefix());
        if (prefixCompare < 0) {
            long preDiff = bbc.prefix() - prefix;
            prefix = bbc.prefix();
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                for (int i = 0; i < counts.length; i++) {
                    counts[i] = bbc.counts()[i];
                }
                return this;
            }
            bBias = (int) (preDiff & MASK);
        } else if (prefixCompare > 0) {
            long preDiff = prefix - bbc.prefix();
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                return this;
            }
            aBias = (int) (preDiff & MASK);
        }

        int overall = 0;
        for (int i = 0; i < hash.m; i++) {
            int a = count(i) + aBias;
            int b = count(i, bbc.counts()) + bBias;

            int max = Math.max(a, b) - aBias - bBias;
            set(i, max);
            overall = Math.max(max, overall);
        }
        if (overall == 0xFF) {
            rollPrefix();
        }
        return this;
    }

    public void reset() {
        for (int i = 0; i < counts.length; i++) {
            counts[i] = 0;
        }
        prefix = 0;
    }

    public int sum() {
        int sum = 0;
        for (int i = 0; i < hash.m; i++) {
            sum += count(i);
        }
        return sum;
    }

    @Override
    public BloomClockValue toBloomClockValue() {
        return new BloomClockValue(prefix, counts);
    }

    public BloomeClock toBloomeClock() {
        return BloomeClock.newBuilder().setPrefix(prefix).setK(hash.k).setCounts(ByteString.copyFrom(counts)).build();
    }

    @Override
    public Clock toClock() {
        return Clock.newBuilder().setPrefix(prefix).setCounts(ByteString.copyFrom(counts)).build();
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

    public boolean validate(StampedBloomeClock clock) {
        BloomeClock vector = clock.getClock();
        return hash.k == vector.getK() && counts.length == vector.getCounts().size();
    }

    private int count(int index) {
        return count(index, counts);
    }

    private void inc(int index) {
        counts[index] += 1;
    }

    private void rollPrefix() {
        int min = MASK;
        for (int i = 0; i < hash.m; i++) {
            int count = count(i);
            if (count == 0x00) {
                log.trace("Overflow");
                return;
            }
            min = Math.min(min, count);
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
