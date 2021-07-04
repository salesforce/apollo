/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bc;

import static com.salesforce.apollo.utils.bc.BloomClock.MASK;
import static com.salesforce.apollo.utils.bc.BloomClock.count;
import static java.util.stream.IntStream.range;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.utils.proto.Clock;
import com.salesforce.apollo.utils.bc.BloomClock.Comparison;
import com.salesforce.apollo.utils.bc.BloomClock.ComparisonResult;

/**
 * @author hal.hildebrand
 *
 */

public record BloomClockValue(long prefix, byte[] counts) implements ClockValue {

    @Override
    public ComparisonResult compareTo(ClockValue b) {
        var bcv = b.toBloomClockValue();
        return happenedBefore(bcv.prefix, bcv.counts);
    }

    public ComparisonResult happenedBefore(long bbcPrefix, byte[] bbcCounts) {
        if (counts.length != bbcCounts.length) {
            throw new IllegalArgumentException("Cannot compare as this clock has a different count size than the specified clock");
        }
        Comparison c = compareWith(bbcPrefix, bbcCounts);
        return switch (c.compared()) {
        case 0 -> new ComparisonResult(0, falsePositiveRate(c));
        case 1 -> new ComparisonResult(1, falsePositiveRate(c));
        case -1 -> new ComparisonResult(-1, falsePositiveRate(c));
        default -> throw new IllegalArgumentException("Unexpected comparison value: " + c.compared());
        };
    }

    public double falsePositiveRate(Comparison c) {
        double x = Math.min(c.sumA(), c.sumB());
        double y = Math.max(c.sumA(), c.sumB());
        return Math.pow(1 - Math.pow(1.0 - (1.0 / m()), x), y);
    }

    public int m() {
        return BloomClock.m(counts);
    }

    public Comparison compareWith(long bbcPrefix, byte[] bbcCounts) {
        int sumA = 0;
        int sumB = 0;
        int lessThan = 0;
        int greaterThan = 0;

        // only one is > 0 if any
        int aBias = 0;
        int bBias = 0;

        int prefixCompare = Long.compareUnsigned(prefix, bbcPrefix);
        int m = m();
        if (prefixCompare < 0) {
            long preDiff = bbcPrefix - prefix;
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                return new Comparison(1, 0, MASK * m);
            }
            bBias = (int) (preDiff & MASK);
        } else if (prefixCompare > 0) {
            long preDiff = prefix - bbcPrefix;
            if (Long.compareUnsigned(preDiff, MASK) > 0) {
                return new Comparison(-1, MASK * m, 0);
            }
            aBias = (int) (preDiff & MASK);
        }

        for (int i = 0; i < m; i++) {
            int a = count(i, counts) + aBias;
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
        return new Comparison(-1, sumA, sumB);
    }

    @Override
    public Clock toClock() {
        return Clock.newBuilder().setPrefix(prefix).setCounts(ByteString.copyFrom(counts)).build();
    }

    @Override
    public BloomClockValue toBloomClockValue() {
        return this;
    }
}
