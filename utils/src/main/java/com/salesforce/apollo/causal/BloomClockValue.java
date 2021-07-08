/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.causal;

import static com.salesforce.apollo.causal.BloomClock.count;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.utils.proto.Clock;

/**
 * @author hal.hildebrand
 *
 */

public record BloomClockValue(long prefix, byte[] counts) implements ClockValue {

    @Override
    public int compareTo(double fpr, ClockValue b) {
        var bcv = b.toBloomClockValue();
        return happenedBefore(fpr, bcv.prefix, bcv.counts);
    }

    public int happenedBefore(double fpr, long bbcPrefix, byte[] bbcCounts) {
        return BloomClock.happenedBefore(fpr, prefix, counts, bbcPrefix, bbcCounts);
    }

    public int m() {
        return BloomClock.m(counts);
    }

    @Override
    public Clock toClock() {
        return Clock.newBuilder().setPrefix(prefix).setCounts(ByteString.copyFrom(counts)).build();
    }

    @Override
    public BloomClockValue toBloomClockValue() {
        return this;
    }

    public int sum() {
        int sum = 0;
        for (int i = 0; i < m(); i++) {
            sum += count(i, counts);
        }
        return sum;
    }
}
