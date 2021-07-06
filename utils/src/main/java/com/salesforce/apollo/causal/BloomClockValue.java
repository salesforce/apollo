/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.causal;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.utils.proto.Clock;
import com.salesforce.apollo.causal.BloomClock.Comparison;
import com.salesforce.apollo.causal.BloomClock.ComparisonResult;

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
        return BloomClock.happenedBefore(prefix, counts, bbcPrefix, bbcCounts);
    }

    public int m() {
        return BloomClock.m(counts);
    }

    public Comparison compareWith(long bbcPrefix, byte[] bbcCounts) {
        return BloomClock.compareWith(prefix, counts, bbcPrefix, bbcCounts);

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
