/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;

import static com.salesforce.apollo.utils.Utils.locked;

import java.time.Instant;
import java.util.concurrent.locks.Lock;

import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.utils.proto.Clock;
import com.salesfoce.apollo.utils.proto.StampedClock;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.utils.bloomFilters.BloomClock.ComparisonResult;

/**
 * @author hal.hildebrand
 *
 */
public record CausalityClock(BloomClock clock, java.time.Clock wallclock, Lock lock) implements ClockValue {

    public Instant instant() {
        return wallclock.instant();
    }

    public BloomClockValue toBloomClockValue() {
        return locked(() -> clock.toBloomClockValue(), lock);
    }

    public ClockValue current() {
        return locked(() -> clock.current(), lock);
    }

    public Instant observe(Digest digest) {
        return locked(() -> {
            clock.add(digest);
            return wallclock.instant();
        }, lock);
    }

    public ClockValue merge(ClockValue b) {
        return locked(() -> clock.merge(b), lock);
    }

    public StampedClock stamp(Digest digest) {
        return locked(() -> {
            clock.add(digest);
            Instant now = wallclock.instant();
            return StampedClock.newBuilder().setStamp(Timestamp.newBuilder().setSeconds(now.getEpochSecond())
                                                               .setNanos(now.getNano()))
                               .setClock(clock.toClock()).build();
        }, lock);
    }

    @Override
    public ComparisonResult compareTo(ClockValue b) {
        return locked(() -> clock.compareTo(b), lock);
    }

    @Override
    public Clock toClock() {
        return locked(() -> clock.toClock(), lock);
    }
}
