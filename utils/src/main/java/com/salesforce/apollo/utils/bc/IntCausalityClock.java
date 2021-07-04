/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bc;

import static com.salesforce.apollo.utils.Utils.locked;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import com.salesfoce.apollo.utils.proto.Clock;
import com.salesfoce.apollo.utils.proto.IntStampedClock;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.utils.bc.BloomClock.ComparisonResult;

/**
 * @author hal.hildebrand
 *
 */
public record IntCausalityClock(BloomClock clock, AtomicInteger instantValue, Lock lock) implements ClockValue {

    public int instant() {
        return instantValue.get();
    }

    public BloomClockValue toBloomClockValue() {
        return locked(() -> clock.toBloomClockValue(), lock);
    }

    public ClockValue current() {
        return locked(() -> clock.current(), lock);
    }

    public int observe(Digest digest) {
        return locked(() -> {
            clock.add(digest);
            return instant();
        }, lock);
    }

    public ClockValue merge(ClockValue b) {
        return locked(() -> clock.merge(b), lock);
    }

    public IntStampedClock stamp(Digest digest) {
        return locked(() -> {
            clock.add(digest);
            return IntStampedClock.newBuilder().setStamp(instant()).setClock(clock.toClock()).build();
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
