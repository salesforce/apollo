/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.causal;

import com.salesfoce.apollo.utils.proto.Clock;
import com.salesfoce.apollo.utils.proto.IntStampedClock;

/**
 * @author hal.hildebrand
 *
 */
public record IntStampedClockValue(BloomClockValue clock, int stamp)
                                  implements StampedClockValue<Integer, IntStampedClock> {

    public static IntStampedClockValue from(IntStampedClock c) {
        return new IntStampedClockValue(ClockValue.of(c.getClock()), c.getStamp());
    }

    @Override
    public BloomClockValue toBloomClockValue() {
        return clock;
    }

    public IntStampedClock toStampedClock() {
        return IntStampedClock.newBuilder().setClock(clock.toClock()).setStamp(stamp).build();
    }

    @Override
    public int compareTo(double fpr, ClockValue b) {
        return clock.compareTo(fpr, b);
    }

    @Override
    public Clock toClock() {
        return clock.toClock();
    }

    @Override
    public Integer instant() {
        return stamp;
    }

    @Override
    public String toString() {
        return "{" + instant() + ":" + BloomClock.print(clock) + "}";
    }

    @Override
    public int sum() {
        return clock.sum();
    }
}
