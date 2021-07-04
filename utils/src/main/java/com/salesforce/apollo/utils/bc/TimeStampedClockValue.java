/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bc;

import java.time.Instant;

import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.utils.proto.Clock;
import com.salesfoce.apollo.utils.proto.StampedClock;
import com.salesforce.apollo.utils.bc.BloomClock.ComparisonResult;

/**
 * @author hal.hildebrand
 *
 */
public record TimeStampedClockValue(BloomClockValue clock, Instant stamp)
                                   implements StampedClockValue<Instant, StampedClock> {

    public static TimeStampedClockValue from(StampedClock c) {
        Timestamp ts = c.getStamp();
        return new TimeStampedClockValue(ClockValue.of(c.getClock()),
                                         Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()));
    }

    @Override
    public BloomClockValue toBloomClockValue() {
        return clock;
    }

    @Override
    public StampedClock toStampedClock() {
        return StampedClock.newBuilder().setClock(clock.toClock())
                           .setStamp(Timestamp.newBuilder().setSeconds(stamp.getEpochSecond())
                                              .setNanos(stamp.getNano()))
                           .build();
    }

    @Override
    public ComparisonResult compareTo(ClockValue b) {
        return clock.compareTo(b);
    }

    @Override
    public Clock toClock() {
        return clock.toClock();
    }

    @Override
    public Instant instant() {
        return null;
    }

}
