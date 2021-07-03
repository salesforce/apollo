/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;

import com.salesfoce.apollo.utils.proto.Clock;
import com.salesforce.apollo.utils.bloomFilters.BloomClock.ComparisonResult;

/**
 * @author hal.hildebrand
 *
 */
public interface ClockValue {

    static ClockValue of(Clock clock) {
        byte[] counts = clock.getCounts().toByteArray();
        return new BloomClockValue(clock.getPrefix(), counts);
    }

    BloomClockValue toBloomClockValue();

    ComparisonResult compareTo(ClockValue b);

    Clock toClock();
}
