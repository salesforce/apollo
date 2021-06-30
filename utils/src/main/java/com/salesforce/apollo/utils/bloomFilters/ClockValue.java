/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;
 
import com.salesforce.apollo.utils.bloomFilters.BloomClock.ComparisonResult;
import com.salesfoce.apollo.utils.proto.*;
/**
 * @author hal.hildebrand
 *
 */
public interface ClockValue {
    ComparisonResult compareTo(ClockValue b);

    Clock toClock();
}
