/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bc;

/**
 * @author hal.hildebrand
 *
 */
public interface StampedClockValue<T extends Comparable<T>, S> extends ClockValue {

    T instant();

    S toStampedClock();

}
