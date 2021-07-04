/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bc;

import java.util.Comparator;

/**
 * @author hal.hildebrand
 *
 */

/**
 * A comparator for Bloom Clock values. Because the Bloom Clock is a
 * probabalistic data structure, this comparator requires a provided <b>false
 * positive rate</b> (FPR). This FPR applies when clock A is compared to clock B
 * and the determination is that A proceeds B - this is the equivalent of
 * "contains" in a vanilla Bloom Filter. The "proceeds", or "contains"
 * determination is however probabalistic in that there is still a possibility
 * this is a false positive (the past is "contained" in the present and future,
 * so the FPR applies to the "proceeded" relationship).
 * <p>
 *
 */
public class ClockValueComparator implements Comparator<ClockValue> {
    private final double fpr;

    /**
     *
     * @param fpr - the False Positive Rate. Acceptable probability from 0.0 -> 1.0
     *            of a false positive when determining precidence.
     */
    public ClockValueComparator(double fpr) {
        this.fpr = fpr;
    }

    /**
     * Provides comparison between two Bloom Clock values. The comparator has a
     * false positive threshold that determines the acceptable threshold of
     * assurance that clock A proceeds clock B.
     * <p>
     * If clock A is ordered after clock B, then this function returns 1
     * <p>
     * If clocks A and B are not comparable, i.e. they are "simultaneous", then this
     * function returns 0.
     * <p>
     * If clock A proceeds B within this comparator's false positive rate, then this
     * function returns -1.
     */
    @Override
    public int compare(ClockValue a, ClockValue b) {
        var comparison = a.compareTo(b);
        if (comparison.comparison() >= 0) {
            return comparison.comparison();
        }
        return comparison.fpr() <= fpr ? -1 : 0;
    }

}
