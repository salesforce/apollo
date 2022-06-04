/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hal.hildebrand
 *
 */
public class TickableRoundScheduler extends RoundScheduler {
    private static final long serialVersionUID = 1L;

    private final AtomicInteger round = new AtomicInteger();

    public TickableRoundScheduler(String label, int roundDuration) {
        super(label, roundDuration);
    }

    public int getRound() {
        return round.get();
    }

    public void tick() {
        tick(round.incrementAndGet());
    }
}
