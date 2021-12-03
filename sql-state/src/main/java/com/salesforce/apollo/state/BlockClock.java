/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * @author hal.hildebrand
 *
 */
public class BlockClock extends Clock {
    private static long txnInc = (long) (Math.pow(2, 31) - 1);

    private volatile long height = 0;
    private volatile long txn    = txnInc;

    private final ZoneId zoneId;

    public BlockClock() {
        this(ZoneOffset.UTC);
    }

    public BlockClock(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    @Override
    public ZoneId getZone() {
        return zoneId;
    }

    @Override
    public Instant instant() {
//        if (true) {
//            return Instant.now();
//        }
        incrementQuery();
        final var currentHeight = height;
        final var currentTxn = txn;
        return Instant.ofEpochSecond(currentHeight, currentTxn);
    }

    @Override
    public Clock withZone(ZoneId zone) {
        if (zone == null || zoneId.equals(zone)) {
            return this;
        }
        return new BlockClock(zone);
    }

    protected void incrementHeight() {
        final var current = height;
        height = current + 1;
        txn = txnInc;
    }

    protected void incrementTxn() {
        final var current = txn;
        txn = current + txnInc;
    }

    private void incrementQuery() {
        final var current = txn;
        txn = current + 1;
    }
}
