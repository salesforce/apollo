/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.kairos;

import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.InstantSource;
import java.time.ZoneId;

/**
 * Kairos simulation time
 * 
 * @author hal.hildebrand
 *
 */
public class Kairos extends Clock implements Serializable {
    private static final long serialVersionUID = 1L;

    private final InstantSource baseSource;
    private final ZoneId        zone;

    Kairos(InstantSource baseSource, ZoneId zone) {
        this.baseSource = baseSource;
        this.zone = zone;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Kairos other) && zone.equals(other.zone) && baseSource.equals(other.baseSource);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public int hashCode() {
        return baseSource.hashCode() ^ zone.hashCode();
    }

    @Override
    public Instant instant() {
        return baseSource.instant();
    }

    @Override
    public long millis() {
        return baseSource.millis();
    }

    @Override
    public String toString() {
        return "SourceClock[" + baseSource + "," + zone + "]";
    }

    @Override
    public Clock withZone(ZoneId zone) {
        if (zone.equals(this.zone)) { // intentional NPE
            return this;
        }
        return new Kairos(baseSource, zone);
    }
}
