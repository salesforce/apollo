/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.leyden;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.leyden.proto.Interval;

import java.util.function.Predicate;

/**
 * @author hal.hildebrand
 */
public class KeyInterval implements Predicate<Digest> {
    private final Digest begin;
    private final Digest end;

    public KeyInterval(Digest begin, Digest end) {
        assert begin.compareTo(end) < 0 : begin + " >= " + end;
        this.begin = begin;
        this.end = end;
    }

    public KeyInterval(Interval interval) {
        this(Digest.from(interval.getStart()), Digest.from(interval.getEnd()));
    }

    public Digest getBegin() {
        return begin;
    }

    public Digest getEnd() {
        return end;
    }

    @Override
    public boolean test(Digest t) {
        return begin.compareTo(t) > 0 && end.compareTo(t) > 0;
    }

    public Interval toInterval() {
        return Interval.newBuilder().setStart(begin.toDigeste()).setEnd(end.toDigeste()).build();
    }

    @Override
    public String toString() {
        return String.format("KeyInterval [begin=%s, end=%s]", begin, end);
    }
}
