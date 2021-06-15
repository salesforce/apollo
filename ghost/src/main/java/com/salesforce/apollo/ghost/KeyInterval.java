/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import java.util.function.Predicate;

import com.salesfoce.apollo.ghost.proto.Interval;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 * @since 220
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
        this(new Digest(interval.getStart()), new Digest(interval.getEnd()));
    }

    @Override
    public boolean test(Digest t) {
        return begin.compareTo(t) > 0 && end.compareTo(t) > 0;
    }

    public Digest getBegin() {
        return begin;
    }

    public Digest getEnd() {
        return end;
    }

    public Interval toInterval() {
        return Interval.newBuilder().setStart(begin.toByteString()).setEnd(end.toByteString()).build();
    }

    @Override
    public String toString() {
        return String.format("KeyInterval [begin=%s, end=%s]", begin, end);
    }
}
