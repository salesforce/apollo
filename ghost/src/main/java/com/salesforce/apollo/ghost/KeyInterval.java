/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import java.util.function.Predicate;

import com.salesforce.apollo.avro.Interval;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class KeyInterval implements Predicate<HASH> {
    private final HashKey begin;
    private final HashKey end;

    public KeyInterval(HashKey begin, HashKey end) {
        assert begin.compareTo(end) < 0 : begin + " >= " + end;
        this.begin = begin;
        this.end = end;
    }

    public KeyInterval(Interval interval) {
        this(new HashKey(interval.getStart()), new HashKey(interval.getEnd()));
    }

    @Override
    public boolean test(HASH t) {
        HashKey other = new HashKey(t);
        return begin.compareTo(other) > 0 && end.compareTo(other) > 0;
    }

    public HashKey getBegin() {
        return begin;
    }

    public HashKey getEnd() {
        return end;
    }

    public Interval toInterval() {
        return new Interval(begin.toHash(), end.toHash());
    }

    @Override
    public String toString() {
        return String.format("KeyInterval [begin=%s, end=%s]", begin, end);
    }
}
