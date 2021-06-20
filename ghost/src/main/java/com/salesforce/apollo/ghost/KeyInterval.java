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
import com.salesforce.apollo.utils.BloomFilter;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class KeyInterval implements Predicate<Digest> {
    private final Digest        begin;
    private BloomFilter<Digest> bff;
    private final Digest        end;

    public KeyInterval(Digest begin, Digest end) {
        this(begin, end, null);
    }

    public KeyInterval(Interval interval) {
        this(new Digest(interval.getStart()), new Digest(interval.getEnd()), BloomFilter.from(interval.getBiff()));
    }

    private KeyInterval(Digest begin, Digest end, BloomFilter<Digest> bff) {
        assert begin.compareTo(end) < 0 : begin + " >= " + end;
        this.begin = begin;
        this.end = end;
        this.bff = bff;
    }

    public boolean contains(Digest e) {
        return bff.contains(e);
    }

    public Digest getBegin() {
        return begin;
    }

    public BloomFilter<Digest> getBff() {
        return bff;
    }

    public Digest getEnd() {
        return end;
    }

    public void setBff(BloomFilter<Digest> bff) {
        this.bff = bff;
    }

    @Override
    public boolean test(Digest t) {
        return begin.compareTo(t) > 0 && end.compareTo(t) > 0;
    }

    public Interval toInterval() {
        return Interval.newBuilder().setStart(begin.toByteString()).setEnd(end.toByteString()).build();
    }

    @Override
    public String toString() {
        return String.format("KeyInterval [%s, %s]", begin, end);
    }
}
