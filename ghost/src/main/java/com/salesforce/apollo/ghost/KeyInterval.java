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
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class KeyInterval implements Predicate<Digest> {
    private final Digest        begin;
    private final Digest        end;
    private BloomFilter<Digest> immutableBff;
    private BloomFilter<Digest> mutableBff;

    public KeyInterval(Digest begin, Digest end) {
        this(begin, null, end, null);
    }

    public KeyInterval(Interval interval) {
        this(new Digest(interval.getStart()), BloomFilter.from(interval.getMutableBff()), new Digest(interval.getEnd()),
                BloomFilter.from(interval.getImmutableBff()));
    }

    private KeyInterval(Digest begin, BloomFilter<Digest> mutableBff, Digest end, BloomFilter<Digest> immutableBff) {
        assert begin.compareTo(end) < 0 : begin + " >= " + end;
        this.begin = begin;
        this.end = end;
        this.immutableBff = immutableBff;
        this.mutableBff = mutableBff;
    }

    public Digest getBegin() {
        return begin;
    }

    public Digest getEnd() {
        return end;
    }

    public BloomFilter<Digest> getImmutableBff() {
        return immutableBff;
    }

    public BloomFilter<Digest> getMutableBff() {
        return immutableBff;
    }

    public boolean immutableContains(Digest e) {
        return immutableBff.contains(e);
    }

    public boolean mutableContains(Digest e) {
        return mutableBff.contains(e);
    }

    public void setImmutableBff(BloomFilter<Digest> immutableBff) {
        this.immutableBff = immutableBff;
    }

    public void setMutableBff(BloomFilter<Digest> mutableBff) {
        this.mutableBff = mutableBff;
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
        return String.format("[%s, %s]", begin, end);
    }
}
