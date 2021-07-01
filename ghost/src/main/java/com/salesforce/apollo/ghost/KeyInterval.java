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
    private BloomFilter<Digest> bindingsBff;
    private BloomFilter<Digest> contentsBff;
    private final Digest        end;

    public KeyInterval(Digest begin, Digest end) {
        this(begin, null, end, null);
    }

    public KeyInterval(Interval interval) {
        this(new Digest(interval.getStart()), BloomFilter.from(interval.getBindingsBff()),
                new Digest(interval.getEnd()), BloomFilter.from(interval.getContentsBff()));
    }

    private KeyInterval(Digest begin, BloomFilter<Digest> bindingsBff, Digest end, BloomFilter<Digest> contentsBff) {
        assert begin.compareTo(end) < 0 : begin + " >= " + end;
        this.begin = begin;
        this.end = end;
        this.contentsBff = contentsBff;
        this.bindingsBff = bindingsBff;
    }

    public boolean bindingsContains(Digest e) {
        return bindingsBff.contains(e);
    }

    public boolean contentsContains(Digest e) {
        return contentsBff.contains(e);
    }

    public Digest getBegin() {
        return begin;
    }

    public BloomFilter<Digest> getBindingsBff() {
        return contentsBff;
    }

    public BloomFilter<Digest> getContentsBff() {
        return contentsBff;
    }

    public Digest getEnd() {
        return end;
    }

    public void setBindingsBff(BloomFilter<Digest> bindingsBff) {
        this.bindingsBff = bindingsBff;
    }

    public void setContentsBff(BloomFilter<Digest> contentsBff) {
        this.contentsBff = contentsBff;
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
