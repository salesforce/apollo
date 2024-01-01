/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import org.apache.commons.math3.random.BitsStreamGenerator;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.Collector;

public class ReservoirSampler<T> implements Collector<T, List<T>, List<T>> {

    private final Predicate<T>        exclude;
    private final BitsStreamGenerator rand;
    private final int                 sz;
    private       AtomicInteger       c = new AtomicInteger();

    public ReservoirSampler(int size, BitsStreamGenerator entropy) {
        this(null, size, entropy);
    }

    public ReservoirSampler(Object excluded, int size, BitsStreamGenerator entropy) {
        this(t -> excluded == null ? false : excluded.equals(t), size, entropy);
    }

    public ReservoirSampler(Predicate<T> excluded, int size, BitsStreamGenerator entropy) {
        assert size >= 0;
        this.exclude = excluded;
        this.sz = size;
        rand = entropy;
    }

    @Override
    public BiConsumer<List<T>, T> accumulator() {
        return this::addIt;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return EnumSet.of(Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH);
    }

    @Override
    public BinaryOperator<List<T>> combiner() {
        return (left, right) -> {
            left.addAll(right);
            return left;
        };
    }

    @Override
    public Function<List<T>, List<T>> finisher() {
        return (i) -> i;
    }

    @Override
    public Supplier<List<T>> supplier() {
        return ArrayList::new;
    }

    private void addIt(final List<T> in, T s) {
        if (exclude != null && exclude.test(s)) {
            return;
        }
        if (in.size() < sz) {
            in.add(s);
        } else {
            int replaceInIndex = (int) (rand.nextLong(sz + (c.getAndIncrement()) + 1));
            if (replaceInIndex < sz) {
                in.set(replaceInIndex, s);
            }
        }
    }

}
