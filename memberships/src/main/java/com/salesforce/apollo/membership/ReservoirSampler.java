/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class ReservoirSampler<T> implements Collector<T, List<T>, List<T>> {

    private int                c = 0;
    private Object             exclude;
    private final SecureRandom rand;
    private final int          sz;

    public ReservoirSampler(Object excluded, int size, SecureRandom entropy) {
        this.exclude = excluded;
        this.sz = size;
        rand = entropy;
    }

    @Override
    public BiConsumer<List<T>, T> accumulator() {
        return this::addIt;
    }

    @Override
    public Set<java.util.stream.Collector.Characteristics> characteristics() {
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
        if (exclude.equals(s)) {
            return;
        }
        if (in.size() < sz) {
            in.add(s);
        } else {
            int replaceInIndex = (int) (rand.nextDouble() * (sz + (c++) + 1));
            if (replaceInIndex < sz) {
                in.set(replaceInIndex, s);
            }
        }
    }

}
