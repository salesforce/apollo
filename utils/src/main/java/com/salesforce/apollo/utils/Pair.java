/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

/**
 * @author hal.hildebrand
 *
 */
public class Pair<A, B> {

    public static final Pair<?, ?> EMPTY_PAIR = new Pair<>(null, null);

    @SuppressWarnings("unchecked")
    public static <A, B> Pair<A, B> emptyPair() {
        return (Pair<A, B>) EMPTY_PAIR;
    }

    public final A a;

    public final B b;

    public Pair(A a, B b) {
        this.a = a;
        this.b = b;
    }
}
