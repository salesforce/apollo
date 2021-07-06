/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.causal;

import java.util.Collection;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface CausalClock<T extends Comparable<T>, S> extends StampedClockValue<T, S> {
    public StampedClockValue<T, S> merge(StampedClockValue<T, S> b);

    public T observeAll(Collection<Digest> digests);

    public S stamp(Digest digest);

    StampedClockValue<T, S> current();

    T observe(Digest digest);

    void reset();
}
