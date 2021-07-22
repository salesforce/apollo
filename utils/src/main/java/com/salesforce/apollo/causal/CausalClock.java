/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.causal;

import java.util.Collection;

import com.salesfoce.apollo.utils.proto.StampedClock;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface CausalClock<T extends Comparable<T>> extends StampedClockValue<T> {
    StampedClockValue<T> current();

    StampedClockValue<T> merge(StampedClockValue<T> b);

    StampedClockValue<T> observe(Digest digest);

    T observeAll(Collection<Digest> digests);

    void reset();

    StampedClock stamp();
}
