/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ring;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hal.hildebrand
 *
 */
@FunctionalInterface
public interface ResultConsumer<M, T, Comm> {
    boolean handle(AtomicInteger tally, Optional<T> result, RingCommunications.Destination<M, Comm> destination);
}
