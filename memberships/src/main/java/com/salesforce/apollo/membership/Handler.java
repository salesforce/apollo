/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author hal.hildebrand
 *
 */
@FunctionalInterface
public interface Handler<T, Comm> {
    boolean handle(ListenableFuture<T> futureSailor, Comm communications, int ring);
}
