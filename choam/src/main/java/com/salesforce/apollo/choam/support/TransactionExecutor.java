/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import com.salesfoce.apollo.choam.proto.ExecutedTransaction;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
@FunctionalInterface
public interface TransactionExecutor extends BiConsumer<ExecutedTransaction, CompletableFuture<?>> {
    default void beginBlock(long height, Digest hash) {
    }
}
