/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.function.BiConsumer;

import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public interface TransactionExecutor extends BiConsumer<ExecutedTransaction, BiConsumer<HashKey, Throwable>> {
    static final TransactionExecutor NULL_EXECUTOR = new TransactionExecutor() {

        @Override
        public void begin() {
        }

        @Override
        public void complete() {
        }

        @Override
        public void accept(ExecutedTransaction t, BiConsumer<HashKey, Throwable> u) {
        }

    };

    void begin();

    void complete();
}
