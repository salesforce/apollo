/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.function.Consumer;

import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;

/**
 * @author hal.hildebrand
 *
 */
public interface TransactionExecutor extends Consumer<ExecutedTransaction> {
    static final TransactionExecutor NULL_EXECUTOR = new TransactionExecutor() {

        @Override
        public void accept(ExecutedTransaction t) {
        }

        @Override
        public void begin() {
        }

        @Override
        public void complete() {
        }
    };

    void begin();

    void complete();
}
