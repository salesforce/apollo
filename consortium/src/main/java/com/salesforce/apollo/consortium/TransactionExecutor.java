/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.function.BiConsumer;

import com.google.protobuf.Any;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface TransactionExecutor {
    default void beginBlock(long height, Digest hash) {
    }

    void execute(Digest blockHash, ExecutedTransaction txn, BiConsumer<? super Object, Throwable> completion);

    default void processGenesis(Any genesisData) {
    }
}
