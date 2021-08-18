/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import com.codahale.metrics.Timer;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */

public record SubmittedTransaction(Digest hash, BiConsumer<?, Throwable> onCompletion, Transaction submitted,
                                   Future<?> timeout, Timer.Context latency) {
    public void cancel() {
        if (timeout != null) {
            timeout.cancel(true);
        }
    }
}
