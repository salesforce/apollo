/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import com.salesfoce.apollo.consortium.proto.Transaction;

/**
 * A pending transaction submitted by a client
 *
 * @author hal.hildebrand
 *
 */
public class SubmittedTransaction {
    public final BiConsumer<Object, Throwable> onCompletion;
    public final Transaction                   submitted;
    public final Future<?>                     timeout;

    public SubmittedTransaction(Transaction submitted, BiConsumer<Object, Throwable> onCompletion, Future<?> timeout) {
        this.submitted = submitted;
        this.onCompletion = onCompletion;
        this.timeout = timeout;
    }

    public void cancel() {
        if (timeout != null) {
            timeout.cancel(true);
        }
    }
}
