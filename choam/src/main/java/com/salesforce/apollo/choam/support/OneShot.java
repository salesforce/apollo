/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.google.protobuf.ByteString;

import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

public class OneShot implements Supplier<ByteString> {
    private final    CountDownLatch latch = new CountDownLatch(1);
    private volatile ByteString     value;

    @Override
    public ByteString get() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            return ByteString.EMPTY;
        }
        final var current = value;
        value = null;
        return current == null ? ByteString.EMPTY : current;
    }

    public void setValue(ByteString value) {
        this.value = value;
        latch.countDown();
    }
}
