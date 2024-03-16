/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

public class OneShot implements Supplier<ByteString> {
    private static final Logger log = LoggerFactory.getLogger(OneShot.class);

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
        log.trace("providing value: " + (current == null ? "null" : String.valueOf(current.size())));
        value = null;
        return current == null ? ByteString.EMPTY : current;
    }

    public void setValue(ByteString value) {
        log.trace("resetting value: " + value);
        this.value = value;
        latch.countDown();
    }
}
