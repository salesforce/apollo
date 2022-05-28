/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.ethereal;

import java.util.ArrayDeque;
import java.util.Deque;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.utils.Entropy;

/**
 * 
 * @author hal.hildebrand
 *
 */
public class SimpleDataSource implements DataSource {
    public final Deque<ByteString> dataStack = new ArrayDeque<>();

    @Override
    public ByteString getData() {
        try {
            Thread.sleep(Entropy.nextBitsStreamLong(10));
        } catch (InterruptedException e) {
        }
        return dataStack.pollFirst();
    }
}
