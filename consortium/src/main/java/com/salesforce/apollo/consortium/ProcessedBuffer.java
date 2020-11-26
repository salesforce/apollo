/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class ProcessedBuffer {
    private final Deque<HashKey> buffer = new ArrayDeque<>();

    private final int bufferSize;

    private final Set<HashKey> set = new HashSet<>();

    public ProcessedBuffer(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void add(HashKey h) {
        if (set.add(h)) {
            buffer.add(h);
            if (buffer.size() > bufferSize) {
                set.remove(buffer.removeFirst());
            }
        }
    }

    public void clear() {
        buffer.clear();
        set.clear();
    }

    public boolean contains(HashKey h) {
        return set.contains(h);
    }
}
