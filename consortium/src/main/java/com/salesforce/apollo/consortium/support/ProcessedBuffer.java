/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.util.Collections;
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public class ProcessedBuffer {
    private final Deque<Digest> buffer = new ConcurrentLinkedDeque<>();

    private final int bufferSize;

    private final Set<Digest> set = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public ProcessedBuffer(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void add(Digest h) {
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

    public boolean contains(Digest h) {
        return set.contains(h);
    }
}
