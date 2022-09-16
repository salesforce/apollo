/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.rbc;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public class DigestWindow {
    private final AtomicInteger                                count    = new AtomicInteger(0);
    private final BlockingDeque<ConcurrentSkipListSet<Digest>> segments = new LinkedBlockingDeque<>();
    private final int                                          windowSize;

    public DigestWindow(int windowSize, int segments) {
        this.windowSize = windowSize;
        for (int i = 0; i < segments; i++) {
            this.segments.add(new ConcurrentSkipListSet<>());
        }
    }

    public void add(Digest element) {
        if (count.incrementAndGet() % windowSize == 0) {
            segments.removeLast();
            segments.addFirst(new ConcurrentSkipListSet<>());
        }
        segments.getFirst().add(element);
    }

    public boolean add(Digest element, Consumer<Digest> ifAbsent) {
        if (count.incrementAndGet() % windowSize == 0) {
            segments.removeLast();
            segments.addFirst(new ConcurrentSkipListSet<>());
        }
        if (segments.getFirst().add(element)) {
            if (ifAbsent != null) {
                ifAbsent.accept(element);
            }
            return true;
        }
        return false;
    }

    public boolean contains(Digest element) {
        for (var biff : segments) {
            if (biff.contains(element)) {
                return true;
            }
        }
        return false;
    }
}
