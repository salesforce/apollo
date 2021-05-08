/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

/**
 * @author hal.hildebrand
 *
 */
public class CountdownAction {
    private final Runnable action;
    private volatile int   countdown;

    public CountdownAction(Runnable action, int countdown) {
        this.action = action;
        this.countdown = countdown;
    }

    public boolean countdown() {
        synchronized (action) {
            final int current = countdown;
            if (current == 0) {
                throw new IllegalStateException("action has already been fired, count is already 0");
            }
            countdown--;
            if (current == 1) {
                action.run();
                return true;
            }
            return false;
        }
    }
}
