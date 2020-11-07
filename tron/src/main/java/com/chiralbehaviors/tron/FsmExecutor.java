/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.chiralbehaviors.tron;

/**
 * @author hal.hildebrand
 *
 */
public interface FsmExecutor<Context, Transitions> {
    default Context context() {
        throw new IllegalStateException();
    }

    Fsm<Context, Transitions> fsm();
}
