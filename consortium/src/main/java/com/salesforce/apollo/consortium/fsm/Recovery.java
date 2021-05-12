/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import com.chiralbehaviors.tron.Entry;

/**
 * @author hal.hildebrand
 *
 */
public enum Recovery implements Transitions {

    SYNCHRONIZE {
        @Entry
        public void recover() {
            context().recover();
        }

    },
    BOOTSTRAP;
}
