/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;

/**
 * @author hal.hildebrand
 *
 */
public enum Reconfigure implements Transitions {
    CHANGE_PRINCIPAL, GATHER {
        @Override
        public Transitions assembled() {
            return NOMINATE;
        }

        @Entry
        public void assembly() {
            context().gatherAssembly();
        }
    },
    NOMINATE {
        @Entry
        public void consolidate() {
            context().convene();
        }

        @Override
        public Transitions reconfigured() {
            return RECONFIGURED;
        }
    },
    RECONFIGURED {
    };
}
