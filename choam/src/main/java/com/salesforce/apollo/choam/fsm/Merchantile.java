/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.Exit;
import com.salesforce.apollo.choam.fsm.Combine.Transitions;

/**
 * @author hal.hildebrand
 *
 */
public enum Merchantile implements Transitions {
    AWAITING_REGENERATION {
        @Exit
        public void cancelTimer() {
            context().cancelTimer(Combine.AWAIT_SYNC);
        }

        @Override
        public Transitions synchronizationFailed() {
            context().awaitRegeneration();
            return null;
        }

        @Entry
        public void synchronizeContext() {
            context().awaitRegeneration();
        }
    },
    BOOTSTRAPPING, INITIAL {
        @Override
        public Transitions start() {
            return RECOVERING;
        }
    },
    OPERATIONAL, PROTOCOL_FAILURE, RECOVERING {
        @Exit
        public void cancelTimer() {
            context().cancelTimer(Combine.AWAIT_SYNC);
        }

        @Override
        public Transitions regenerate() {
            return REGENERATING;
        }

        @Override
        public Transitions synchronizationFailed() {
            return AWAITING_REGENERATION;
        }

        @Entry
        public void synchronizeContext() {
            context().awaitSynchronization();
        }

        @Override
        public Transitions synchronizing() {
            return SYNCHRONIZING;
        }
    },
    REGENERATING {
        @Entry
        public void regenerateView() {
            context().regenerate();
        }
    },
    SYNCHRONIZING;

    @Override
    public Transitions regenerated() {
        return OPERATIONAL;
    }
}
