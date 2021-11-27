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
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;

/**
 * @author hal.hildebrand
 *
 */
public enum Merchantile implements Transitions {
    AWAITING_REGENERATION {

        @Exit
        public void cancelTimer() {
            context().cancelTimer(Combine.AWAIT_REGEN);
        }

        @Override
        public Transitions combine() {
            context().combine();
            return null;
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
    BOOTSTRAPPING {
        @Override
        public Transitions combine() {
            return null; // Just queue up any blocks
        }

        @Override
        public Transitions synchronizing() {
            return SYNCHRONIZING;
        }
    },
    CHECKPOINTING {

        @Override
        public Transitions combine() {
            return null; // Just queue up any blocks
        }

        @Override
        public Transitions finishCheckpoint() {
            fsm().pop().combine();
            return null;
        }
    },
    INITIAL {
        @Override
        public Transitions start() {
            return RECOVERING;
        }
    },
    OPERATIONAL {

        @Override
        public Transitions beginCheckpoint() {
            fsm().push(CHECKPOINTING);
            return null;
        }

        @Override
        public Transitions combine() {
            context().combine();
            return null;
        }

    },
    PROTOCOL_FAILURE, RECOVERING {
        @Override
        public Transitions bootstrap(HashedCertifiedBlock anchor) { 
            context().recover(anchor);
            return BOOTSTRAPPING;
        }

        @Exit
        public void cancelTimer() {
            context().cancelTimer(Combine.AWAIT_SYNC);
        }

        @Override
        public Transitions combine() {
            context().anchor();
            return null;
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
    },
    REGENERATING {

        @Override
        public Transitions combine() {
            context().combine();
            return null;
        }

        @Entry
        public void regenerateView() {
            context().regenerate();
        }
    },
    SYNCHRONIZING {
        @Override
        public Transitions combine() {
            return null; // Just queue up any blocks
        }
    };

    @Override
    public Transitions regenerated() {
        return OPERATIONAL;
    }
}
