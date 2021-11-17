/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.FsmExecutor;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;

/**
 * @author hal.hildebrand
 *
 */
public interface Combine {

    interface Transitions extends FsmExecutor<Combine, Combine.Transitions> {
        default Transitions beginCheckpoint() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions bootstrap(HashedCertifiedBlock anchor) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions combine() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions fail() {
            return Merchantile.PROTOCOL_FAILURE;
        }

        default Transitions finishCheckpoint() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions regenerate() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions regenerated() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions start() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions synchronizationFailed() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions synchronizing() {
            throw fsm().invalidTransitionOn();
        }
    }

    static final String AWAIT_SYNC = "AWAIT_SYNC";

    void anchor();

    void awaitRegeneration();

    void awaitSynchronization();

    void cancelTimer(String timer);

    void combine();

    void recover(HashedCertifiedBlock anchor);

    void regenerate();
}
