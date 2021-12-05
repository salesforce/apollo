/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.FsmExecutor;

/**
 * Leaf action interface for the view reconfiguration FSM
 * 
 * @author hal.hildebrand
 *
 */
public interface Reconfiguration {
    /** Transition events for the view reconfiguration FSM **/
    interface Transitions extends FsmExecutor<Reconfiguration, Transitions> {
        default Transitions assembled() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions complete() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions failed() {
            return Reconfigure.PROTOCOL_FAILURE;
        }

        default Transitions gathered() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions nextEpoch(int epoch) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions reconfigureBlock() {
            throw fsm().invalidTransitionOn();
        }
    }

    void certify();

    default void certifyBlock() {
        // do nothing
    }

    void complete();

    void elect();

    void failed();

    void gather();

    void nominate();

    default void produceBlock() {
        // do nothing
    }
}
