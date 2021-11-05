/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.FsmExecutor;

/**
 * Leaf action interface for the Producer FSM
 * 
 * @author hal.hildebrand
 *
 */
public interface Driven {
    /** Transition events for the Producer FSM */
    interface Transitions extends FsmExecutor<Driven, Transitions> {
        static Logger log = LoggerFactory.getLogger(Transitions.class);

        default Transitions assembled() {
            return null;
        }

        default Transitions checkpoint() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions complete() {
            return null;
        }

        default Transitions establish() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions failed() {
            return Earner.PROTOCOL_FAILURE;
        }

        default Transitions lastBlock() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions start() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions viewComplete() {
            throw fsm().invalidTransitionOn();
        }
    }

    public static String PERIODIC_VALIDATIONS = "PERIODIC_VALIDATIONS";
    public static String SYNC                 = "SYNC";

    void checkpoint();

    void complete();

    void reconfigure();

    void startProduction();
}
