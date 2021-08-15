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
import com.salesfoce.apollo.choam.proto.Publish;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;

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

        default Transitions drain() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions establish() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions failed() {
            return Earner.PROTOCOL_FAILURE;
        }

        default Transitions generated() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions key(HashedCertifiedBlock keyBlock) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions publish(Publish publish) {
            return null;
        }

        default Transitions publishedBlock() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions start() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions validate(Validate validate) {
            return null;
        }
    }

    void complete();

    void epochEnd();

    void startProduction();

    void valdateBlock(Validate validate); 
}
