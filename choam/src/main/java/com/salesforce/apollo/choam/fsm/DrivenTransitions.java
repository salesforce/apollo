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
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.Publish;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.choam.Producer.Driven;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;

/**
 * Transition events for the Producer FSM
 * 
 * @author hal.hildebrand
 *
 */
public interface DrivenTransitions extends FsmExecutor<Driven, DrivenTransitions> {
    static Logger log = LoggerFactory.getLogger(DrivenTransitions.class);

    default DrivenTransitions assumeDelegate() {
        throw fsm().invalidTransitionOn();
    }

    default DrivenTransitions assumePrincipal() {
        throw fsm().invalidTransitionOn();
    }

    default DrivenTransitions establish() {
        throw fsm().invalidTransitionOn();
    }

    default DrivenTransitions generated() {
        throw fsm().invalidTransitionOn();
    }

    default DrivenTransitions key(HashedCertifiedBlock keyBlock) {
        throw fsm().invalidTransitionOn();
    }

    default DrivenTransitions publish(Publish publish) {
        throw fsm().invalidTransitionOn();
    }

    default DrivenTransitions reconfigure(Block reconfigure) {
        throw fsm().invalidTransitionOn();
    }

    default DrivenTransitions regenerate() {
        throw fsm().invalidTransitionOn();
    }

    default DrivenTransitions start() {
        throw fsm().invalidTransitionOn();
    }

    default DrivenTransitions validate(Validate validate) {
        throw fsm().invalidTransitionOn();
    }
}
