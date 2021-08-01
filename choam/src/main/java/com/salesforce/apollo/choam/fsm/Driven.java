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
import com.salesfoce.apollo.choam.proto.Joins;
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
            throw fsm().invalidTransitionOn();
        }

        default Transitions assumeDelegate() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions assumePrincipal() {
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

        default Transitions joins(Joins joins) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions key(HashedCertifiedBlock keyBlock) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions nominated() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions publish(Publish publish) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions reconfigure() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions reconfigure(Block reconfigure) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions reconfigured() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions regenerate() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions start() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions validate(Validate validate) {
            throw fsm().invalidTransitionOn();
        }

    }

    String RECONFIGURE = "RECONFIGURE";
    String RECONVENE   = "RECONVENE";

    void assemble(Joins joins);

    void awaitView();

    void cancelTimer(String label);

    void convene();

    void establishPrincipal();

    void gatherAssembly();

    void generateView();

    void initialState();

    void reconfigure();

    void reconfigure(Block reconfigure);

    void reform();

    void validation(Validate validate);
}
