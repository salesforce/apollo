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
import com.salesfoce.apollo.choam.proto.Sync;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.crypto.Digest;

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

        default Transitions joins(Joins joins) {
            return null;
        }

        default Transitions key(HashedCertifiedBlock keyBlock) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions nominated() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions publish(Publish publish) {
            return null;
        }

        default Transitions publishedBlock() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions reconfigure() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions reconfigure(Block reconfigure) {
            return null;
        }

        default Transitions reconfigured() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions start() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions sync(Sync sync, Digest from) {
            return null;
        }

        default Transitions synchd() {
            return null;
        }

        default Transitions synchronize() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions validate(Validate validate) {
            return null;
        }

        default Transitions validateView(Validate validate) {
            return null;
        }
    }

    String RECONFIGURE = "RECONFIGURE";
    String RECONVENE   = "RECONVENE";
    String SYNCHRONIZE = "SYNCHRONIZE";

    void assemble(Joins joins);

    void cancelTimer(String label);

    void complete();

    void convene();

    void epochEnd();

    void gatherAssembly();

    void published(Publish published);

    void reconfigure();

    void reconfigure(Block reconfigure);

    void startProduction();

    void sync(Sync sync, Digest from);

    void synchronize();

    void valdateBlock(Validate validate);

    void validation(Validate validate);
}
