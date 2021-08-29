/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.FsmExecutor;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitResult.Outcome;
import com.salesfoce.apollo.choam.proto.Transaction;
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

        default Transitions assemblyFailed() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions checkpoint() {
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

        default Transitions lastBlock() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions start() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions submit(Transaction transaction, CompletableFuture<SubmitResult> result) {
            log.trace("Failure to submit txn, inactive committee member");
            result.complete(SubmitResult.newBuilder().setOutcome(Outcome.INACTIVE_COMMITTEE).build());
            return null;
        }

        default Transitions validate(Validate validate) {
            return null;
        }
    }

    void checkpoint();

    void prepareAssembly();

    void preSpice();

    void reconfigure();

    void startProduction();

    void submit(Transaction transaction, CompletableFuture<SubmitResult> result);

    void valdateBlock(Validate validate);
}
