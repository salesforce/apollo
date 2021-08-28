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

import com.chiralbehaviors.tron.Entry;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;

/**
 * Producer Finite State Machine
 * 
 * @author hal.hildebrand
 *
 */
public enum Earner implements Driven.Transitions {
    ASSEMBLE {

        @Override
        public Transitions lastBlock() {
            return PRE_SPICE;
        }

        @Entry
        public void prepareAssembly() {
            context().prepareAssembly();
        }

        @Override
        public Transitions submit(Transaction transaction, CompletableFuture<SubmitResult> result) {
            context().submit(transaction, result);
            return null;
        }

        @Override
        public Transitions validate(Validate validate) {
            context().valdateBlock(validate);
            return null;
        }
    },
    COMPLETE {

        @Entry
        public void reconfigure() {
            context().reconfigure();
        }

        @Override
        public Transitions validate(Validate validate) {
            context().valdateBlock(validate);
            return null;
        }
    },
    INITIAL {
        @Override
        public Transitions start() {
            return SPICE;
        }
    },
    PRE_SPICE {

        @Override
        public Transitions lastBlock() {
            return COMPLETE;
        }

        @Entry
        public void resumeProduction() {
            context().preSpice();
        }

        @Override
        public Transitions submit(Transaction transaction, CompletableFuture<SubmitResult> result) {
            context().submit(transaction, result);
            return null;
        }

        @Override
        public Transitions validate(Validate validate) {
            context().valdateBlock(validate);
            return null;
        }
    },
    PROTOCOL_FAILURE {
        @Entry
        public void terminate() {
            log.error("Protocol failure", new Exception("Protocol failure at: " + fsm().getPreviousState()));
        }
    },
    SPICE {

        @Override
        public Transitions lastBlock() {
            return ASSEMBLE;
        }

        @Entry
        public void startProduction() {
            context().startProduction();
        }

        @Override
        public Transitions submit(Transaction transaction, CompletableFuture<SubmitResult> result) {
            context().submit(transaction, result);
            return null;
        }

        @Override
        public Transitions validate(Validate validate) {
            context().valdateBlock(validate);
            return null;
        }
    };

    private static final Logger log = LoggerFactory.getLogger(Earner.class);
}
