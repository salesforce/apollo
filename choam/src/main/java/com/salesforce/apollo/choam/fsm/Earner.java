/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.Exit;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;

/**
 * Producer Finite State Machine
 * 
 * @author hal.hildebrand
 *
 */
public enum Earner implements Driven.Transitions {
    CHECKPOINTING {

        @Entry
        public void check() {
            context().checkpoint();
        }

        @Override
        public Transitions lastBlock() {
            return SPICE;
        }
    },
    COMPLETE {
        @Exit
        public void bail() {
            context().cancelTimers();
        }

        @Override
        public Transitions lastBlock() {
            context().complete();
            return null;
        }

        @Entry
        public void reconfigure() {
            context().reconfigure();
        }
    },
    INITIAL {
        @Override
        public Transitions checkpoint() {
            return CHECKPOINTING;
        }

        @Override
        public Transitions start() {
            return SPICE;
        }
    },
    PROTOCOL_FAILURE {
        @Override
        public Transitions assembled() {
            return null;
        }

        @Override
        public Transitions checkpoint() {
            return null;
        }

        @Override
        public Transitions establish() {
            return null;
        }

        @Override
        public Transitions failed() {
            return null;
        }

        @Override
        public Transitions lastBlock() {
            return null;
        }

        @Override
        public Transitions start() {
            return null;
        }

        @Entry
        public void terminate() {
            log.error("Protocol failure", new Exception("Protocol failure at: " + fsm().getPreviousState()));
            context().complete();
        }
    },
    SPICE {

        @Override
        public Transitions lastBlock() {
            return COMPLETE;
        }

        @Entry
        public void startProduction() {
            context().startProduction();
        }
    };

    private static final Logger log = LoggerFactory.getLogger(Earner.class);
}
