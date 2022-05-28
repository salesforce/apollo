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
import com.salesforce.apollo.choam.fsm.Driven.Transitions;

/**
 * Producer Finite State Machine
 * 
 * @author hal.hildebrand
 *
 */
public enum Earner implements Driven.Transitions {
    AWAIT_VIEW {
        @Override
        public Transitions assembled() {
            context().assembled();
            return null;
        }

        @Entry
        public void checkAssembly() {
            context().checkAssembly();
        }

        @Override
        public Transitions lastBlock() {
            return COMPLETE;
        }

        @Override
        public Transitions viewComplete() {
            context().assembled();
            return null;
        }
    },
    CHECKPOINTING {

        @Entry
        public void check() {
            context().checkpoint();
        }

        @Override
        public Transitions checkpointed() {
            return SPICE;
        }
    },
    COMPLETE {
    },
    DRAIN { // currently unused.

        @Override
        public Transitions assembled() {
            context().reconfigure();
            return null;
        }

        @Entry
        public void drain() {
            context().drain();
        }

        @Override
        public Transitions viewComplete() {
            return null;
        }

        @Override
        public Transitions newEpoch(int epoch, int lastEpoch) {
            return AWAIT_VIEW;
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
            context().fail();
        }
    },
    SPICE {
        @Override
        public Transitions assembled() {
            context().reconfigure();
            return null;
        }

        @Override
        public Transitions newEpoch(int epoch, int lastEpoch) {
            if (lastEpoch == epoch) {
                return AWAIT_VIEW;
            }
            if (epoch == 0) {
                context().produceAssemble();
            }
            return null;
        }

        @Entry
        public void startProduction() {
            context().startProduction();
        }

        @Override
        public Transitions viewComplete() {
            return null;
        }
    };

    private static final Logger log = LoggerFactory.getLogger(Earner.class);
}
