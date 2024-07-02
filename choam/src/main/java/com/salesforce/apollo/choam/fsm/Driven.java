/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.FsmExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leaf action interface for the Producer FSM
 *
 * @author hal.hildebrand
 */
public interface Driven {

    void assemble();

    void checkpoint();

    void complete();

    void fail();

    void reconfigure();

    void startProduction();

    enum Earner implements Driven.Transitions {
        ASSEMBLE {
            @Entry
            public void assemble() {
                context().assemble();
            }

            @Override
            public Transitions assembled() {
                return SPICE;
            }
        }, CHECKPOINTING {
            @Entry
            public void check() {
                context().checkpoint();
            }

            @Override
            public Transitions checkpointed() {
                return ASSEMBLE;
            }
        }, COMPLETE {
            @Entry
            public void complete() {
                context().complete();
            }
        }, INITIAL {
            @Override
            public Transitions checkpoint() {
                return CHECKPOINTING;
            }

            @Override
            public Transitions start() {
                return ASSEMBLE;
            }
        }, END_EPOCHS {
            @Override
            public Transitions newEpoch(int epoch, boolean lastEpoch) {
                if (lastEpoch) {
                    context().reconfigure();
                }
                return null;
            }

            @Override
            public Transitions lastBlock() {
                return COMPLETE;
            }
        }, PROTOCOL_FAILURE {
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
        }, SPICE {
            @Override
            public Transitions viewComplete() {
                return END_EPOCHS;
            }
        }
    }

    /** Transition events for the Producer FSM */
    interface Transitions extends FsmExecutor<Driven, Transitions> {
        Logger log = LoggerFactory.getLogger(Transitions.class);

        default Transitions assembled() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions checkpoint() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions checkpointed() {
            throw fsm().invalidTransitionOn();
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

        default Transitions newEpoch(int epoch, boolean lastEpoch) {
            return null;
        }

        default Transitions start() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions viewComplete() {
            throw fsm().invalidTransitionOn();
        }
    }
}
