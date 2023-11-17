/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.FsmExecutor;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Leaf action interface for the Producer FSM
 *
 * @author hal.hildebrand
 */
public interface Driven {
    public static String PERIODIC_VALIDATIONS = "PERIODIC_VALIDATIONS";
    public static String SYNC                 = "SYNC";

    void assembled();

    void checkAssembly();

    void checkpoint();

    void complete();

    void create(List<ByteString> preblock, boolean last);

    void fail();

    void produceAssemble();

    void reconfigure();

    void startProduction();

    enum Earner implements Driven.Transitions {
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
            public Transitions create(List<ByteString> preblock, boolean last) {
                context().checkAssembly();
                return super.create(preblock, last);
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
        }, CHECKPOINTING {
            @Entry
            public void check() {
                context().checkpoint();
            }

            @Override
            public Transitions checkpointed() {
                return SPICE;
            }
        }, COMPLETE {
        }, INITIAL {
            @Override
            public Transitions checkpoint() {
                return CHECKPOINTING;
            }

            @Override
            public Transitions start() {
                return SPICE;
            }
        }, PROTOCOL_FAILURE {
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
        }, SPICE {
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

    /** Transition events for the Producer FSM */
    interface Transitions extends FsmExecutor<Driven, Transitions> {
        static Logger log = LoggerFactory.getLogger(Transitions.class);

        default Transitions assembled() {
            return null;
        }

        default Transitions checkpoint() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions checkpointed() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions create(List<ByteString> preblock, boolean last) {
            context().create(preblock, last);
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

        default Transitions newEpoch(int epoch, int lastEpoch) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions start() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions viewComplete() {
            throw fsm().invalidTransitionOn();
        }
    }
}
