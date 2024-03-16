/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.Exit;
import com.chiralbehaviors.tron.FsmExecutor;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import org.slf4j.LoggerFactory;

/**
 * @author hal.hildebrand
 */
public interface Combine {
    String AWAIT_REGEN = "AWAIT_REGEN";
    String AWAIT_SYNC  = "AWAIT_SYNC";

    void anchor();

    void awaitRegeneration();

    void awaitSynchronization();

    void cancelTimer(String timer);

    void combine();

    void recover(HashedCertifiedBlock anchor);

    void regenerate();

    enum Mercantile implements Transitions {
        AWAITING_REGENERATION {
            @Exit
            public void cancelTimer() {
                context().cancelTimer(Combine.AWAIT_REGEN);
            }

            @Override
            public Transitions combine() {
                context().combine();
                return null;
            }

            @Override
            public Transitions synchronizationFailed() {
                context().awaitRegeneration();
                return null;
            }

            @Entry
            public void synchronizeContext() {
                context().awaitRegeneration();
            }
        }, BOOTSTRAPPING {
            @Override
            public Transitions combine() {
                return null; // Just queue up any blocks
            }

            @Override
            public Transitions synchronizing() {
                return SYNCHRONIZING;
            }
        }, CHECKPOINTING {
            @Override
            public Transitions combine() {
                return null; // Just queue up any blocks
            }

            @Override
            public Transitions finishCheckpoint() {
                fsm().pop().combine();
                return null;
            }
        }, INITIAL {
            @Override
            public Transitions start() {
                return RECOVERING;
            }
        }, OPERATIONAL {
            @Override
            public Transitions beginCheckpoint() {
                fsm().push(CHECKPOINTING);
                return null;
            }

            @Override
            public Transitions combine() {
                context().combine();
                return null;
            }

        }, PROTOCOL_FAILURE, RECOVERING {
            @Override
            public Transitions bootstrap(HashedCertifiedBlock anchor) {
                try {
                    context().recover(anchor);
                } catch (Exception e) {
                    LoggerFactory.getLogger(Mercantile.class).info("Unable to recover anchor: {}", anchor, e);
                    return null;
                }
                return BOOTSTRAPPING;
            }

            @Exit
            public void cancelTimer() {
                context().cancelTimer(Combine.AWAIT_SYNC);
            }

            @Override
            public Transitions combine() {
                context().anchor();
                return null;
            }

            @Override
            public Transitions regenerate() {
                return REGENERATING;
            }

            @Override
            public Transitions synchronizationFailed() {
                return AWAITING_REGENERATION;
            }

            @Entry
            public void synchronizeContext() {
                context().awaitSynchronization();
            }
        }, REGENERATING {
            @Override
            public Transitions combine() {
                context().combine();
                return null;
            }

            @Override
            public Transitions nextView() {
                return RECOVERING;
            }

            @Entry
            public void regenerateView() {
                context().regenerate();
            }
        }, SYNCHRONIZING {
            @Override
            public Transitions combine() {
                return null; // Just queue up any blocks
            }
        };

        @Override
        public Transitions regenerated() {
            return OPERATIONAL;
        }
    }

    interface Transitions extends FsmExecutor<Combine, Combine.Transitions> {
        default Transitions beginCheckpoint() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions bootstrap(HashedCertifiedBlock anchor) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions combine() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions fail() {
            return Mercantile.PROTOCOL_FAILURE;
        }

        default Transitions finishCheckpoint() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions nextView() {
            return null;
        }

        default Transitions regenerate() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions regenerated() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions start() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions synchronizationFailed() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions synchronizing() {
            throw fsm().invalidTransitionOn();
        }
    }
}
