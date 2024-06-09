/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.FsmExecutor;

/**
 * @author hal.hildebrand
 */
public interface Reconfiguration {
    void certify();

    void checkAssembly();

    void checkViews();

    void chill();

    void complete();

    void convened();

    void failed();

    void finish();

    void publishViews();

    void vibeCheck();

    enum Reconfigure implements Transitions {
        AWAIT_ASSEMBLY {
            // Publish the Views of this node
            @Entry
            public void publish() {
                context().publishViews();
            }

            // We have a >= majority submitting view proposals
            @Override
            public Transitions proposed() {
                return CONVIENE;
            }
        }, CONVIENE {
            @Override
            public Transitions countdownCompleted() {
                return proposed();
            }

            // We have a >= majority of members submitting view proposals
            @Override
            public Transitions proposed() {
                return VIEW_AGREEMENT;
            }

            @Entry
            public void conviene() {
                context().convened();
            }
        }, CERTIFICATION {
            // We have a full complement of the committee view proposals
            @Override
            public Transitions certified() {
                return RECONFIGURED;
            }

            // We have waited to get a full compliment and have now finished waiting
            @Override
            public Transitions countdownCompleted() {
                return RECONFIGURED;
            }

            // See if we already have a full complement of Joins of the next committee
            // if not set a deadline
            @Entry
            public void certify() {
                context().certify();
            }
        }, GATHER {
            @Override
            public Transitions chill() {
                return CHILLIN;
            }

            // We have a full complement of the new committee Joins
            @Override
            public Transitions certified() {
                return CERTIFICATION;
            }

            // Check to see if we already have a full complement of committee Joins
            @Entry
            public void gather() {
                context().checkAssembly();
            }

            @Override
            public Transitions checkAssembly() {
                context().checkAssembly();
                return null;
            }
        }, CHILLIN {
            @Override
            public Transitions countdownCompleted() {
                return certified();
            }

            // We have what we have
            @Override
            public Transitions certified() {
                return CERTIFICATION;
            }

            @Override
            public Transitions checkAssembly() {
                context().checkAssembly();
                return null;
            }

            @Entry
            public void vibin() {
                context().vibeCheck();
            }

            // Check to see if we already have a full complement of committee Joins
            @Entry
            public void chillin() {
                context().chill();
            }
        }, PROTOCOL_FAILURE {
            @Override
            public Transitions certified() {
                return null;
            }

            @Override
            public Transitions complete() {
                return null;
            }

            @Override
            public Transitions failed() {
                return null;
            }

            @Entry
            public void terminate() {
                context().failed();
            }
        }, RECONFIGURED {
            // Finish and close down the assembly
            @Override
            public Transitions complete() {
                context().finish();
                return null;
            }

            @Override
            public Transitions countdownCompleted() {
                return null;
            }

            // Complete the configuration protocol
            // The slate of the ViewAssembly now contains
            // the SignedViewMembers of the next committee
            @Entry
            public void completion() {
                context().complete();
            }
        }, VIEW_AGREEMENT {
            // Vote on the Views gathered
            @Entry
            public void viewConsensus() {
                context().checkViews();
            }

            // The View for the assembly has been selected
            @Override
            public Transitions viewAcquired() {
                return GATHER;
            }

            // no op
            @Override
            public Transitions proposed() {
                return null;
            }
        }

    }

    interface Transitions extends FsmExecutor<Reconfiguration, Transitions> {

        default Transitions certified() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions checkAssembly() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions chill() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions complete() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions countdownCompleted() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions failed() {
            return Reconfigure.PROTOCOL_FAILURE;
        }

        default Transitions proposed() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions viewAcquired() {
            throw fsm().invalidTransitionOn();
        }
    }
}
