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

    void complete();

    void elect();

    void failed();

    void viewAgreement();

    enum Reconfigure implements Transitions {
        AWAIT_ASSEMBLY {
            @Override
            public Transitions assembled() {
                return VIEW_AGREEMENT;
            }
        }, CERTIFICATION {
            @Override
            public Transitions certified() {
                return RECONFIGURE;
            }

            @Entry
            public void certify() {
                context().certify();
            }

            @Override
            public Transitions gathered() {
                return CERTIFICATION;
            }
        }, GATHER {
            @Override
            public Transitions gathered() {
                return CERTIFICATION;
            }

            @Override
            public Transitions viewDetermined() {
                return null;
            }
        }, PROTOCOL_FAILURE {
            @Override
            public Transitions assembled() {
                return null;
            }

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
        }, RECONFIGURE {
            @Override
            public Transitions complete() {
                return RECONFIGURED;
            }

            @Entry
            public void elect() {
                context().elect();
            }
        }, RECONFIGURED {
            @Override
            public Transitions complete() {
                return null;
            }

            @Entry
            public void completion() {
                context().complete();
            }
        }, VIEW_AGREEMENT {
            @Entry
            public void viewConsensus() {
                context().viewAgreement();
            }

            @Override
            public Transitions viewDetermined() {
                return GATHER;
            }
        }
    }

    interface Transitions extends FsmExecutor<Reconfiguration, Transitions> {
        default Transitions assembled() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions certified() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions complete() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions failed() {
            return Reconfigure.PROTOCOL_FAILURE;
        }

        default Transitions gathered() {
            return null;
        }

        default Transitions viewDetermined() {
            throw fsm().invalidTransitionOn();
        }
    }
}
