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

    void complete();

    void elect();

    void failed();

    void finish();

    void publishViews();

    enum Reconfigure implements Transitions {
        AWAIT_ASSEMBLY {
            @Entry
            public void publish() {
                context().publishViews();
            }

            @Override
            public Transitions certified() {
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
        }, GATHER {
            @Override
            public Transitions certified() {
                return CERTIFICATION;
            }

            @Entry
            public void gather() {
                context().checkAssembly();
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
                context().finish();
                return null;
            }

            @Entry
            public void completion() {
                context().complete();
            }
        }, VIEW_AGREEMENT {
            @Entry
            public void viewConsensus() {
                context().checkViews();
            }

            @Override
            public Transitions certified() {
                return GATHER;
            }
        }
    }

    interface Transitions extends FsmExecutor<Reconfiguration, Transitions> {

        default Transitions certified() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions complete() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions failed() {
            return Reconfigure.PROTOCOL_FAILURE;
        }
    }
}
