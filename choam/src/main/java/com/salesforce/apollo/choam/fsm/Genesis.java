/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.FsmExecutor;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;

/**
 * @author hal.hildebrand
 *
 */
public interface Genesis {
    enum BrickLayer implements Transitions {

        CERTIFICATION {
            @Entry
            public void certify() {
                context().certify();
            }

            @Override
            public Transitions process(PreBlock preblock, boolean last) {
                context().certify(preblock, last);
                return last ? PUBLISH : null;
            }
        },
        FAIL {
        },
        INITIAL {
            @Entry
            public void gather() {
                context().gather();
            }

            @Override
            public Transitions nextEpoch(Integer epoch) {
                return epoch.equals(0) ? null : NOMINATION;

            }

            @Override
            public Transitions process(PreBlock preblock, boolean last) {
                context().gather(preblock, last);
                return null;
            }
        },
        NOMINATION {
            @Override
            public Transitions nextEpoch(Integer epoch) {
                return CERTIFICATION;
            }

            @Entry
            public void nominate() {
                context().nominate();
            }

            @Override
            public Transitions process(PreBlock preblock, boolean last) {
                context().nominations(preblock, last);
                return null;
            }
        },
        PUBLISH {
            @Entry
            public void publish() {
                context().publish();
            }
        };

    }

    interface Transitions extends FsmExecutor<Genesis, Genesis.Transitions> {

        default Transitions nextEpoch(Integer epoch) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions process(PreBlock preblock, boolean last) {
            throw fsm().invalidTransitionOn();
        }
    }

    public void certify();

    public void certify(PreBlock preblock, boolean last);

    public void gather();

    public void gather(PreBlock preblock, boolean last);

    public void nominate();

    public void nominations(PreBlock preblock, boolean last);

    public void publish();
}
