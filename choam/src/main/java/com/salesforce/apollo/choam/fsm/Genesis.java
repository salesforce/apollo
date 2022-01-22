/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.FsmExecutor;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;

/**
 * @author hal.hildebrand
 *
 */
public interface Genesis {
    public interface Transitions extends FsmExecutor<Genesis, Genesis.Transitions> {

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
