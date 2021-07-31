/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.chiralbehaviors.tron.Fsm;
import com.salesforce.apollo.choam.fsm.Regenerate;
import com.salesforce.apollo.choam.fsm.Regeneration;

/**
 * @author hal.hildebrand
 *
 */
public class ViewRegeneration {
    public class Regen implements Regeneration {

        @Override
        public void awaitView() {
            // TODO Auto-generated method stub

        }

        @Override
        public void establishPrincipal() {
            // TODO Auto-generated method stub

        }

        @Override
        public void generateView() {
            // TODO Auto-generated method stub

        }
    }

    private final Fsm<Regeneration, Regeneration.Transitions> fsm;
    @SuppressWarnings("unused")
    private final Regeneration.Transitions                    transitions;

    public ViewRegeneration() {
        fsm = Fsm.construct(new Regen(), Regeneration.Transitions.class, Regenerate.BUILD, true);
        transitions = fsm.getTransitions();
    }
}
