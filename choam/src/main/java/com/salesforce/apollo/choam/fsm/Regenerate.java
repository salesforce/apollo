/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry;

/**
 * Reconfigures the view for the committee
 * 
 * @author hal.hildebrand
 *
 */
public enum Regenerate implements DrivenTransitions {
    BUILD {

        @Override
        public DrivenTransitions assumeDelegate() {
            return DELEGATE;
        }

        @Override
        public DrivenTransitions assumePrincipal() {
            return PRINCIPAL;
        }

        @Entry
        public void establishPrincipal() {
            context().establishPrincipal();
        }
    },
    DELEGATE { 
        @Entry
        public void awaitView() {
            context().awaitView();
        }
    }, ESTABLISHED, GENERATED   {

        @Override
        public DrivenTransitions assumeDelegate() {
            return Earner.DELEGATE;
        }

        @Override
        public DrivenTransitions assumePrincipal() {
            return Earner.PRINCIPAL;
        }

        @Entry
        public void establishPrincipal() {
            context().establishPrincipal();
        }
    },
    PRINCIPAL { 
        @Entry
        public void generateView() {
            context().generateView();
        }
    };

    @Override
    public DrivenTransitions establish() {
        return ESTABLISHED;
    }

    @Override
    public DrivenTransitions generated() {
        return GENERATED;
    }
}
