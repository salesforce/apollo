/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

/**
 * Producer Finite State Machine
 * 
 * @author hal.hildebrand
 *
 */
public enum Earner implements DrivenTransitions {
    DELEGATE, INITIAL {

        @Override
        public DrivenTransitions regenerate() {
            return Regenerate.BUILD;
        }

        @Override
        public DrivenTransitions start() {
            context().initialState();
            return null;
        }

    },
    PRINCIPAL, PROTOCOL_FAILURE;

    @Override
    public DrivenTransitions assumeDelegate() {
        return DELEGATE;
    }

    @Override
    public DrivenTransitions assumePrincipal() {
        return PRINCIPAL;
    }
}
