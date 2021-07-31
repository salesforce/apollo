/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.salesforce.apollo.choam.fsm.Driven.Transitions;

/**
 * Producer Finite State Machine
 * 
 * @author hal.hildebrand
 *
 */
public enum Earner implements Driven.Transitions {
    DELEGATE, INITIAL {

        @Override
        public Transitions reconfigure() {
            return Reconfigure.GATHER;
        }

        @Override
        public Transitions start() {
            context().initialState();
            return null;
        }

    },
    PRINCIPAL, PROTOCOL_FAILURE;

    @Override
    public Transitions assumeDelegate() {
        return DELEGATE;
    }

    @Override
    public Transitions assumePrincipal() {
        return PRINCIPAL;
    }
}
