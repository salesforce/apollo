/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.FsmExecutor;

/**
 * @author hal.hildebrand
 *
 */
public interface Regeneration {
    interface Transitions extends FsmExecutor<Regeneration, Transitions> {

        default Transitions assumeDelegate() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions assumePrincipal() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions establish() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions generated() {
            throw fsm().invalidTransitionOn();
        }
    }

    void awaitView();

    void establishPrincipal();

    void generateView();
}
