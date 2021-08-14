/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.FsmExecutor;
import com.salesfoce.apollo.choam.proto.Joins;
import com.salesfoce.apollo.choam.proto.Validate;

/**
 * @author hal.hildebrand
 *
 */
public interface Reconfiguration {
    interface Transitions extends FsmExecutor<Reconfiguration, Transitions> {
        default Transitions assembled() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions failed() {
            return Reconfigure.PROTOCOL_FAILURE;
        }

        default Transitions joins(Joins joins) {
            throw fsm().invalidTransitionOn();
        }

        default Transitions nominated() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions reconfigured() {
            throw fsm().invalidTransitionOn();
        }

        default Transitions validate(Validate validate) {
            throw fsm().invalidTransitionOn();
        }
    }

    void complete();

    void convene();

    void failed();

    void gatherAssembly();

    void validation(Validate validate);
}
