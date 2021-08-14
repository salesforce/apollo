/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.choam.fsm.Reconfiguration.Transitions;

/**
 * @author hal.hildebrand
 *
 */
public enum Reconfigure implements Transitions {
    PROTOCOL_FAILURE {
        @Entry
        public void terminate() {
            context().failed();
        }
    },
    GATHER {

        @Override
        public Transitions assembled() {
            return CONVENE;
        }

        @Entry
        public void assembly() {
            context().gatherAssembly();
        }
    },
    CONVENE {

        @Entry
        public void consolidate() {
            context().convene();
        }

        @Override
        public Transitions nominated() {
            return NOMINATION;
        }
    },
    NOMINATION {

        @Override
        public Transitions validate(Validate validate) {
            context().validation(validate);
            return null;
        }

        @Override
        public Transitions reconfigured() {
            return RECONFIGURED;
        }
    },
    RECONFIGURED {

        @Entry
        public void complete() {
            context().complete();
        }
    };
}
