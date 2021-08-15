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
    COMPLETED {

        @Entry
        public void completion() {
            context().complete();
        }

        @Override
        public Transitions validate(Validate validate) {
            // Ignored, as we have already published the reconfiguration block
            return null;
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

        @Override
        public Transitions validate(Validate validate) {
            context().validation(validate);
            return null;
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
    NOMINATION {

        @Override
        public Transitions reconfigured() {
            return RECONFIGURED;
        }

        @Override
        public Transitions validate(Validate validate) {
            context().validation(validate);
            return null;
        }
    },
    PROTOCOL_FAILURE {
        @Entry
        public void terminate() {
            context().failed();
        }
    },
    RECONFIGURED {

        @Override
        public Transitions complete() {
            return COMPLETED;
        }

        @Entry
        public void validations() {
            context().continueValidating();
        }

        @Override
        public Transitions validate(Validate validate) {
            // Ignored, as we have already published the reconfiguration block
            return null;
        }

    };
}
