/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import com.chiralbehaviors.tron.Entry;

/**
 * State machine submap for generating a view
 * 
 * @author hal.hildebrand
 *
 */
public enum GenerateView implements Transitions {

    FAILURE, INITIAL {

        @Override
        public Transitions fail() {
            return FAILURE;
        }

        public Transitions success() {
            return KEY_GENERATED;
        }
    },
    KEY_GENERATED {

        @Override
        public Transitions fail() {
            return FAILURE;
        }

        @Entry
        public void generateConsensusKey() {
            context().awaitViewMembers();
        }

        @Override
        public Transitions success() {
            return ELECT_LEADER;
        }
    },
    ELECT_LEADER { 
    };

}
