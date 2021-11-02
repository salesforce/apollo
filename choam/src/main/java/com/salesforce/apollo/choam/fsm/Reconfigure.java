/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry;
import com.salesforce.apollo.choam.fsm.Reconfiguration.Transitions;

/**
 * @author hal.hildebrand
 *
 */
public enum Reconfigure implements Transitions {
    CERTIFICATION {
        @Entry
        public void certify() {
            context().certify();
        }

        @Override
        public Transitions nextEpoch(int epoch) {
            return RECONFIGURE;
        }
    },
    GATHER {

        @Entry
        public void assembly() {
            context().gather();
        }

        @Override
        public Transitions gathered() {
            return NOMINATION;
        }

        @Override
        public Transitions nextEpoch(int epoch) {
            return null;
        }
    },
    NOMINATION {

        @Override
        public Transitions nextEpoch(int eoch) {
            return CERTIFICATION;
        }

        @Entry
        public void nominate() {
            context().nominate();
        }
    },
    PROTOCOL_FAILURE {
        @Entry
        public void terminate() {
            context().failed();
        }
    },
    RECONFIGURE {
        @Override
        public Transitions complete() {
            return RECONFIGURED;
        }

        @Entry
        public void elect() {
            context().elect();
        }
    },
    RECONFIGURE_BLOCK {
        @Entry
        public void certifyBlock() {
            context().certifyBlock();
        }

        @Override
        public Transitions complete() {
            context().produceBlock();
            return null;
        }

        @Override
        public Transitions nextEpoch(int epoch) {
            return null;
        }

    },
    RECONFIGURED {

        @Override
        public Transitions complete() {
            return null;
        }

        @Entry
        public void completion() {
            context().complete();
        }

        @Override
        public Transitions reconfigureBlock() {
            return RECONFIGURE_BLOCK;
        }
    };
}
