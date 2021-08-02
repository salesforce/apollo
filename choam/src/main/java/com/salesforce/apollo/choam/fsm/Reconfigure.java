/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.Exit;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.Joins;
import com.salesfoce.apollo.choam.proto.Publish;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesforce.apollo.choam.fsm.Driven.Transitions;

/**
 * @author hal.hildebrand
 *
 */
public enum Reconfigure implements Transitions {
    CHANGE_PRINCIPAL, GATHER {
        @Override
        public Transitions assembled() {
            return NOMINATE;
        }

        @Entry
        public void assembly() {
            context().gatherAssembly();
        }
    },
    NOMINATE {
        @Exit
        public void cancelTimer() {
            context().cancelTimer(Driven.RECONVENE);
        }

        @Entry
        public void consolidate() {
            context().convene();
        }

        @Override
        public Transitions joins(Joins joins) {
            context().assemble(joins);
            return null;
        }

        @Override
        public Transitions nominated() {
            return NOMINATION;
        }
    },
    NOMINATION {
        @Exit
        public void cancelTimer() {
            context().cancelTimer(Driven.RECONFIGURE);
        }

        @Override
        public Transitions joins(Joins joins) {
            return null; // ignored upon nomination
        }

        @Entry
        public void publish() {
            context().reconfigure();
        }

        @Override
        public Transitions publish(Publish published) {
            context().published(published);
            return null;
        }

        @Override
        public Transitions reconfigure(Block reconfigure) {
            context().reconfigure(reconfigure);
            return null;
        }

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
    RECONFIGURED {

        @Override
        public Transitions publish(Publish publish) {
            return null; // ignored after reconfiguration
        }

        @Override
        public Transitions reconfigure(Block reconfigure) {
            return null; // ignored after reconfiguration
        }

        @Override
        public Transitions validate(Validate validate) {
            return null; // ignored after reconfiguration
        }
    };
}
