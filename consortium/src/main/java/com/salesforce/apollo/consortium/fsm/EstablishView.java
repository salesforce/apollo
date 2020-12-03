/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.Exit;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.Consortium.Timers;
import com.salesforce.apollo.consortium.CurrentBlock;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public enum EstablishView implements Transitions {

    BUILD {

        @Exit
        public void cancel() {
            context().cancel(Timers.AWAIT_VIEW_MEMBERS);
        }

        @Override
        public Transitions formView() {
            return ESTABLISHED;
        }

        @Entry
        public void petitionToJoin() {
            context().joinView();
        }
    },
    ESTABLISHED {

        @Override
        public Transitions becomeFollower() {
            return FOLLOWER;
        }

        @Override
        public Transitions becomeLeader() {
            return LEADER;
        }
    },
    FOLLOWER, LEADER {

        @Override
        public Transitions deliverBlock(Block block, Member from) {
            throw fsm().invalidTransitionOn();
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            context().deliverValidate(validation);
            context().totalOrderDeliver();
            return null;
        }

        @Entry
        public void gen() {
            context().generateView();
        }

        @Override
        public Transitions processGenesis(CurrentBlock next) {
            context().processGenesis(next);
            return null;
        }
    };

    @Override
    public Transitions deliverTransaction(Transaction txn, Member from) {
        context().receiveJoin(txn);
        return null;
    }

    @Override
    public Transitions receive(Transaction transacton, Member from) {
        context().receiveJoin(transacton);
        return null;
    }

    @Override
    public Transitions deliverBlock(Block block, Member from) {
        context().deliverBlock(block, from);
        return null;
    }
}
