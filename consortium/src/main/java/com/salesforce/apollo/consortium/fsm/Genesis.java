/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import com.chiralbehaviors.tron.Entry;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.TotalOrdering;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.Consortium.CollaboratorContext;
import com.salesforce.apollo.membership.Member;

/**
 * State machine submap for generating the genesis block
 *
 * @author hal.hildebrand
 *
 */
public enum Genesis implements Transitions {

    FOLLOWER {

        @Override
        public Transitions deliverBlock(Block block, Member from) {
            context().deliverGenesisBlock(block, from);
            return null;
        }

        @Override
        public Transitions deliverTotalOrdering(TotalOrdering msg, Member from) {
            return null;
        }

        @Override
        public Transitions deliverTransaction(Transaction txn, Member from) {
            context().receive(txn);
            return null;
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            context().validate(validation);
            return null;
        }

        @Override
        public Transitions fail() {
            fsm().pop().fail();
            return null;
        }

        @Override
        public Transitions genesisAccepted() {
            return ORDERED;
        }

        @Override
        public Transitions missingGenesis() {
            return null;
        }

        @Override
        public Transitions receive(Transaction transacton, Member from) {
            context().receive(transacton);
            return null;
        }
    },
    GENERATE {

        @Override
        public Transitions becomeFollower() {
            return FOLLOWER;
        }

        @Override
        public Transitions becomeLeader() {
            return LEADER;
        }

        @Override
        public Transitions deliverTransaction(Transaction txn, Member from) {
            context().receive(txn);
            return null;
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            context().validate(validation);
            return null;
        }

        @Override
        public Transitions fail() {
            return null;
        }

        @Override
        public Transitions genesisAccepted() {
            return ORDERED;
        }

        @Override
        public Transitions receive(Transaction transacton, Member from) {
            context().receive(transacton);
            return null;
        }

    },
    LEADER {
        @Override
        public Transitions deliverTransaction(Transaction txn, Member from) {
            context().receive(txn);
            return null;
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            context().validate(validation);
            context().totalOrderDeliver();
            return null;
        }

        @Override
        public Transitions fail() {
            fsm().pop().fail();
            return null;
        }

        @Entry
        public void generate() {
            context().generateGenesis();
        }

        @Override
        public Transitions genesisAccepted() {
            return ORDERED;
        }

        @Override
        public Transitions receive(Transaction transacton, Member from) {
            context().receive(transacton);
            return null;
        }

    },
    ORDERED {
        @Override
        public Transitions becomeClient() {
            fsm().pop().becomeClient();
            return null;
        }

        @Entry
        public void cancelAll() {
            CollaboratorContext context = context();
            context.quiesce();
        }

        @Override
        public Transitions deliverBlock(Block block, Member from) {
            return null;
        }

        @Override
        public Transitions deliverTransaction(Transaction txn, Member from) {
            return null;
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            return null;
        }

        @Override
        public Transitions genesisAccepted() {
            return null;
        }

        @Override
        public Transitions join() {
            fsm().pop().joinGenesis();
            return null;
        }

        @Override
        public Transitions receive(Transaction transacton, Member from) {
            return null;
        }

    },
}
