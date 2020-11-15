/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.Exit;
import com.chiralbehaviors.tron.InvalidTransition;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Proclamation;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.Consortium.Timers;
import com.salesforce.apollo.consortium.CurrentBlock;
import com.salesforce.apollo.consortium.PendingTransactions.EnqueuedTransaction;
import com.salesforce.apollo.membership.Member;

/**
 * Finite state machine for the Collaborator in a Consortium
 *
 * @author hal.hildebrand
 *
 */
public enum CollaboratorFsm implements Transitions {
    CLIENT, FOLLOWER

    {

        @Override
        public Transitions deliverBlock(Block block, Member from) {
            context().deliverBlock(block, from);
            return null;
        }

        @Override
        public Transitions deliverProclamation(Proclamation p, Member from) {
            context().deliverProclamation(p, from);
            return null;
        }

        @Override
        public Transitions deliverTransaction(Transaction txn) {
            context().add(txn);
            return null;
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            context().validate(validation);
            return null;
        }

        @Override
        public Transitions submit(EnqueuedTransaction enqueuedTransaction) {
            context().submit(enqueuedTransaction);
            return null;
        }

    },
    INITIAL {
        @Entry
        public void initialize() {
            context().nextView();
        }

        @Override
        public Transitions start() {
            return RECOVERING;
        }
    },
    JOINING_MEMBER {
        @Override
        public Transitions becomeFollower() {
            return FOLLOWER;
        }

        @Override
        public Transitions becomeLeader() {
            return LEADER;
        }

        @Override
        public Transitions deliverBlock(Block block, Member from) {
            return null;
        }

        @Override
        public Transitions deliverProclamation(Proclamation p, Member from) {
            return null;
        }

        @Override
        public Transitions deliverTransaction(Transaction txn) {
            return null;
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            return null;
        }

        @Entry
        public void enterView() {
            context().enterView();
        }

    },
    LEADER {

        @Override
        public Transitions deliverBlock(Block block, Member from) {
            return null;
        }

        @Override
        public Transitions deliverTransaction(Transaction txn) {
            context().evaluate(txn);
            return null;
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            context().validate(validation);
            context().totalOrderDeliver();
            return null;
        }

        @Override
        public Transitions drainPending() {
            context().generateNextBlock();
            return null;
        }

        @Entry
        public void generate() {
            context().becomeLeader();
        }

        @Override
        public Transitions submit(EnqueuedTransaction enqueuedTransaction) {
            context().evaluate(enqueuedTransaction);
            return null;
        }
    },
    PROTOCOL_FAILURE {

        @Override
        public Transitions processCheckpoint(CurrentBlock next) {
            throw new InvalidTransition();
        }

        @Override
        public Transitions processGenesis(CurrentBlock next) {
            throw new InvalidTransition();
        }

        @Override
        public Transitions processReconfigure(CurrentBlock next) {
            throw new InvalidTransition();
        }

        @Override
        public Transitions processUser(CurrentBlock next) {
            throw new InvalidTransition();
        }

        @Entry
        public void shutdown() {
            context().shutdown();
        }

    },
    RECOVERED {
        @Override
        public Transitions becomeClient() {
            return CLIENT;
        }

        @Override
        public Transitions genesisAccepted() {
            return null;
        }

        @Override
        public Transitions processGenesis(CurrentBlock next) {
            context().processGenesis(next);
            return null;
        }

    },
    RECOVERING {
        @Entry
        public void awaitGenesis() {
            context().awaitGenesis();
        }

        @Override
        public Transitions becomeClient() {
            return RECOVERED;
        }

        @Exit
        public void cancelTimer() {
            context().cancel(Timers.AWAIT_GENESIS);
        }

        @Override
        public Transitions fail() {
            return PROTOCOL_FAILURE;
        }

        @Override
        public Transitions join() {
            fsm().push(Genesis.GENERATE);
            return null;
        }

        @Override
        public Transitions joinGenesis() {
            return JOINING_MEMBER;
        }

        @Override
        public Transitions missingGenesis() {
            context().awaitGenesis();
            return null;
        }

        @Override
        public Transitions processGenesis(CurrentBlock next) {
            context().processGenesis(next);
            return null;
        }
    };

}
