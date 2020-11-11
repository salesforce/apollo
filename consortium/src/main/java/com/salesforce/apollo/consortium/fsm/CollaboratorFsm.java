/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    AWAIT_VIEW_CHANGE {

        @Override
        public Transitions fail() {
            return PROTOCOL_FAILURE;
        }
    },
    CLIENT, FOLLOWER

    {

        @Override
        public Transitions deliverBlock(Block block, Member from) {
            context().deliverBlock(block, from);
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

    },
    GENERATE_GENESIS {
        public Transitions becomeFollower() {
            return GENERATE_GENESIS_FOLLOWER;
        }

        public Transitions becomeLeader() {
            return GENERATE_GENESIS_LEADER;
        }

        @Exit
        public void cancel() {
            context().cancel(Timers.AWAIT_FORMATION);
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
            return null;
        }

        @Override
        public Transitions deliverBlock(Block block, Member from) {
            return null;
        }

        @Override
        public Transitions fail() {
            context().awaitFormation();
            return null;
        }

        @Override
        public Transitions submit(EnqueuedTransaction enqueuedTransaction) {
            context().submitJoin(enqueuedTransaction);
            return null;
        }

        @Entry
        public void vote() {
            context().awaitFormation();
        }
    },
    GENERATE_GENESIS_FOLLOWER {
        @Override
        public Transitions deliverBlock(Block block, Member from) {
            context().deliverBlock(block, from);
            return null;
        }

        @Override
        public Transitions deliverProclamation(Proclamation p, Member from) {
            context().resendPending(p, from);
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

        public Transitions fail() {
            return PROTOCOL_FAILURE;
        }

        @Override
        public Transitions missingGenesis() {
            return null;
        }

        @Override
        public Transitions submit(EnqueuedTransaction enqueuedTransaction) {
            context().submitJoin(enqueuedTransaction);
            return null;
        }

        public Transitions success() {
            return FOLLOWER;
        }
    },
    GENERATE_GENESIS_LEADER {
        @Override
        public Transitions deliverBlock(Block block, Member from) {
            context().deliverBlock(block, from);
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

        public Transitions fail() {
            return PROTOCOL_FAILURE;
        }

        @Entry
        public void generate() {
            context().generateGenesis();
        }

        @Override
        public Transitions submit(EnqueuedTransaction enqueuedTransaction) {
            context().submitJoin(enqueuedTransaction);
            return null;
        }

        public Transitions success() {
            return LEADER;
        }

    },
    GENESIS_PROCESSED {

        @Override
        public Transitions becomeClient() {
            return CLIENT;
        }

        @Override
        public Transitions becomeFollower() {
            return FOLLOWER;
        }

        @Override
        public Transitions becomeLeader() {
            return LEADER;
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
    JOINING {

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
        public Transitions fail() {
            return PROTOCOL_FAILURE;
        }

        @Override
        public Transitions submit(EnqueuedTransaction enqueuedTransaction) {
            context().submit(enqueuedTransaction);
            return null;
        }

        @Override
        public Transitions success() {
            return AWAIT_VIEW_CHANGE;
        }

        @Entry
        public void vote() {
            context().join();
        }
    },
    LEADER {
        @Override
        public Transitions deliverBlock(Block block, Member from) {
            if (!context().member().equals(from)) {
                log.trace("Leader, ignoring block proposal from {}", from);
            }
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

    },
    RECOVERING {
        @Entry
        public void awaitGenesis() {
            context().awaitGenesis();
        }

        public Transitions becomeClient() {
            return null;
        }

        @Exit
        public void cancelTimer() {
            context().cancel(Timers.AWAIT_GENESIS);
        }

        public Transitions join() {
            return GENERATE_GENESIS;
        }

        @Override
        public Transitions missingGenesis() {
            context().awaitGenesis();
            return null;
        }
    };

    private static final Logger log = LoggerFactory.getLogger(CollaboratorFsm.class);

}
