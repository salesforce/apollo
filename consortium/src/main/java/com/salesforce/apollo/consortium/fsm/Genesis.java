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
import com.salesfoce.apollo.consortium.proto.Proclamation;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.PendingTransactions.EnqueuedTransaction;
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
        public Transitions submit(EnqueuedTransaction enqueuedTransaction) {
            context().submitJoin(enqueuedTransaction);
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

        @Exit
        public void cancel() {
            context().cancelAll();
        }

        @Override
        public Transitions deliverBlock(Block block, Member from) {
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
            return null;
        }

        @Override
        public Transitions fail() {
            context().awaitFormation();
            return null;
        }

        @Override
        public Transitions genesisAccepted() {
            return ORDERED;
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
    LEADER {
        @Override
        public Transitions deliverTransaction(Transaction txn) {
            context().add(txn);
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
        public Transitions submit(EnqueuedTransaction enqueuedTransaction) {
            context().submitJoin(enqueuedTransaction);
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
            context().cancelAll();
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

        @Override
        public Transitions join() {
            fsm().pop().joinGenesis();
            return null;
        }

        @Override
        public Transitions submit(EnqueuedTransaction enqueuedTransaction) {
            return null;
        }

    },
}
