/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import java.util.List;

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.Exit;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.TotalOrdering;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.Consortium.Timers;
import com.salesforce.apollo.consortium.CurrentBlock;
import com.salesforce.apollo.consortium.EnqueuedTransaction;
import com.salesforce.apollo.membership.Member;

/**
 * Finite state machine for the Collaborator in a Consortium
 *
 * @author hal.hildebrand
 *
 */
public enum CollaboratorFsm implements Transitions {

    CLIENT {
    },
    FOLLOWER

    {

        @Override
        public Transitions becomeFollower() {
            return null;
        }

        @Override
        public Transitions deliverBlock(Block block, Member from) {
            context().deliverBlock(block, from);
            return null;
        }

        @Override
        public Transitions deliverStop(Stop stop, Member from) {
            return null; // TODO
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            return null; // TODO
        }

        @Override
        public Transitions deliverSync(Sync syncData, Member from) {
            return null; // TODO
        }

        @Override
        public Transitions deliverTotalOrdering(TotalOrdering msg, Member from) {
            context().deliverTotalOrdering(msg, from);
            return null;
        }

        @Override
        public Transitions startRegencyChange(List<EnqueuedTransaction> transactions) {
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
        public Transitions deliverStop(Stop stop, Member from) {
            return null;
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            // TODO Auto-generated method stub
            return super.deliverStopData(stopData, from);
        }

        @Override
        public Transitions deliverSync(Sync syncData, Member from) {
            // TODO Auto-generated method stub
            return super.deliverSync(syncData, from);
        }

        @Override
        public Transitions deliverTransaction(Transaction txn, Member from) {
            return null;
        }

        @Override
        public Transitions deliverTransactions(ReplicateTransactions txns, Member from) {
            return null;
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            return null;
        }

        @Override
        public Transitions receive(Transaction transacton, Member from) {
            return null;
        }

        @Override
        public Transitions startRegencyChange(List<EnqueuedTransaction> transactions) {
            return null;
        }

    },
    LEADER {
        @Override
        public Transitions deliverBlock(Block block, Member from) {
            return null;
        }

        @Override
        public Transitions deliverStop(Stop stop, Member from) {
            return null; // TODO
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            return null; // TODO
        }

        @Override
        public Transitions deliverSync(Sync syncData, Member from) {
            return null; // TODO
        }

        @Override
        public Transitions deliverTotalOrdering(TotalOrdering msg, Member from) {
            context().deliverTotalOrdering(msg, from);
            return null;
        }

        @Override
        public Transitions drainPending() {
            context().drainBlocks();
            return null;
        }

        @Entry
        public void generate() {
            context().generateBlocks();
        }

        @Override
        public Transitions startRegencyChange(List<EnqueuedTransaction> transactions) {
            return null;
        }
    },
    PROTOCOL_FAILURE {

        @Override
        public Transitions processCheckpoint(CurrentBlock next) {
            throw fsm().invalidTransitionOn();
        }

        @Override
        public Transitions processGenesis(CurrentBlock next) {
            throw fsm().invalidTransitionOn();
        }

        @Override
        public Transitions processReconfigure(CurrentBlock next) {
            throw fsm().invalidTransitionOn();
        }

        @Override
        public Transitions processUser(CurrentBlock next) {
            throw fsm().invalidTransitionOn();
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
        public Transitions becomeFollower() {
            return FOLLOWER;
        }

        @Override
        public Transitions becomeLeader() {
            return LEADER;
        }

        @Override
        public Transitions genesisAccepted() {
            return null;
        }

        @Override
        public Transitions joinAsMember() {
            return JOINING_MEMBER;
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
            context().establishGenesisView();
        }

        @Override
        public Transitions becomeClient() {
            return RECOVERED;
        }

        @Override
        public Transitions becomeFollower() {
            return FOLLOWER;
        }

        @Override
        public Transitions becomeLeader() {
            return LEADER;
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
        public Transitions generateView() {
            fsm().push(EstablishView.BUILD);
            return null;
        }

        @Override
        public Transitions genesisAccepted() {
            return RECOVERED;
        }

        @Override
        public Transitions joinAsMember() {
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

    @Override
    public Transitions deliverTransaction(Transaction txn, Member from) {
        context().receive(txn);
        return null;
    }

    @Override
    public Transitions deliverTransactions(ReplicateTransactions txns, Member from) {
        context().receive(txns, from);
        return null;
    }

    @Override
    public Transitions deliverValidate(Validate validation) {
        context().validate(validation);
        context().totalOrderDeliver();
        return null;
    }

    @Override
    public Transitions receive(Transaction transacton, Member from) {
        context().receive(transacton);
        return null;
    }
}
