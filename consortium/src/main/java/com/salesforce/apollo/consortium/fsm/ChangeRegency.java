/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import java.util.List;

import com.chiralbehaviors.tron.Entry;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.CurrentBlock;
import com.salesforce.apollo.consortium.EnqueuedTransaction;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public enum ChangeRegency implements Transitions {
    AWAIT_SYNCHRONIZATION {

        @Override
        public Transitions deliverStop(Stop stop, Member from) {
            context().deliverStop(stop, from);
            return null;
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            context().deliverStopData(stopData, from);
            return null;
        }

        @Override
        public Transitions deliverSync(Sync syncData, Member from) {
            context().deliverSync(syncData, from);
            return SYNCHRONIZED;
        }

        @Entry
        public void regentElected() {
            context().establishNextRegent();
        }

        @Override
        public Transitions startRegencyChange(List<EnqueuedTransaction> transactions) {
            return null;
        }

        @Override
        public Transitions syncd() {
            return SYNCHRONIZED;
        }

        @Override
        public Transitions synchronizingLeader() {
            return SYNCHRONIZING_LEADER;
        }
    },
    INITIAL {
        @Override
        public Transitions continueChangeRegency(List<EnqueuedTransaction> transactions) {
            context().changeRegency(transactions);
            return null;
        }

        @Override
        public Transitions deliverStop(Stop stop, Member from) {
            context().deliverStop(stop, from);
            return null;
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            context().deliverStopData(stopData, from);
            return null;
        }

        @Override
        public Transitions deliverSync(Sync syncData, Member from) {
            context().establishNextRegent();
            context().deliverSync(syncData, from);
            return SYNCHRONIZED;
        }

        @Override
        public Transitions establishNextRegent() {
            return AWAIT_SYNCHRONIZATION;
        }

        @Override
        public Transitions startRegencyChange(List<EnqueuedTransaction> transactions) {
            return null;
        }

        @Override
        public Transitions syncd() {
            return SYNCHRONIZED;
        }

        @Override
        public Transitions synchronizingLeader() {
            return SYNCHRONIZING_LEADER;
        }
    },
    SYNCHRONIZED {

        @Override
        public Transitions becomeFollower() {
            fsm().pop().becomeFollower();
            return null;
        }

        @Override
        public Transitions becomeLeader() {
            fsm().pop().becomeLeader();
            return null;
        }

        @Override
        public Transitions deliverStop(Stop stop, Member from) {
            return null;
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            return null;
        }
    },
    SYNCHRONIZING_LEADER {

        @Override
        public Transitions deliverStop(Stop stop, Member from) {
            return null;
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            return null;
        }

        @Override
        public Transitions deliverSync(Sync syncData, Member from) {
            context().deliverSync(syncData, from);
            return null;
        }

        @Override
        public Transitions syncd() {
            return SYNCHRONIZED;
        }

    };

    @Override
    public Transitions deliverBlock(Block block, Member from) {
        context().deliverBlock(block, from);
        return null;
    }

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
        context().deliverValidate(validation);
        return null;
    }

    @Override
    public Transitions genesisAccepted() {
        fsm().pop().genesisAccepted();
        return null;
    }

    @Override
    public Transitions receive(Transaction transacton, Member from) {
        context().receive(transacton);
        return null;
    }

    @Override
    public Transitions processCheckpoint(CurrentBlock next) {
        context().processCheckpoint(next);
        return null;
    }

    @Override
    public Transitions processGenesis(CurrentBlock next) {
        context().processGenesis(next);
        return null;
    }

    @Override
    public Transitions processReconfigure(CurrentBlock next) {
        context().processReconfigure(next);
        return null;
    }

    @Override
    public Transitions processUser(CurrentBlock next) {
        context().processUser(next);
        return null;
    }
}
