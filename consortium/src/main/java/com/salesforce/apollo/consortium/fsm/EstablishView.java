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
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.CollaboratorContext;
import com.salesforce.apollo.consortium.Consortium.Timers;
import com.salesforce.apollo.consortium.support.EnqueuedTransaction;
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
    FOLLOWER {

        @Override
        public Transitions becomeFollower() {
            return null;
        }

        @Override
        public Transitions becomeLeader() {
            return LEADER;
        }

        @Override
        public Transitions deliverStop(Stop stop, Member from) {
            context().delay(stop, from);
            return null;
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            context().delay(stopData, from);
            return null;
        }

        @Override
        public Transitions deliverSync(Sync sync, Member from) {
            context().delay(sync, from);
            return null;
        }

        @Override
        public Transitions receive(ReplicateTransactions txns, Member from) {
            context().delay(txns, from);
            return null;
        }

        @Override
        public Transitions startRegencyChange(List<EnqueuedTransaction> transactions) {
            return null;
        }

    },
    GENERATED, LEADER {

        @Override
        public Transitions becomeFollower() {
            return FOLLOWER;
        }

        @Override
        public Transitions becomeLeader() {
            return null;
        }

        @Override
        public Transitions deliverBlock(Block block, Member from) {
            throw fsm().invalidTransitionOn();
        }

        @Override
        public Transitions deliverStop(Stop stop, Member from) {
            context().delay(stop, from);
            return null;
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            context().delay(stopData, from);
            return null;
        }

        @Override
        public Transitions deliverSync(Sync sync, Member from) {
            context().delay(sync, from);
            return null;
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            CollaboratorContext context = context();
            context.deliverValidate(validation);
            context.totalOrderDeliver();
            return null;
        }

        @Entry
        public void gen() {
            context().generateView();
        }

        @Override
        public Transitions receive(ReplicateTransactions txns, Member from) {
            context().delay(txns, from);
            return null;
        }

        @Override
        public Transitions startRegencyChange(List<EnqueuedTransaction> transactions) {
            return null;
        }
    };

    @Override
    public Transitions genesisAccepted() {
        return GENERATED;
    }

    @Override
    public Transitions receive(Transaction transacton, Member from) {
        context().receiveJoin(transacton);
        return null;
    }
}
