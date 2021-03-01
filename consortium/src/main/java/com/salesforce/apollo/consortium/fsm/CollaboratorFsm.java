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
import com.salesfoce.apollo.consortium.proto.CheckpointProcessing;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.CollaboratorContext;
import com.salesforce.apollo.consortium.Consortium.Timers;
import com.salesforce.apollo.membership.Member;

/**
 * Finite state machine for the Collaborator in a Consortium
 *
 * @author hal.hildebrand
 *
 */
public enum CollaboratorFsm implements Transitions {

    CHECKPOINT_RECOVERY {
    },
    CLIENT {
    },
    FOLLOWER {

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
        public Transitions deliverCheckpointing(CheckpointProcessing checkpointProcessing, Member from) {
            context().deliverCheckpointing(checkpointProcessing, from);
            return Checkpointing.FOLLOWER;
        }

        @Override
        public Transitions joinAsMember() {
            return JOINING_MEMBER;
        }
    },
    INITIAL {

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
        public Transitions deliverValidate(Validate validation) {
            return null;
        }

        @Override
        public Transitions receive(ReplicateTransactions txns, Member from) {
            return null;
        }

        @Override
        public Transitions receive(Transaction transacton, Member from) {
            return null;
        }

    },
    LEADER {

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

        @Exit
        public void cancelBatchGeneration() {
            CollaboratorContext context = context();
            context.cancel(Timers.FLUSH_BATCH);
        }

        @Override
        public Transitions deliverValidate(Validate validation) {
            CollaboratorContext context = context();
            context.deliverValidate(validation);
            context.totalOrderDeliver();
            return null;
        }

        @Entry
        public void generate() {
            context().initializeConsensus();
            context().generateBlock();
        }

        @Override
        public Transitions generateCheckpoint() {
            context().cancel(Timers.FLUSH_BATCH);
            return Checkpointing.LEADER;
        }

        @Override
        public Transitions joinAsMember() {
            return JOINING_MEMBER;
        }
    },
    PROTOCOL_FAILURE {

        @Override
        public Transitions becomeClient() {
            throw fsm().invalidTransitionOn();
        }

        @Override
        public Transitions becomeFollower() {
            throw fsm().invalidTransitionOn();
        }

        @Override
        public Transitions becomeLeader() {
            throw fsm().invalidTransitionOn();
        }

        @Override
        public Transitions joinAsMember() {
            throw fsm().invalidTransitionOn();
        }

        @Entry
        public void terminate() {
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
        public Transitions joinAsMember() {
            return JOINING_MEMBER;
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
        public Transitions joinAsMember() {
            return JOINING_MEMBER;
        }

        @Override
        public Transitions missingGenesis() {
            context().awaitGenesis();
            return null;
        }
    };
}
