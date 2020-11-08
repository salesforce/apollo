/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.Consortium.CurrentBlock;
import com.salesforce.apollo.consortium.PendingTransactions.EnqueuedTransaction;
import com.salesforce.apollo.membership.Member;

/**
 * Finite state machine for the Collaborator in a Consortium
 * 
 * @author hal.hildebrand
 *
 */
public enum CollaboratorFsm implements Transitions {
    CLIENT {

        @Override
        public Transitions processCheckpoint(CurrentBlock next) {
            if (!context().processCheckpoint(next)) {
                return PROTOCOL_FAILURE;
            }
            return null;
        }

        @Override
        public Transitions processUser(CurrentBlock next) {
            if (!context().processUser(next)) {
                return PROTOCOL_FAILURE;
            }
            return null;
        }
    },
    FOLLOWER

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

        @Override
        public Transitions processCheckpoint(CurrentBlock next) {
            if (!context().processCheckpoint(next)) {
                return PROTOCOL_FAILURE;
            }
            return null;
        }

        @Override
        public Transitions processUser(CurrentBlock next) {
            if (!context().processUser(next)) {
                return PROTOCOL_FAILURE;
            }
            return null;
        }

        @Override
        public Transitions submit(EnqueuedTransaction enqueuedTransaction) {
            context().submit(enqueuedTransaction);
            return null;
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

        @Override
        public Transitions genesisAccepted() {
            return GENESIS_PROCESSED;
        }

        @Override
        public Transitions processGenesis(CurrentBlock next) {
            if (!context().processGenesis(next)) {
                return PROTOCOL_FAILURE;
            }
            return null;
        }

    },
    LEADER {
        @Override
        public Transitions deliverBlock(Block block, Member from) {
            if (!context().member().equals(from)) {
                log.trace("Rejecting block proposal from {}", from);
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
        public Transitions processCheckpoint(CurrentBlock next) {
            if (!context().processCheckpoint(next)) {
                return PROTOCOL_FAILURE;
            }
            return null;
        }

        @Override
        public Transitions processUser(CurrentBlock next) {
            if (!context().processUser(next)) {
                return PROTOCOL_FAILURE;
            }
            return null;
        }

        @Override
        public Transitions submit(EnqueuedTransaction enqueuedTransaction) {
            context().submit(enqueuedTransaction);
            return null;
        }
    },
    PROTOCOL_FAILURE;

    private static final Logger log = LoggerFactory.getLogger(CollaboratorFsm.class);

}
