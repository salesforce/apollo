/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesforce.apollo.consortium.PendingTransactions.EnqueuedTransaction;

/**
 * @author hal.hildebrand
 *
 */
public enum CollaboratorFsmImpl implements CollaboratorFsm {
    CLIENT, FOLLOWER {

        @Override
        public CollaboratorFsm deliverTransaction(Transaction txn) {
            context().add(txn);
            return null;
        }

        @Override
        public CollaboratorFsm submit(EnqueuedTransaction enqueuedTransaction) {
            context().submit(enqueuedTransaction);
            return null;
        }

    },
    GENESIS_PROCESSED {

        @Override
        public CollaboratorFsm becomeClient() {
            return CLIENT;
        }

        @Override
        public CollaboratorFsm becomeFollower() {
            return FOLLOWER;
        }

        @Override
        public CollaboratorFsm becomeLeader() {
            return LEADER;
        }
    },
    INITIAL {

        public CollaboratorFsm genesisAccepted() {
            return GENESIS_PROCESSED;
        }
    },
    LEADER {

        @Override
        public CollaboratorFsm deliverTransaction(Transaction txn) {
            context().add(txn);
            return null;
        }

        @Override
        public CollaboratorFsm submit(EnqueuedTransaction enqueuedTransaction) {
            context().submit(enqueuedTransaction);
            return null;
        }
    };
}
