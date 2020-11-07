/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import com.chiralbehaviors.tron.FsmExecutor;
import com.chiralbehaviors.tron.InvalidTransition;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesfoce.apollo.proto.ID;
import com.salesforce.apollo.consortium.Consortium.CollaboratorContext;

/**
 * @author hal.hildebrand
 *
 */
public interface CollaboratorFsm extends FsmExecutor<CollaboratorContext, CollaboratorFsm> {
    default CollaboratorFsm becomeClient() {
        throw new InvalidTransition();
    }

    default CollaboratorFsm becomeFollower() {
        throw new InvalidTransition();
    }

    default CollaboratorFsm becomeLeader() {
        throw new InvalidTransition();
    }

    default CollaboratorFsm deliverBlock(Block parseFrom) {
        throw new InvalidTransition();
    }

    default CollaboratorFsm deliverPersist(ID hash) {
        throw new InvalidTransition();
    }

    default CollaboratorFsm deliverTransaction(Transaction txn) {
        throw new InvalidTransition();
    }

    default CollaboratorFsm deliverValidate(Validate validation) {
        throw new InvalidTransition();
    }

    default CollaboratorFsm genesisAccepted() {
        throw new InvalidTransition();
    }
    default CollaboratorFsm submit(PendingTransactions.EnqueuedTransaction enqueuedTransaction) {
        throw new InvalidTransition();
    }
}
