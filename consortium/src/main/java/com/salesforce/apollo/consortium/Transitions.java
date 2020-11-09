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
import com.salesforce.apollo.membership.Member;

/**
 * Transition interface for the Collaborator FSM model
 * 
 * @author hal.hildebrand
 *
 */
public interface Transitions extends FsmExecutor<CollaboratorContext, Transitions> {
    default Transitions becomeClient() {
        throw new InvalidTransition();
    }

    default Transitions becomeFollower() {
        throw new InvalidTransition();
    }

    default Transitions becomeLeader() {
        throw new InvalidTransition();
    }

    default Transitions deliverBlock(Block block, Member from) {
        throw new InvalidTransition();
    }

    default Transitions deliverPersist(ID hash) {
        throw new InvalidTransition();
    }

    default Transitions deliverTransaction(Transaction txn) {
        throw new InvalidTransition();
    }

    default Transitions deliverValidate(Validate validation) {
        throw new InvalidTransition();
    }

    default Transitions genesisAccepted() {
        throw new InvalidTransition();
    }

    default Transitions processCheckpoint(CurrentBlock next) {
        throw new InvalidTransition();
    }

    default Transitions processGenesis(CurrentBlock next) {
        throw new InvalidTransition();
    }

    default Transitions processReconfigure(CurrentBlock next) {
        throw new InvalidTransition();
    }

    default Transitions processUser(CurrentBlock next) {
        throw new InvalidTransition();
    }

    default Transitions submit(PendingTransactions.EnqueuedTransaction enqueuedTransaction) {
        throw new InvalidTransition();
    }
}
