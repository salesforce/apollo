/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import com.chiralbehaviors.tron.FsmExecutor;
import com.chiralbehaviors.tron.InvalidTransition;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Proclamation;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesfoce.apollo.consortium.proto.ViewMember;
import com.salesfoce.apollo.proto.ID;
import com.salesforce.apollo.consortium.Consortium.CollaboratorContext;
import com.salesforce.apollo.consortium.CurrentBlock;
import com.salesforce.apollo.consortium.PendingTransactions;
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

    default Transitions deliverProclamation(Proclamation p, Member from) {
        throw new InvalidTransition();
    }

    default Transitions deliverTransaction(Transaction txn) {
        throw new InvalidTransition();
    }

    default Transitions deliverValidate(Validate validation) {
        throw new InvalidTransition();
    }

    default Transitions deliverViewMember(ViewMember parseFrom) {
        throw new InvalidTransition();
    }

    default Transitions fail() {
        throw new InvalidTransition();
    }

    default Transitions generateGenesis() {
        throw new InvalidTransition();
    }

    default Transitions genesisAccepted() {
        throw new InvalidTransition();
    }

    default Transitions join() {
        throw new InvalidTransition();
    }

    default Transitions joined() {
        throw new InvalidTransition();
    }

    default Transitions joinGenesis() {
        throw new InvalidTransition();
    }

    default Transitions missingGenesis() {
        throw new InvalidTransition();
    }

    default Transitions processCheckpoint(CurrentBlock next) {
        context().processCheckpoint(next);
        return null;
    }

    default Transitions processGenesis(CurrentBlock next) {
        context().processGenesis(next);
        return null;
    }

    default Transitions processReconfigure(CurrentBlock next) {
        context().processReconfigure(next);
        return null;
    }

    default Transitions processUser(CurrentBlock next) {
        context().processUser(next);
        return null;
    }

    default Transitions recovering() {
        throw new InvalidTransition();
    }

    default Transitions start() {
        throw new InvalidTransition();
    }

    default Transitions stop() {
        return CollaboratorFsm.INITIAL;
    }

    default Transitions submit(PendingTransactions.EnqueuedTransaction enqueuedTransaction) {
        throw new InvalidTransition();
    }

    default Transitions success() {
        throw new InvalidTransition();
    }
}
