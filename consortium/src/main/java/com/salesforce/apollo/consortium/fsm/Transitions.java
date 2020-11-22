/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import java.util.List;

import com.chiralbehaviors.tron.FsmExecutor;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.TotalOrdering;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.Consortium.CollaboratorContext;
import com.salesforce.apollo.consortium.CurrentBlock;
import com.salesforce.apollo.consortium.EnqueuedTransaction;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;

/**
 * Transition interface for the Collaborator FSM model
 *
 * @author hal.hildebrand
 *
 */
public interface Transitions extends FsmExecutor<CollaboratorContext, Transitions> {
    default Transitions becomeClient() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions becomeFollower() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions becomeLeader() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverBlock(Block block, Member from) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverPersist(HashKey hash) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverStop(Stop stop, Member from) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverStopData(StopData stopData, Member from) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverSync(Sync syncData, Member from) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverTotalOrdering(TotalOrdering to, Member from) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverTransaction(Transaction transaction, Member from) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverTransactions(ReplicateTransactions transactions, Member from) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverValidate(Validate validation) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions drainPending() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions establishNextRegent() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions fail() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions formView() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions generateGenesis() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions genesisAccepted() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions join() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions joinAsMember() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions joined() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions missingGenesis() {
        throw fsm().invalidTransitionOn();
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

    default Transitions receive(Transaction transaction, Member from) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions recovering() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions start() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions startRegencyChange(List<EnqueuedTransaction> transactions) {
        context().changeRegency(transactions);
        return null;
    }

    default Transitions stop() {
        return CollaboratorFsm.INITIAL;
    }

    default Transitions success() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions syncd() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions synchronizingLeader() {
        throw fsm().invalidTransitionOn();
    }
}
