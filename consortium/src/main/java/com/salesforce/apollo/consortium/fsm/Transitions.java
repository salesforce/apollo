/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.fsm;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.FsmExecutor;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.CheckpointProcessing;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.CollaboratorContext;
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
    static Logger log = LoggerFactory.getLogger(Transitions.class);

    default Transitions becomeClient() {
        fsm().pop().becomeClient();
        return null;
    }

    default Transitions becomeFollower() {
        fsm().pop().becomeFollower();
        return null;
    }

    default Transitions becomeLeader() {
        fsm().pop().becomeLeader();
        return null;
    }

    default Transitions continueChangeRegency(List<EnqueuedTransaction> transactions) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverBlock(Block block, Member from) {
        context().deliverBlock(block, from);
        return null;
    }

    default Transitions deliverCheckpointing(CheckpointProcessing checkpointProcessing, Member from) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverPersist(HashKey hash) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions deliverStop(Stop stop, Member from) {
        CollaboratorContext context = context();
        if (stop.getNextRegent() > context.currentRegent() + 1) {
            log.debug("Delaying future Stop: {} > {} from: {} on: {} at: {}", stop.getNextRegent(),
                      context.currentRegent() + 1, from, context.getMember(), this);
            context.delay(stop, from);
        } else if (stop.getNextRegent() == context.currentRegent() + 1) {
            context.deliverStop(stop, from);
        } else {
            log.debug("Discarding stale Stop: {} from: {} on: {} at: {}", stop.getNextRegent(), from,
                      context.getMember(), this);
        }
        return null;
    }

    default Transitions deliverStopData(StopData stopData, Member from) {
        CollaboratorContext context = context();
        if (stopData.getCurrentRegent() > context.nextRegent()) {
            log.debug("Delaying future StopData: {} > {} from: {} on: {} at: {}", stopData.getCurrentRegent(),
                      context.nextRegent(), from, context.getMember(), this);
            context.delay(stopData, from);
            return null;
        } else if (stopData.getCurrentRegent() < context.nextRegent()) {
            log.debug("Discarding stale StopData: {} < {} from: {} on: {} at: {}", stopData.getCurrentRegent(),
                      context.nextRegent(), from, context.getMember(), this);
            return null;
        }
        if (context.isRegent(stopData.getCurrentRegent())) {
            log.debug("Preemptively becoming synchronizing leader, StopData: {} from: {} on: {} at: {}",
                      stopData.getCurrentRegent(), from, context.getMember(), this);
            fsm().push(ChangeRegency.SYNCHRONIZING_LEADER).deliverStopData(stopData, from);
        }
        return null;
    }

    default Transitions deliverSync(Sync sync, Member from) {
        CollaboratorContext context = context();
        if (context().nextRegent() == sync.getCurrentRegent()) {
            if (context.isRegent(sync.getCurrentRegent())) {
                log.debug("Invalid state: {}, discarding invalid Sync: {} from: {} on: {} at: {}", this,
                          sync.getCurrentRegent(), from, context.getMember(), this);
            } else {
                fsm().push(ChangeRegency.AWAIT_SYNCHRONIZATION).deliverSync(sync, from);
            }
        } else if (sync.getCurrentRegent() > context().nextRegent()) {
            log.debug("Delaying future Sync: {} > {} from: {} on: {} at: {}", sync.getCurrentRegent(),
                      context.nextRegent(), from, context.getMember(), this);
            context.delay(sync, from);
        } else {
            log.debug("Discarding stale Sync: {} from: {} on: {} at: {}", sync.getCurrentRegent(), from,
                      context.getMember(), this);
        }
        return null;
    }

    default Transitions deliverValidate(Validate validation) {
        context().deliverValidate(validation);
        return null;
    }

    default Transitions fail() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions formView() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions generateCheckpoint() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions generateView() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions genesisAccepted() {
        return null;
    }

    default Transitions joinAsMember() {
        fsm().pop().joinAsMember();
        return null;
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

    default Transitions receive(ReplicateTransactions txns, Member from) {
        context().receive(txns, from);
        return null;
    }

    default Transitions receive(Transaction transaction, Member from) {
        context().receive(transaction);
        return null;
    }

    default Transitions shutdown() {
        return CollaboratorFsm.INITIAL;
    }

    default Transitions start() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions startRegencyChange(List<EnqueuedTransaction> transactions) {
        fsm().push(ChangeRegency.INITIAL).continueChangeRegency(transactions);
        return null;
    }

    default Transitions stopped() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions syncd() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions synchronize(int elected, Map<Member, StopData> regencyData) {
        throw fsm().invalidTransitionOn();
    }

    default Transitions synchronizedProcess(CertifiedBlock block) {
        context().synchronizedProcess(block);
        return null;
    }

    default Transitions synchronizingLeader() {
        throw fsm().invalidTransitionOn();
    }

    default Transitions checkpointGenerated() {
        throw fsm().invalidTransitionOn();
    }
}
