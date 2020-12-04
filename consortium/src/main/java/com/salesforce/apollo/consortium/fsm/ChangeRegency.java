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

import com.chiralbehaviors.tron.Entry;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesforce.apollo.consortium.CollaboratorContext;
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
            CollaboratorContext context = context();
            if (stop.getNextRegent() > context.currentRegent() + 1) {
                log.info("Delaying future Stop: {} > {} from: {} on: {} at: {}", stop.getNextRegent(),
                         context.currentRegent() + 1, from, context.getMember(), this);
                context.delay(stop, from);
            } else {
                log.info("Discarding stale Stop: {} from: {} on: {} at: {}", stop.getNextRegent(), from,
                         context.getMember(), this);
            }
            return null;
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            CollaboratorContext context = context();
            if (stopData.getCurrentRegent() > context.nextRegent()) {
                log.info("Delaying future StopData: {} > {} from: {} on: {} at: {}", stopData.getCurrentRegent(),
                         context.nextRegent(), from, context.getMember(), this);
                context.delay(stopData, from);
                return null;
            } else {
                log.info("Discarding stale StopData: {} at: {} from: {} on: {} at: {}", stopData.getCurrentRegent(),
                         this, from, context.getMember(), this);
                return null;
            }
        }

        @Override
        public Transitions deliverSync(Sync sync, Member from) {
            CollaboratorContext context = context();
            if (context().nextRegent() == sync.getCurrentRegent()) {
                context().deliverSync(sync, from);
            } else if (sync.getCurrentRegent() > context().nextRegent()) {
                log.info("Delaying future Sync: {} > {} from: {} on: {} at: {}", sync.getCurrentRegent(),
                         context.nextRegent(), from, context.getMember(), this);
                context.delay(sync, from);
            } else {
                log.info("Discarding stale Sync: {} from: {} on: {} at: {}", sync.getCurrentRegent(), from,
                         context.getMember(), this);
            }
            return null;
        }

        @Entry
        public void regentElected() {
            context().establishNextRegent();
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
            CollaboratorContext context = context();
            if (stop.getNextRegent() > context.currentRegent() + 1) {
                log.info("Delaying future Stop: {} > {} from: {} on: {} at: {}", stop.getNextRegent(),
                         context.currentRegent() + 1, from, context.getMember(), this);
                context.delay(stop, from);
            } else if (stop.getNextRegent() == context.currentRegent() + 1) {
                context.deliverStop(stop, from);
            } else {
                log.info("Discarding stale Stop: {} from: {} on: {} at {}", stop.getNextRegent(), from,
                         context.getMember(), this);
            }
            return null;
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            CollaboratorContext context = context();
            if (stopData.getCurrentRegent() > context.nextRegent()) {
                log.info("Delaying future StopData: {} > {} from: {} on: {} at: {}", stopData.getCurrentRegent(),
                         context.nextRegent(), from, context.getMember(), this);
                context.delay(stopData, from);
                return null;
            } else if (stopData.getCurrentRegent() < context.nextRegent()) {
                log.info("Discarding stale StopData: {} from: {} on: {} at: {}", stopData.getCurrentRegent(), from,
                         context.getMember(), this);
                return null;
            }
            if (context.isRegent(stopData.getCurrentRegent())) {
                log.info("Preemptively becoming leader, StopData: {} from: {} on: {} at: {}",
                         stopData.getCurrentRegent(), from, context.getMember(), this);
                fsm().push(SYNCHRONIZING_LEADER).deliverStopData(stopData, from);
            }
            return null;
        }

        @Override
        public Transitions deliverSync(Sync sync, Member from) {
            CollaboratorContext context = context();
            if (context.nextRegent() == sync.getCurrentRegent()) {
                fsm().push(AWAIT_SYNCHRONIZATION).deliverSync(sync, from);
            } else if (sync.getCurrentRegent() > context().nextRegent()) {
                log.info("Delaying future Sync: {} > {} from: {} on: {} at: {}", sync.getCurrentRegent(),
                         context.nextRegent(), from, context.getMember(), this);
                context.delay(sync, from);
            } else {
                log.info("Discarding stale Sync: {} nextRegent: {} from: {} on: {} at: {}", sync.getCurrentRegent(),
                         context.nextRegent(), from, context.getMember(), this);
            }
            return null;
        }

        @Override
        public Transitions establishNextRegent() {
            return AWAIT_SYNCHRONIZATION;
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
    SYNCHRONIZED, SYNCHRONIZING_LEADER {

        @Override
        public Transitions deliverStop(Stop stop, Member from) {
            CollaboratorContext context = context();
            if (stop.getNextRegent() > context.currentRegent() + 1) {
                log.info("Delaying future Stop: {} > {} from: {} on: {} at: {}", stop.getNextRegent(),
                         context.currentRegent() + 1, from, context.getMember(), this);
                context.delay(stop, from);
            } else {
                log.info("Discarding stale Stop: {} from: {} on: {} at: {}", stop.getNextRegent(), from,
                         context.getMember(), this);
            }
            return null;
        }

        @Override
        public Transitions deliverStopData(StopData stopData, Member from) {
            CollaboratorContext context = context();
            if (stopData.getCurrentRegent() > context.nextRegent()) {
                log.info("Delaying future StopData: {} > {} from: {} on: {} at: {}", stopData.getCurrentRegent(),
                         context.nextRegent(), from, context.getMember(), this);
                context.delay(stopData, from);
                return null;
            } else if (stopData.getCurrentRegent() < context.nextRegent()) {
                log.info("Discarding stale StopData: {} < {} from: {} on: {} at: {}", stopData.getCurrentRegent(),
                         context.currentRegent(), from, context.getMember(), this);
                return null;
            }
            context.deliverStopData(stopData, from);
            return null;
        }

        @Override
        public Transitions deliverSync(Sync sync, Member from) {
            CollaboratorContext context = context();
            if (context().nextRegent() == sync.getCurrentRegent()) {
                if (context.isRegent(sync.getCurrentRegent()) && context.getMember().equals(from)) {
                    context().deliverSync(sync, from);
                } else {
                    log.info("Discarding invalid Sync: {} from: {} on: {} at: {}", sync.getCurrentRegent(), from,
                             context.getMember(), this);
                }
            } else if (sync.getCurrentRegent() > context().nextRegent()) {
                log.info("Delaying future Sync: {} > {} from: {} on: {} at: {}", sync.getCurrentRegent(),
                         context.nextRegent(), from, context.getMember(), this);
                context.delay(sync, from);
            } else {
                log.info("Discarding stale Sync: {} from: {} on: {} at: {}", sync.getCurrentRegent(), from,
                         context.getMember());
            }
            return null;
        }

        @Override
        public Transitions syncd() {
            return SYNCHRONIZED;
        }

        @Override
        public Transitions synchronize(int elected, Map<Member, StopData> regencyData) {
            try {
                context().synchronize(elected, regencyData);
            } catch (Throwable t) {
                t.printStackTrace();
            }
            return null;
        }

        @Override
        public Transitions synchronizingLeader() {
            return null;
        }
    };

    private static final Logger log = LoggerFactory.getLogger(ChangeRegency.class);

    @Override
    public Transitions receive(Transaction transacton, Member from) {
        context().receive(transacton);
        return null;
    }
}
