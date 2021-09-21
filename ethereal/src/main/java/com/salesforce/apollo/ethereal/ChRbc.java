/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ethereal.proto.ChRbcMessage;
import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesfoce.apollo.ethereal.proto.UnitReference;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Adder.AdderImpl.WaitingPreUnit;
import com.salesforce.apollo.utils.Channel;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.RoundScheduler.Timer;

/**
 * @author hal.hildebrand
 *
 */
public class ChRbc {

    private interface ProposedUnit {

        void allParentsAreOutput();

        void commit(short pid);

        void prevote(short pid);

        void scheduleCommit();

        void schedulePrevote();

    }

    private class ProposedUnitImpl implements ProposedUnit {
        final Set<Short>         commits       = new HashSet<>();
        final Digest             hash;
        final int                height;
        final AtomicBoolean      parentsOutput = new AtomicBoolean();
        final Set<Short>         prevotes      = new HashSet<>();
        final UnitReference      ref;
        final Map<String, Timer> timers        = new HashMap<>();

        ProposedUnitImpl(Digest hash, long uid) {
            ref = UnitReference.newBuilder().setHash(hash.toDigeste()).setUid(uid).build();
            height = PreUnit.decode(uid).height();
            this.hash = hash;
        }

        @Override
        public void allParentsAreOutput() {
            parentsOutput.set(true);
        }

        @Override
        public void commit(short pid) {
            commits.add(pid);
        }

        @Override
        public void prevote(short pid) {
            prevotes.add(pid);
            schedulePrevote();
            scheduleCommit();
        }

        @Override
        public void scheduleCommit() {
            if (shouldCommit()) {
                timers.computeIfAbsent(COMMIT, h -> {
                    log.info("Committing: {} height: {} on: {}", hash, height, config().pid());
                    bc.accept(ChRbcMessage.newBuilder().setCommit(ref).build());
                    return scheduler.schedule(periodicCommit(), 1);
                });
            }
        }

        @Override
        public void schedulePrevote() {
            if (shouldPrevote()) {
                timers.computeIfAbsent(PREVOTE, h -> {
                    log.info("Prevoting: {} height: {} on: {}", hash, height, config().pid());
                    bc.accept(ChRbcMessage.newBuilder().setPrevote(ref).build());
                    return scheduler.schedule(periodicPrevote(), 1);
                });
            }
        }

        private Runnable periodicCommit() {
            AtomicReference<Runnable> c = new AtomicReference<>();
            c.set(() -> {
                log.info("Recommitting: {} height: {} on: {}", hash, height, config().pid());
                bc.accept(ChRbcMessage.newBuilder().setCommit(ref).build());
                timers.put(COMMIT, scheduler.schedule(c.get(), 1));
            });
            return c.get();
        }

        private Runnable periodicPrevote() {
            AtomicReference<Runnable> c = new AtomicReference<>();
            c.set(() -> {
                log.info("Re-prevoting: {} height: {} on: {}", hash, height, config().pid());
                bc.accept(ChRbcMessage.newBuilder().setPrevote(ref).build());
                timers.put(PREVOTE, scheduler.schedule(c.get(), 1));
            });
            return c.get();
        }

        private boolean shouldCommit() {
            return parentsOutput.get() && prevotes.size() > minimalQuorum && height <= round.get() + 1;
        }

        private boolean shouldPrevote() {
            return height <= round.get() + 1;
        }
    }

    private class SubmittedUnit extends ProposedUnitImpl {
        final PreUnit_s unit;

        SubmittedUnit(Unit unit) {
            super(unit.hash(), unit.id());
            parentsOutput.set(true);
            commits.add(unit.creator());
            prevotes.add(unit.creator());
            this.unit = unit.toPreUnit_s();
            propose();
        }

        private void propose() {
            timers.computeIfAbsent(PROPOSE, l -> scheduler.schedule(() -> propose(), 1));
            bc.accept(ChRbcMessage.newBuilder().setPropose(unit).build());
        }
    }

    private static final String COMMIT  = "COMMIT";
    private static final Logger log     = LoggerFactory.getLogger(ChRbc.class);
    private static final String PREVOTE = "PREVOTE";

    private static final String             PROPOSE  = "PROPOSE";
    private final Consumer<ChRbcMessage>    bc;
    private final short                     minimalQuorum;
    private final Orderer                   orderer;
    private final Map<Digest, ProposedUnit> proposed = new ConcurrentHashMap<>();
    @SuppressWarnings("unused")
    private final Channel<WaitingPreUnit>   ready;
    private final AtomicInteger             round    = new AtomicInteger();

    private final RoundScheduler scheduler;

    public ChRbc(RoundScheduler scheduler, Consumer<ChRbcMessage> bc, Orderer orderer, Channel<WaitingPreUnit> ready) {
        this.ready = ready;
        this.scheduler = scheduler;
        this.bc = bc;
        this.orderer = orderer;
        minimalQuorum = Dag.minimalQuorum(config().nProc(), config().bias());
    }

    public void parentsOutput(Digest hash) {
        final ProposedUnit propU = proposed.get(hash);
        propU.allParentsAreOutput();
        propU.scheduleCommit();
    }

    public void process(short pid, ChRbcMessage msg) {
        switch (msg.getTCase()) {
        case COMMIT:
            commit(pid, msg.getPrevote().getUid(), new Digest(msg.getCommit().getHash()));
            break;
        case PREVOTE:
            prevote(pid, msg.getPrevote().getUid(), new Digest(msg.getPrevote().getHash()));
            break;
        case PROPOSE:
            orderer.addPreunits(pid,
                                Collections.singletonList(PreUnit.from(msg.getPropose(), config().digestAlgorithm())));
            break;
        case T_NOT_SET:
            break;
        default:
            break;
        }
    }

    public void proposed(WaitingPreUnit wpu) {
        proposed.computeIfAbsent(wpu.pu.hash(), h -> new ProposedUnitImpl(wpu.pu.hash(), wpu.pu.id()))
                .schedulePrevote();
        log.info("Proposed: {} on: {}", wpu, config().pid());
    }

    public void submit(Unit u) {
        assert !proposed.containsKey(u.hash());
        proposed.put(u.hash(), new SubmittedUnit(u));
        log.info("submitted: {} on: {}", u, config().pid());
    }

    private void commit(short pid, long uid, Digest digest) {
        var pu = proposed.computeIfAbsent(digest, h -> new ProposedUnitImpl(digest, uid));
        pu.commit(pid);
        log.info("Commit: {}:{} from: {} on: {}", digest, uid, pid, config().pid());
    }

    private Config config() {
        return orderer.getConfig();
    }

    private void prevote(short pid, long uid, Digest digest) {
        ProposedUnit proposedU = proposed.computeIfAbsent(digest, d -> new ProposedUnitImpl(d, uid));
        proposedU.prevote(pid);
        log.info("Prevote: {}:{} from: {} on: {}", digest, uid, pid, config().pid());
    }
}
