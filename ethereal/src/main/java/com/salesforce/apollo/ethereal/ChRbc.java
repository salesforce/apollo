/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ethereal.proto.ChRbcMessage;
import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesfoce.apollo.ethereal.proto.UnitReference;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Adder.AdderImpl.WaitingPreUnit;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.RoundScheduler.Timer;

/**
 * @author hal.hildebrand
 *
 */
public class ChRbc {

    private class MissingUnit extends ProposedUnit {
        final Digest               hash;
        final List<WaitingPreUnit> neededBy = new ArrayList<>();
        final long                 uid;

        public MissingUnit(Digest digest, long uid) {
            hash = digest;
            this.uid = uid;
        }

        @Override
        Digest hash() {
            return hash;
        }

        @Override
        int height() {
            return PreUnit.decode(uid).height();
        }

        @Override
        UnitReference ref() {
            return UnitReference.getDefaultInstance();
        }

        @Override
        boolean shouldCommit() {
            return false;
        }

        @Override
        boolean shouldPrevote() {
            return false;
        }
    }

    abstract private class ProposedUnit {
        final Set<Short>         commits  = new HashSet<>();
        final Set<Short>         prevotes = new HashSet<>();
        final Map<String, Timer> timers   = new HashMap<>();

        void commit(short pid) {
            commits.add(pid);
        }

        void complete() {
            timers.values().forEach(t -> t.cancel());
            timers.clear();
        }

        abstract Digest hash();

        abstract int height();

        Runnable periodicCommit() {
            AtomicReference<Runnable> c = new AtomicReference<>();
            c.set(() -> {
                log.info("Recommitting: {} height: {} on: {}", hash(), height(), config.pid());
                bc.accept(ChRbcMessage.newBuilder().setCommit(ref()).build());
                timers.put(COMMIT, scheduler.schedule(c.get(), 1));
            });
            return c.get();
        }

        Runnable periodicPrevote() {
            AtomicReference<Runnable> c = new AtomicReference<>();
            c.set(() -> {
                log.info("Re-prevoting: {} height: {} on: {}", hash(), height(), config.pid());
                bc.accept(ChRbcMessage.newBuilder().setPrevote(ref()).build());
                timers.put(PREVOTE, scheduler.schedule(c.get(), 1));
            });
            return c.get();
        }

        void prevote(short pid) {
            prevotes.add(pid);
            schedulePrevote();
            scheduleCommit();
        }

        abstract UnitReference ref();

        void scheduleCommit() {
            if (shouldCommit()) {
                timers.computeIfAbsent(COMMIT, h -> {
                    log.info("Committing: {} height: {} on: {}", hash(), height(), config.pid());
                    bc.accept(ChRbcMessage.newBuilder().setCommit(ref()).build());
                    return scheduler.schedule(periodicCommit(), 1);
                });
            }
        }

        void schedulePrevote() {
            if (shouldPrevote()) {
                timers.computeIfAbsent(PREVOTE, h -> {
                    log.info("Prevoting: {} height: {} on: {}", hash(), height(), config.pid());
                    bc.accept(ChRbcMessage.newBuilder().setPrevote(ref()).build());
                    return scheduler.schedule(periodicPrevote(), 1);
                });
            }
        }

        abstract boolean shouldCommit();

        abstract boolean shouldPrevote();
    }

    private class SubmittedUnit extends ProposedUnit {
        final PreUnit_s             pus;
        private final UnitReference ref;
        private final Unit          unit;

        SubmittedUnit(Unit unit) {
            this.unit = unit;
            commits.add(unit.creator());
            prevotes.add(unit.creator());
            this.pus = unit.toPreUnit_s();
            ref = UnitReference.newBuilder().setHash(unit.hash().toDigeste()).setUid(unit.id()).build();
            propose();
        }

        @Override
        Digest hash() {
            return unit.hash();
        }

        @Override
        int height() {
            return unit.height();
        }

        @Override
        UnitReference ref() {
            return ref;
        }

        @Override
        boolean shouldCommit() {
            return true;
        }

        @Override
        boolean shouldPrevote() {
            return true;
        }

        private void propose() {
            timers.computeIfAbsent(PROPOSE, l -> scheduler.schedule(() -> propose(), 1));
            bc.accept(ChRbcMessage.newBuilder().setPropose(pus).build());
        }
    }

    private class WaitingUnit extends ProposedUnit {
        private final List<WaitingUnit> children       = new ArrayList<>();
        private boolean                 failed         = false;
        private int                     missingParents = 0;
        private final PreUnit           pu;
        private int                     waitingParents = 0;

        private WaitingUnit(PreUnit pu) {
            this.pu = pu;
        }

        private WaitingUnit(PreUnit pu, MissingUnit mp) {
            this(pu);
            commits.addAll(mp.commits);
            prevotes.addAll(mp.prevotes);
        }

        @Override
        Digest hash() {
            return pu.hash();
        }

        @Override
        int height() {
            return pu.height();
        }

        @Override
        UnitReference ref() {
            // TODO Auto-generated method stub
            return null;
        }

        boolean shouldCommit() {
            return parentsOutput() && commits.size() > minimalQuorum && height() <= round + 1;
        }

        boolean shouldPrevote() {
            return height() <= round + 1;
        }

        private boolean parentsOutput() {
            return waitingParents == 0 && missingParents == 0;
        }
    }

    private static final String COMMIT  = "COMMIT";
    private static final Logger log     = LoggerFactory.getLogger(ChRbc.class);
    private static final String PREVOTE = "PREVOTE";
    private static final String PROPOSE = "PROPOSE";

    private final Consumer<ChRbcMessage>    bc;
    private final Config                    config;
    private volatile int                    epoch;
    private final short                     minimalQuorum;
    private final Map<Digest, ProposedUnit> proposed = new ConcurrentHashMap<>();
    private volatile int                    round;
    private final RoundScheduler            scheduler;

    public ChRbc(Config config, RoundScheduler scheduler, Consumer<ChRbcMessage> bc) {
        this.config = config;
        this.scheduler = scheduler;
        this.bc = bc;
        minimalQuorum = Dag.minimalQuorum(config.nProc(), config.bias());
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
            proposed(pid, msg.getPropose());
            break;
        default:
            log.trace("What you talking about? {}", msg);
            break;
        }
    }

    public void submit(Unit u) {
        assert !proposed.containsKey(u.hash());
        proposed.put(u.hash(), new SubmittedUnit(u)); // purposeful overwrite if exists ;)
        log.info("submitted: {} on: {}", u, config.pid());
    }

    private void commit(short pid, long uid, Digest digest) {
        var pu = proposed.computeIfAbsent(digest, h -> new MissingUnit(digest, uid));
        pu.commit(pid);
        log.info("Commit: {}:{} from: {} on: {}", digest, uid, pid, config.pid());
    }

    private void prevote(short pid, long uid, Digest digest) {
        ProposedUnit proposedU = proposed.computeIfAbsent(digest, d -> new MissingUnit(d, uid));
        proposedU.prevote(pid);
        log.info("Prevote: {}:{} from: {} on: {}", digest, uid, pid, config.pid());
    }

    private void proposed(short pid, PreUnit_s propose) {
        PreUnit pu = PreUnit.from(propose, config.digestAlgorithm());
        if (!validate(pu)) {
            return;
        }
        proposed.compute(pu.hash(), (h, pending) -> {
            if (pending == null) {
                return new WaitingUnit(pu);
            }
            if (pending instanceof MissingUnit u) {
                return new WaitingUnit(pu, u);
            }
            return pending;
        }).schedulePrevote();
        log.info("Proposed: {} on: {}", pu, config.pid());
    }

    private boolean validate(PreUnit pu) {
        if (pu.creator() == config.pid()) {
            log.debug("Self created: {} on: {}", pu, config.pid());
            return false;
        }
        if (pu.creator() >= config.nProc()) {
            log.debug("Invalid creator: {} > {} on: {}", pu, config.nProc() - 1, config.pid());
            return false;
        }

        if ((pu.creator() >= config.nProc())) {
            log.debug("Invalid creator: {} on: {}", pu, epoch, config.pid());
            return false;
        }
        if (pu.epoch() != epoch) {
            log.error("Invalid epoch: {} expected: {}, but received: {} on: {}", pu, epoch, pu.epoch(), config.pid());
            return false;
        }
        return true;
    }
}
