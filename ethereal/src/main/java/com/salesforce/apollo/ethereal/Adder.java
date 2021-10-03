/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.ethereal.PreUnit.id;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ethereal.proto.ChRbcMessage;
import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesfoce.apollo.ethereal.proto.UnitReference;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Dag.AmbiguousParents;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.RoundScheduler.Timer;

/**
 * @author hal.hildebrand
 *
 */
public interface Adder {

    class AdderImpl implements Adder {

        static class MissingPreUnit {
            final Set<Short>           commits  = Collections.newSetFromMap(new ConcurrentHashMap<>());
            final List<WaitingPreUnit> neededBy = new ArrayList<>();
            final Set<Short>           prevotes = Collections.newSetFromMap(new ConcurrentHashMap<>());
            final Instant              requested;

            MissingPreUnit(Instant requested) {
                this.requested = requested;
            }
        }

        abstract class Pending {
            final Set<Short>         commits  = Collections.newSetFromMap(new ConcurrentHashMap<>());
            final Set<Short>         prevotes = Collections.newSetFromMap(new ConcurrentHashMap<>());
            final PreUnit            pu;
            final UnitReference      ref;
            final Map<String, Timer> timers   = new HashMap<>();

            Pending(PreUnit pu) {
                this.pu = pu;
                this.ref = UnitReference.newBuilder().setHash(pu.hash().toDigeste()).setUid(pu.id()).build();
            }

            void commit(short pid) {
                commits.add(pid);
            }

            void complete() {
                timers.values().forEach(t -> t.cancel());
                timers.clear();
            }

            Runnable periodicCommit() {
                AtomicReference<Runnable> c = new AtomicReference<>();
                c.set(() -> {
                    log.info("Committing: {} height: {} on: {}", pu.hash(), pu.height(), conf.pid());
                    rbc.accept(ChRbcMessage.newBuilder().setCommit(ref).build());
                    timers.put(COMMIT, scheduler.schedule(c.get(), 1));
                });
                return c.get();
            }

            Runnable periodicPrevote() {
                AtomicReference<Runnable> c = new AtomicReference<>();
                c.set(() -> {
                    log.info("Prevoting: {} height: {} on: {}", pu.hash(), pu.height(), conf.pid());
                    rbc.accept(ChRbcMessage.newBuilder().setPrevote(ref).build());
                    timers.put(PREVOTE, scheduler.schedule(c.get(), 1));
                });
                return c.get();
            }

            void prevote(short pid) {
                prevotes.add(pid);
                schedulePrevote();
                scheduleCommit();
            }

            void scheduleCommit() {
                if (shouldCommit()) {
                    timers.computeIfAbsent(COMMIT, h -> {
                        log.info("Committing: {} height: {} on: {}", pu.hash(), pu.height(), conf.pid());
                        rbc.accept(ChRbcMessage.newBuilder().setCommit(ref).build());
                        return scheduler.schedule(periodicCommit(), 1);
                    });
                }
            }

            void schedulePrevote() {
                if (shouldPrevote()) {
                    timers.computeIfAbsent(PREVOTE, h -> {
                        log.info("Prevoting: {} height: {} on: {}", pu.hash(), pu.height(), conf.pid());
                        rbc.accept(ChRbcMessage.newBuilder().setPrevote(ref).build());
                        return scheduler.schedule(periodicPrevote(), 1);
                    });
                }
            }

            abstract boolean shouldCommit();

            abstract boolean shouldPrevote();
        }

        class SubmittedUnit extends Pending {
            private final Map<String, Timer> timers = new HashMap<>();
            private final PreUnit_s          unit;

            SubmittedUnit(Unit u) {
                super(u);
                unit = u.toPreUnit_s();
                propose();
            }

            public String toString() {
                return "spu[" + pu.shortString() + "]";
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
                log.debug("Proposing: {}:{} on: {}", pu.hash(), pu, conf.pid());
                timers.computeIfAbsent(PROPOSE, l -> scheduler.schedule(() -> propose(), 1));
                rbc.accept(ChRbcMessage.newBuilder().setPropose(unit).build());
            }
        }

        class WaitingPreUnit extends Pending {
            private final List<WaitingPreUnit> children       = new ArrayList<>();
            private volatile boolean           failed         = false;
            private volatile int               missingParents = 0;
            private final long                 source;
            private volatile int               waitingParents = 0;

            WaitingPreUnit(PreUnit pu, long source) {
                super(pu);
                this.source = source;
            }

            public String toString() {
                return "wpu[" + pu.shortString() + "]";
            }

            @Override
            boolean shouldCommit() {
                return parentsOutput() && commits.size() > minimalQuorum && pu.height() + 1 <= round.get();
            }

            @Override
            boolean shouldPrevote() {
                return pu.height() + 1 <= round.get();
            }

            void stateFrom(MissingPreUnit mp) {
                commits.addAll(mp.commits);
                prevotes.addAll(mp.prevotes);
            }

            /**
             * sendIfReady checks if a waitingPreunit is ready (has no waiting or missing
             * parents). If yes, the preunit is inserted into ye DAG.
             */
            void sendIfReady() {
                if (parentsOutput()) {
                    log.trace("Sending unit for processing: {} on: {}", this, conf.pid());
                    handleReady(this);
                }
            }

            private boolean parentsOutput() {
                final int cMissingParents = missingParents;
                final int cWaitingParents = waitingParents;
                return cWaitingParents == 0 && cMissingParents == 0;
            }
        }

        private static final String COMMIT  = "COMMIT";
        private static final Logger log     = LoggerFactory.getLogger(Adder.class);
        private static final String PREVOTE = "PREVOTE";
        private static final String PROPOSE = "PROPOSE";

        private final Config                      conf;
        private final Dag                         dag;
        private final int                         minimalQuorum;
        private final Map<Long, MissingPreUnit>   missing     = new ConcurrentHashMap<>();
        private final Lock                        mtx         = new ReentrantLock();
        private final Consumer<ChRbcMessage>      rbc;
        private final AtomicInteger               round       = new AtomicInteger();
        private final RoundScheduler              scheduler;
        private final Map<Long, SubmittedUnit>    submitted   = new ConcurrentHashMap<>();
        private final Map<Digest, WaitingPreUnit> waiting     = new ConcurrentHashMap<>();
        private final Map<Long, WaitingPreUnit>   waitingById = new ConcurrentHashMap<>();

        public AdderImpl(Dag dag, Config conf, Consumer<ChRbcMessage> rbc, RoundScheduler scheduler) {
            this.dag = dag;
            this.conf = conf;
            this.rbc = rbc;
            this.scheduler = scheduler;
            minimalQuorum = Dag.minimalQuorum(conf.nProc(), conf.bias());
        }

        /**
         * Checks basic correctness of a slice of preunits and then adds correct ones to
         * the buffer zone. Returned slice can have the following members: - DataError -
         * if creator or signature are wrong - DuplicateUnit, DuplicatePreunit - if such
         * a unit is already in dag/waiting - UnknownParents - in that case the preunit
         * is normally added and processed, error is returned only for log purpose.
         */
        @Override
        public Map<Digest, Correctness> addPreunits(short source, List<PreUnit> preunits) {
            mtx.lock();
            try {
                var errors = new HashMap<Digest, Correctness>();
                var failed = new ArrayList<Boolean>();
                for (int i = 0; i < preunits.size(); i++) {
                    failed.add(false);
                }
                var alreadyInDag = dag.get(preunits.stream().map(e -> e.hash()).toList());

                log.debug("Add preunits: {} already in dag: {} on: {}", preunits, alreadyInDag, conf.pid());

                for (int i = 0; i < preunits.size(); i++) {
                    var pu = preunits.get(i);
                    if (pu.creator() == conf.pid()) {
                        log.debug("Self created: {} on: {}", pu, conf.pid());
                        continue;
                    }
                    if (pu.creator() >= conf.nProc()) {
                        log.debug("Invalid creator: {} > {} on: {}", pu, conf.nProc() - 1, conf.pid());
                        errors.put(pu.hash(), Correctness.DATA_ERROR);
                        failed.set(i, true);
                    }
                    if (alreadyInDag.get(i) != null) {
                        Correctness check = checkCorrectness(pu);
                        if (check != Correctness.CORRECT) {
                            log.debug("Failed correctness check: {} : {} on: {}", pu, check, conf.pid());
                            errors.put(pu.hash(), check);
                            failed.set(i, true);
                        } else {
                            log.trace("Duplicate unit: {} on: {}", pu, conf.pid());
                            errors.put(pu.hash(), Correctness.DUPLICATE_UNIT);
                            failed.set(i, true);
                        }
                    }
                }
                for (int i = 0; i < preunits.size(); i++) {
                    if (!failed.get(i)) {
                        addToWaiting(preunits.get(i), source);
                    }
                }
                return errors;
            } finally {
                mtx.unlock();
            }
        }

        @Override
        public void chRbc(short from, ChRbcMessage msg) {
            switch (msg.getTCase()) {
            case COMMIT:
                commit(from, msg.getPrevote().getUid(), new Digest(msg.getCommit().getHash()));
                break;
            case PREVOTE:
                prevote(from, msg.getPrevote().getUid(), new Digest(msg.getPrevote().getHash()));
                break;
            default:
                break;
            }
        }

        @Override
        public void close() {
            log.trace("Closing adder epoch: {} on: {}", dag.epoch(), conf.pid());
        }

        @Override
        public void submit(Unit u) {
            log.trace("Submit: {} on: {}", u, conf.pid());
            submitted.put(u.id(), new SubmittedUnit(u));
            rbc.accept(ChRbcMessage.newBuilder().setPropose(u.toPreUnit_s()).build());
            final int r = u.level();
            round.set(r);
            submitted.entrySet().forEach(e -> {
                if (e.getValue().pu.round(conf) < r) {
                    submitted.remove(e.getKey());
                    e.getValue().complete();
                }
            });
        }

        // addPreunit as a waitingPreunit to the buffer zone.
        private void addToWaiting(PreUnit pu, short source) {
            if (waiting.containsKey(pu.hash())) {
                log.trace("Already waiting unit: {} on: {}", pu, conf.pid());
                return;
            }
            var id = pu.id();
            if (waitingById.get(id) != null) {
                throw new IllegalStateException("Fork in the road"); // fork! TODO
            }
            var wp = new WaitingPreUnit(pu, source);
            waiting.put(pu.hash(), wp);
            waitingById.put(id, wp);
            checkParents(wp);
            checkIfMissing(wp);
            if (wp.missingParents > 0) {
                log.trace("missing parents: {} for: {} on: {}", wp.missingParents, wp, conf.pid());
                return;
            }
            log.trace("Unit now waiting: {} on: {}", pu, conf.pid());
            wp.sendIfReady();
        }

        private Correctness checkCorrectness(PreUnit pu) {
            if ((pu.creator() >= dag.nProc()) || (pu.epoch() != dag.epoch())) {
                return Correctness.DATA_ERROR;
            }
            if (pu.epoch() != dag.epoch()) {
                log.error("Invalid epoch: {} expected: {}, but received: {} on: {}", pu, dag.epoch(), pu.epoch(),
                          conf.pid());
                return Correctness.DATA_ERROR;
            }
            // TODO verify signature
            return Correctness.CORRECT;
        }

        /**
         * checkIfMissing sets the children() attribute of a newly created
         * waitingPreunit, depending on if it was missing
         */
        private void checkIfMissing(WaitingPreUnit wp) {
            log.trace("Checking if missing: {} on: {}", wp, conf.pid());
            var mp = missing.get(wp.pu.id());
            if (mp != null) {
                wp.stateFrom(mp);
                wp.children.clear();
                wp.children.addAll(mp.neededBy);
                for (var ch : wp.children) {
                    ch.missingParents--;
                    ch.waitingParents++;
                    log.trace("Found parent {} for: {} on: {}", wp, ch, conf.pid());
                }
                missing.remove(wp.pu.id());
            } else {
                wp.children.clear();
            }
        }

        /**
         * finds out which parents of a newly created WaitingPreUnit are in the dag,
         * which are waiting, and which are missing. Sets values of waitingParents() and
         * missingParents accordingly. Additionally, returns maximal heights of dag.
         */
        private int[] checkParents(WaitingPreUnit wp) {
            var epoch = wp.pu.epoch();
            var maxHeights = dag.maxView().heights();
            var heights = wp.pu.view().heights();
            for (short creator = 0; creator < heights.length; creator++) {
                var height = heights[creator];
                if (height > maxHeights[creator]) {
                    long parentID = id(height, creator, epoch);
                    var par = waitingById.get(parentID);
                    if (par != null) {
                        wp.waitingParents++;
                        par.children.add(wp);
                    } else {
                        wp.missingParents++;
                        registerMissing(parentID, wp);
                    }
                }
            }
            return maxHeights;
        }

        private void commit(short pid, long uid, Digest digest) {
            Pending pu = waitingById.get(uid);
            if (pu == null) {
                pu = submitted.get(uid);
            }
            if (pu != null) {
                pu.commit(pid);
                log.info("Commit: {}:{} from: {} on: {}", digest, uid, pid, conf.pid());
                return;
            }
            log.info("Commit for missing: {}:{} from: {} on: {}", digest, uid, pid, conf.pid());
            missing.computeIfAbsent(uid, id -> new MissingPreUnit(conf.clock().instant())).commits.add(pid);
        }

        private void handleInvalidControlHash(long sourcePID, PreUnit witness, Unit[] parents) {
            log.debug("Invalid control hash from: {} witness: {} parents: {} on: {}", sourcePID, witness, parents,
                      conf.pid());
            var ids = new ArrayList<Long>();
            short pid = 0;
            for (var height : witness.view().heights()) {
                ids.add(PreUnit.id(height, pid++, witness.epoch()));
            }
        }

        private void handleReady(WaitingPreUnit wp) {
            log.debug("Handle ready: {} on: {}", wp, conf.pid());
            mtx.lock();
            try {
                // 1. Decode Parents
                var decoded = dag.decodeParents(wp.pu);
                var parents = decoded.parents();
                if (decoded.inError()) {
                    if (decoded instanceof AmbiguousParents ap) {
                        parents = new Unit[parents.length];
                        int i = 0;
                        for (var possibleParents : ap.units()) {
                            if (possibleParents.isEmpty()) {
                                break;
                            }
                            if (possibleParents.size() == 1) {
                                parents[i++] = possibleParents.get(0);
                            }
                        }
                    }
                    wp.failed = true;
                    return;
                }
                var digests = Stream.of(parents).map(e -> e == null ? (Digest) null : e.hash()).map(e -> (Digest) e)
                                    .toList();
                Digest calculated = Digest.combine(conf.digestAlgorithm(), digests.toArray(new Digest[digests.size()]));
                if (!calculated.equals(wp.pu.view().controlHash())) {
                    wp.failed = true;
                    handleInvalidControlHash(wp.source, wp.pu, parents);
                    return;
                }

                // 2. Build Unit
                var freeUnit = dag.build(wp.pu, parents);

                // 3. Check
                var err = dag.check(freeUnit);
                if (err != null) {
                    log.warn("Failed: {} check: {} on: {}", freeUnit, err, conf.pid());
                } else {
                    // 4. Insert
                    dag.insert(freeUnit);
                    log.debug("Inserted: {} on: {}", freeUnit, conf.pid());
                }
            } finally {
                remove(wp);
                mtx.unlock();
            }
        }

        private void prevote(short pid, long uid, Digest digest) {
            Pending pu = waitingById.get(uid);
            if (pu == null) {
                pu = submitted.get(uid);
            }
            if (pu != null) {
                pu.prevote(pid);
                log.info("Prevote: {}:{} from: {} on: {}", digest, uid, pid, conf.pid());
                return;
            }
            log.info("Prevote for missing: {}:{} from: {} on: {}", digest, uid, pid, conf.pid());
            missing.computeIfAbsent(uid, id -> new MissingPreUnit(conf.clock().instant())).prevotes.add(pid);
        }

        /**
         * registerMissing registers the fact that the given WaitingPreUnit needs an
         * unknown unit with the given id.
         */
        private void registerMissing(long id, WaitingPreUnit wp) {
            missing.computeIfAbsent(id, i -> new MissingPreUnit(conf.clock().instant())).neededBy.add(wp);
            log.trace("missing parent: {} for: {} on: {}", PreUnit.decode(id), wp, conf.pid());
        }

        /** remove waitingPreunit from the buffer zone and notify its children. */
        private void remove(WaitingPreUnit wp) {
            mtx.lock();
            try {
                if (wp.failed) {
                    removeFailed(wp);
                } else {
                    waiting.remove(wp.pu.hash());
                    waitingById.remove(wp.pu.id());
                    for (var ch : wp.children) {
                        ch.waitingParents--;
                        ch.sendIfReady();
                    }
                }
            } finally {
                mtx.unlock();
            }
        }

        /**
         * removeFailed removes from the buffer zone a ready preunit which we failed to
         * add, together with all its descendants.
         */
        private void removeFailed(WaitingPreUnit wp) {
            waiting.remove(wp.pu.hash());
            waitingById.remove(wp.pu.id());
            for (var ch : wp.children) {
                removeFailed(ch);
            }
        }

    }

    enum Correctness {
        ABIGUOUS_PARENTS, COMPLIANCE_ERROR, CORRECT, DATA_ERROR, DUPLICATE_PRE_UNIT, DUPLICATE_UNIT, UNKNOWN_PARENTS;
    }

    /**
     * Checks basic correctness of a slice of preunits and then adds correct ones to
     * the buffer zone. Returned slice can have the following members: - DataError -
     * if creator or signature are wrong - DuplicateUnit, DuplicatePreunit - if such
     * a unit is already in dag/waiting - UnknownParents - in that case the preunit
     * is normally added and processed, error is returned only for log purpose.
     */
    Map<Digest, Correctness> addPreunits(short source, List<PreUnit> preunits);

    /** Handle the CH-RBC protocol msgs PREVOTE and COMMIT **/
    void chRbc(short from, ChRbcMessage msg);

    void close();

    void submit(Unit u);

}
