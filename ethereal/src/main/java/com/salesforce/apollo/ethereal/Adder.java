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
import java.util.function.Supplier;
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
                if (commits.add(pid)) {
                    log.trace("Commit: {} for: {}:{} height: {} from: {} on: {}", commits.size(), pu, pu.hash(),
                              pu.height(), pid, conf.pid());
                    if (commits.size() > 2 * conf.byzantine()) {
                        sendIfReady();
                    } else {
                        maybeScheduleCommit();
                    }
                }
            }

            void complete() {
                timers.values().forEach(t -> t.cancel());
                timers.clear();
            }

            void maybeScheduleCommit() {
                if (shouldCommit()) {
                    scheduleCommit();
                }
            }

            void maybeSchedulePrevote() {
                if (shouldPrevote()) {
                    schedulePrevote();
                }
            }

            void prevote(short pid) {
                if (prevotes.add(pid)) {
                    log.trace("Prevote: {} for: {}:{} height: {} from: {} on: {}", prevotes.size(), pu, pu.hash(),
                              pu.height(), pid, conf.pid());
                    maybeSchedulePrevote();
                    if (prevotes.size() > 2 * conf.byzantine()) {
                        scheduleCommit();
                    }
                }
            }

            void scheduleCommit() {
                timers.computeIfAbsent(COMMIT, h -> {
                    commits.add(conf.pid());
                    rbc.accept(ChRbcMessage.newBuilder().setCommit(ref).build());
                    return scheduler.schedule(periodicCommit(), 1);
                });
            }

            void schedulePrevote() {
                timers.computeIfAbsent(PREVOTE, h -> {
                    prevotes.add(conf.pid());
                    rbc.accept(ChRbcMessage.newBuilder().setPrevote(ref).build());
                    return scheduler.schedule(periodicPrevote(), 1);
                });
            }

            abstract void sendIfReady();

            abstract boolean shouldCommit();

            abstract boolean shouldPrevote();

            private Runnable periodicCommit() {
                AtomicReference<Runnable> c = new AtomicReference<>();
                c.set(() -> {
                    log.info("Committing: {} height: {} on: {}", pu.hash(), pu.height(), conf.pid());
                    rbc.accept(ChRbcMessage.newBuilder().setCommit(ref).build());
                    timers.put(COMMIT, scheduler.schedule(c.get(), 1));
                });
                return c.get();
            }

            private Runnable periodicPrevote() {
                AtomicReference<Runnable> c = new AtomicReference<>();
                c.set(() -> {
                    log.info("Prevoting: {} height: {} on: {}", pu.hash(), pu.height(), conf.pid());
                    rbc.accept(ChRbcMessage.newBuilder().setPrevote(ref).build());
                    timers.put(PREVOTE, scheduler.schedule(c.get(), 1));
                });
                return c.get();
            }
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
            void commit(short pid) {
                if (commits.add(pid)) {
                    if (commits.size() > 2 * conf.byzantine()) {
                        Timer t = timers.remove(COMMIT);
                        if (t != null) {
                            t.cancel();
                        }
                        t = timers.remove(PROPOSE);
                        if (t != null) {
                            t.cancel();
                        }
                    }

                }
            }

            @Override
            void prevote(short pid) {
                if (prevotes.add(pid)) {
                    if (prevotes.size() > 2 * conf.byzantine()) {
                        Timer t = timers.remove(PREVOTE);
                        if (t != null) {
                            t.cancel();
                        }
                        t = timers.remove(PROPOSE);
                        if (t != null) {
                            t.cancel();
                        }
                    }
                }
            }

            @Override
            void sendIfReady() {
                // Already in DAG
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
                schedulePrevote();
                scheduleCommit();
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

            boolean committed() {
                return commits.size() > 2 * conf.byzantine();
            }

            /**
             * sendIfReady checks if a waitingPreunit is ready (has no waiting or missing
             * parents). If yes, the preunit is inserted into ye DAG.
             */
            void sendIfReady() {
                final boolean po = parentsOutput();
                final boolean co = committed();
                if (po && co) {
                    complete();
                    handleReady(this);
                }
            }

            @Override
            boolean shouldCommit() {
                return commits.size() > conf.byzantine();
            }

            @Override
            boolean shouldPrevote() {
                return pu.height() == 0 || round.get() + 1 >= pu.height();
            }

            void stateFrom(MissingPreUnit mp) {
                commits.addAll(mp.commits);
                prevotes.addAll(mp.prevotes);
            }

            void updated() {
                maybeSchedulePrevote();
                maybeScheduleCommit();
                sendIfReady();
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
        }

        /**
         * Checks basic correctness of a slice of preunits and then adds correct ones to
         * the buffer zone. Returned slice can have the following members: - DataError -
         * if creator or signature are wrong - DuplicateUnit, DuplicatePreunit - if such
         * a unit is already in dag/waiting - UnknownParents - in that case the preunit
         * is normally added and processed, error is returned only for log purpose.
         */
        @Override
        public Map<Digest, Correctness> add(short source, PreUnit pu) {
            return exclusive(() -> {
                var errors = new HashMap<Digest, Correctness>();
                var alreadyInDag = dag.get(pu.hash());
                if (alreadyInDag != null) {
                    log.debug("Add: {} already in dag: {} on: {}", pu, alreadyInDag, conf.pid());
                    return errors;
                }

                if (pu.creator() == conf.pid()) {
                    log.debug("Self created: {} on: {}", pu, conf.pid());
                    return errors;
                }
                if (pu.creator() >= conf.nProc()) {
                    log.debug("Invalid creator: {} > {} on: {}", pu, conf.nProc() - 1, conf.pid());
                    errors.put(pu.hash(), Correctness.DATA_ERROR);
                }
                Correctness check = checkCorrectness(pu);
                if (check != Correctness.CORRECT) {
                    log.debug("Failed correctness check: {} : {} on: {}", pu, check, conf.pid());
                    errors.put(pu.hash(), check);
                }
                if (errors.isEmpty()) {
                    addToWaiting(pu, source);
                }
                return errors;
            });
        }

        @Override
        public void chRbc(short from, ChRbcMessage msg) {
            exclusive(() -> {
                switch (msg.getTCase()) {
                case COMMIT:
                    commit(from, msg.getCommit().getUid(), new Digest(msg.getCommit().getHash()));
                    break;
                case PREVOTE:
                    prevote(from, msg.getPrevote().getUid(), new Digest(msg.getPrevote().getHash()));
                    break;
                default:
                    break;
                }
            });
        }

        @Override
        public void close() {
            log.trace("Closing adder epoch: {} on: {}", dag.epoch(), conf.pid());
        }

        @Override
        public void submit(Unit u) {
            exclusive(() -> {
                log.trace("Submit: {}:{} on: {}", u, u.hash(), conf.pid());
                submitted.put(u.id(), new SubmittedUnit(u));
                rbc.accept(ChRbcMessage.newBuilder().setPropose(u.toPreUnit_s()).build());
                final int r = u.level();
                round.set(r);
                submitted.entrySet().forEach(e -> {
                    if (e.getValue().pu.round(conf) <= r) {
                        submitted.remove(e.getKey());
                        e.getValue().complete();
                    }
                });
                waiting.values().forEach(wpu -> wpu.updated());
            });
        }

        // addPreunit as a waitingPreunit to the buffer zone.
        private void addToWaiting(PreUnit pu, short source) {
            if (waiting.containsKey(pu.hash())) {
                log.trace("Already waiting unit: {}:{} on: {}", pu, pu.hash(), conf.pid());
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
            log.trace("Unit now waiting: {}:{} on: {}", pu, pu.hash(), conf.pid());
            wp.updated();
        }

        private Correctness checkCorrectness(PreUnit pu) {
            if ((pu.creator() >= dag.nProc()) || (pu.epoch() != dag.epoch())) {
                log.error("Invalid unit: {} failed: nProc: {} epoch: {} on: {}", pu, conf.nProc(), dag.epoch(),
                          conf.pid());
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
            var mp = missing.remove(wp.pu.id());
            if (mp != null) {
                wp.stateFrom(mp);
                wp.children.clear();
                wp.children.addAll(mp.neededBy);
                for (var ch : wp.children) {
                    ch.missingParents--;
                    ch.waitingParents++;
                    log.trace("Found parent {} for: {} on: {}", wp, ch, conf.pid());
                }
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
            if (dag.get(digest) != null) {
//                log.trace("Commit for inserted: {}:{} from: {} on: {}", digest, uid, pid, conf.pid());
                return;
            }
            Pending pu = waitingById.get(uid);
            if (pu != null) {
                pu.commit(pid);
                return;
            }
            pu = submitted.get(uid);
            if (pu != null) {
                pu.commit(pid);
                return;
            }
            var mu = missing.computeIfAbsent(uid, id -> new MissingPreUnit(conf.clock().instant()));
            mu.commits.add(pid);
            log.trace("Commit: {} for missing: {}:{} from: {} on: {}", mu.commits.size(), digest, uid, pid, conf.pid());
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
            try {
                log.debug("Handle ready: {} on: {}", wp, conf.pid());
                if (!waiting.containsKey(wp.pu.hash())) {
                    log.trace("Ignoring redundant ready: {} on: {}", wp, conf.pid());
                }
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
                    log.warn("Failed: {}:{} check: {} on: {}", freeUnit, freeUnit.hash(), err, conf.pid());
                } else {
                    // 4. Insert
                    dag.insert(freeUnit);
                    log.debug("Inserted: {}:{} on: {}", freeUnit, freeUnit.hash(), conf.pid());
                }
            } finally {
                remove(wp);
            }
        }

        private void prevote(short pid, long uid, Digest digest) {
            exclusive(() -> {
                if (dag.get(digest) != null) {
                    return;
                }
                Pending pu = waitingById.get(uid);
                if (pu != null) {
                    pu.prevote(pid);
                    return;
                }
                pu = submitted.get(uid);
                if (pu != null) {
                    pu.prevote(pid);
                    return;
                }
                var mu = missing.computeIfAbsent(uid, id -> new MissingPreUnit(conf.clock().instant()));
                mu.prevotes.add(pid);
                log.trace("Prevote: {} for missing: {}:{} from: {} on: {}", mu.prevotes.size(), digest, uid, pid,
                         conf.pid());
            });
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

        private <T> T exclusive(Supplier<T> call) {
            mtx.lock();
            try {
                return call.get();
            } finally {
                mtx.unlock();
            }
        }

        private void exclusive(Runnable f) {
            mtx.lock();
            try {
                f.run();
            } finally {
                mtx.unlock();
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
    Map<Digest, Correctness> add(short source, PreUnit pu);

    /** Handle the CH-RBC protocol msgs PREVOTE and COMMIT **/
    void chRbc(short from, ChRbcMessage msg);

    void close();

    void submit(Unit u);

}
