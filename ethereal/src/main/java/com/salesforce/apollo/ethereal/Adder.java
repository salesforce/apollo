/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.ethereal.PreUnit.id;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ethereal.proto.ChRbcMessage;
import com.salesfoce.apollo.ethereal.proto.UnitReference;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Adder.AdderImpl.WaitingPreUnit;
import com.salesforce.apollo.ethereal.Dag.AmbiguousParents;
import com.salesforce.apollo.utils.Channel;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.RoundScheduler.Timer;
import com.salesforce.apollo.utils.SimpleChannel;

/**
 * @author hal.hildebrand
 *
 */
public interface Adder {

    class AdderImpl implements Adder {

        class WaitingPreUnit {
            private static final String COMMIT  = "COMMIT";
            private static final Logger log     = LoggerFactory.getLogger(WaitingPreUnit.class);
            private static final String PREVOTE = "PREVOTE";

            final List<WaitingPreUnit>       children       = new ArrayList<>();
            final Set<Short>                 commits        = Collections.newSetFromMap(new ConcurrentHashMap<>());
            final AtomicBoolean              failed         = new AtomicBoolean();
            final long                       id;
            final AtomicInteger              missingParents = new AtomicInteger();
            final Set<Short>                 prevotes       = Collections.newSetFromMap(new ConcurrentHashMap<>());
            final PreUnit                    pu;
            final long                       source;
            final AtomicInteger              waitingParents = new AtomicInteger();
            private final UnitReference      ref;
            private final Map<String, Timer> timers         = new HashMap<>();

            WaitingPreUnit(PreUnit pu, long id, long source) {
                this.pu = pu;
                this.id = id;
                this.source = source;
                this.ref = UnitReference.newBuilder().setHash(pu.hash().toDigeste()).setUid(id).build();
            }

            public void commit(short pid) {
                commits.add(pid);
            }

            public void prevote(short pid) {
                prevotes.add(pid);
                schedulePrevote();
                scheduleCommit();
            }

            public void schedulePrevote() {
                if (shouldPrevote()) {
                    timers.computeIfAbsent(PREVOTE, h -> {
                        log.info("Prevoting: {} height: {} on: {}", pu.hash(), pu.height(), conf.pid());
                        chRBC.accept(ChRbcMessage.newBuilder().setPrevote(ref).build());
                        return scheduler.schedule(periodicPrevote(), 1);
                    });
                }
            }

            public String toString() {
                return "wpu[" + pu.shortString() + "]";
            }

            private boolean parentsOutput() {
                return waitingParents.get() == 0 && missingParents.get() == 0;
            }

            private Runnable periodicCommit() {
                AtomicReference<Runnable> c = new AtomicReference<>();
                c.set(() -> {
                    log.info("Recommitting: {} height: {} on: {}", pu.hash(), pu.height(), conf.pid());
                    chRBC.accept(ChRbcMessage.newBuilder().setCommit(ref).build());
                    timers.put(COMMIT, scheduler.schedule(c.get(), 1));
                });
                return c.get();
            }

            private Runnable periodicPrevote() {
                AtomicReference<Runnable> c = new AtomicReference<>();
                c.set(() -> {
                    log.info("Re-prevoting: {} height: {} on: {}", pu.hash(), pu.height(), conf.pid());
                    chRBC.accept(ChRbcMessage.newBuilder().setPrevote(ref).build());
                    timers.put(PREVOTE, scheduler.schedule(c.get(), 1));
                });
                return c.get();
            }

            private void scheduleCommit() {
                if (shouldCommit()) {
                    timers.computeIfAbsent(COMMIT, h -> {
                        log.info("Committing: {} height: {} on: {}", pu.hash(), pu.height(), conf.pid());
                        chRBC.accept(ChRbcMessage.newBuilder().setCommit(ref).build());
                        return scheduler.schedule(periodicCommit(), 1);
                    });
                }
            }

            private boolean shouldCommit() {
                return parentsOutput() && commits.size() > minimalQuorum && pu.height() <= round.get() + 1;
            }

            private boolean shouldPrevote() {
                return pu.height() <= round.get() + 1;
            }
        }

        private static final Logger               log         = LoggerFactory.getLogger(Adder.class);
        private final Consumer<ChRbcMessage>      chRBC;
        private final Config                      conf;
        private final Dag                         dag;
        private final int                         minimalQuorum;
        private final Map<Long, missingPreUnit>   missing     = new ConcurrentHashMap<>();
        private final Lock                        mtx         = new ReentrantLock();
        private final Channel<WaitingPreUnit>     ready;
        private final AtomicInteger               round       = new AtomicInteger();
        private final RoundScheduler              scheduler;
        private final Map<Digest, WaitingPreUnit> waiting     = new ConcurrentHashMap<>();
        private final Map<Long, WaitingPreUnit>   waitingById = new ConcurrentHashMap<>();

        public AdderImpl(Dag dag, Config conf, Consumer<ChRbcMessage> chRBC, RoundScheduler scheduler) {
            this.dag = dag;
            this.conf = conf;
            this.chRBC = chRBC;
            this.scheduler = scheduler;

            ready = new SimpleChannel<>(String.format("Ready units for: %s", conf.pid()),
                                        conf.epochLength() * conf.nProc() * 10);
            ready.consume(readyHandler());
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
            ready.close();
        }

        @Override
        public void submit(Unit u) {
            chRBC.accept(ChRbcMessage.newBuilder().setPropose(u.toPreUnit_s()).build()); // TODO CH-RBC
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
            var wp = new WaitingPreUnit(pu, id, source);
            waiting.put(pu.hash(), wp);
            waitingById.put(id, wp);
            checkParents(wp);
            checkIfMissing(wp);
            if (wp.missingParents.get() > 0) {
                log.trace("missing parents: {} for: {} on: {}", wp.missingParents, wp, conf.pid());
                return;
            }
            log.trace("Unit now waiting: {} on: {}", pu, conf.pid());
            sendIfReady(wp);
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
            var mp = missing.get(wp.id);
            if (mp != null) {
                wp.children.clear();
                wp.children.addAll(mp.neededBy);
                for (var ch : wp.children) {
                    ch.missingParents.decrementAndGet();
                    ch.waitingParents.incrementAndGet();
                    log.trace("Found parent {} for: {} on: {}", wp, ch, conf.pid());
                }
                missing.remove(wp.id);
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
                        wp.waitingParents.incrementAndGet();
                        par.children.add(wp);
                    } else {
                        wp.missingParents.incrementAndGet();
                        registerMissing(parentID, wp);
                    }
                }
            }
            return maxHeights;
        }

        private void commit(short pid, long uid, Digest digest) {
            var pu = waitingById.get(uid);
            pu.commit(pid);
            log.info("Commit: {}:{} from: {} on: {}", digest, uid, pid, conf.pid());
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
                    wp.failed.set(true);
                    return;
                }
                var digests = Stream.of(parents).map(e -> e == null ? (Digest) null : e.hash()).map(e -> (Digest) e)
                                    .toList();
                Digest calculated = Digest.combine(conf.digestAlgorithm(), digests.toArray(new Digest[digests.size()]));
                if (!calculated.equals(wp.pu.view().controlHash())) {
                    wp.failed.set(true);
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
            var pu = waitingById.get(uid);
            if (pu != null) {
                pu.prevote(pid);
                log.info("Prevote: {}:{} from: {} on: {}", digest, uid, pid, conf.pid());
            }
        }

        private Consumer<List<WaitingPreUnit>> readyHandler() {
            @SuppressWarnings("unchecked")
            Deque<WaitingPreUnit>[] ready = new ArrayDeque[conf.nProc()];
            for (int i = 0; i < ready.length; i++) {
                ready[i] = new ArrayDeque<>();
            }
            return wpus -> {
                wpus.forEach(wpu -> {
                    ready[wpu.pu.creator()].add(wpu);
                });
                boolean handled = true;
                while (handled) {
                    handled = false;
                    for (int i = 0; i < ready.length; i++) {
                        final Deque<WaitingPreUnit> q = ready[i];
                        if (!q.isEmpty()) {
                            handleReady(q.removeFirst());
                            handled = true;
                        }
                    }
                }
            };
        }

        /**
         * registerMissing registers the fact that the given WaitingPreUnit needs an
         * unknown unit with the given id.
         */
        private void registerMissing(long id, WaitingPreUnit wp) {
            missing.putIfAbsent(id, new missingPreUnit(new ArrayList<>(), conf.clock().instant()));
            missing.get(id).neededBy().add(wp);
            log.trace("missing parent: {} for: {} on: {}", PreUnit.decode(id), wp, conf.pid());
        }

        /** remove waitingPreunit from the buffer zone and notify its children. */
        private void remove(WaitingPreUnit wp) {
            mtx.lock();
            try {
                if (wp.failed.get()) {
                    removeFailed(wp);
                } else {
                    waiting.remove(wp.pu.hash());
                    waitingById.remove(wp.id);
                    for (var ch : wp.children) {
                        ch.waitingParents.decrementAndGet();
                        sendIfReady(ch);
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
            waitingById.remove(wp.id);
            for (var ch : wp.children) {
                removeFailed(ch);
            }
        }

        /**
         * sendIfReady checks if a waitingPreunit is ready (has no waiting or missing
         * parents). If yes, the preunit is sent to the channel corresponding to its
         * dedicated worker.
         */
        private void sendIfReady(WaitingPreUnit wp) {
            if (wp.waitingParents.get() == 0 && wp.missingParents.get() == 0) {
                log.trace("Sending unit for processing: {} on: {}", wp, conf.pid());
                ready.submit(wp);
            }
        }

    }

    enum Correctness {
        ABIGUOUS_PARENTS, COMPLIANCE_ERROR, CORRECT, DATA_ERROR, DUPLICATE_PRE_UNIT, DUPLICATE_UNIT, UNKNOWN_PARENTS;
    }

    record missingPreUnit(List<WaitingPreUnit> neededBy, java.time.Instant requested) {}

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
