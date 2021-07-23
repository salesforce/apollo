/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.ethereal.PreUnit.id;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Dag.AmbiguousParents;
import com.salesforce.apollo.utils.SimpleChannel;

/**
 * @author hal.hildebrand
 *
 */
public interface Adder {

    class AdderImpl implements Adder {
        private static final Logger log = LoggerFactory.getLogger(Adder.class);

        private final Config                          conf;
        private final Dag                             dag;
        private final Map<Long, missingPreUnit>       missing     = new ConcurrentHashMap<>();
        private final Lock                            mtx         = new ReentrantLock();
        private final Map<Digest, waitingPreUnit>     waiting     = new ConcurrentHashMap<>();
        private final Map<Long, waitingPreUnit>       waitingById = new ConcurrentHashMap<>();
        private final SimpleChannel<waitingPreUnit>[] ready;

        @SuppressWarnings("unchecked")
        public AdderImpl(Dag dag, Config conf) {
            this.dag = dag;
            this.conf = conf;
            ready = new SimpleChannel[conf.nProc()];
            for (int i = 0; i < conf.nProc(); i++) {
                SimpleChannel<waitingPreUnit> channel = new SimpleChannel<>(conf.epochLength());
                channel.consume(wpus -> wpus.forEach(wpu -> handleReady(wpu)));
                ready[i] = channel;
            }
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
            var errors = new HashMap<Digest, Correctness>();
            var failed = new ArrayList<Boolean>();
            for (int i = 0; i < preunits.size(); i++) {
                failed.add(false);
            }
            var alreadyInDag = dag.get(preunits.stream().map(e -> e.hash()).toList());

            for (int i = 0; i < preunits.size(); i++) {
                var pu = preunits.get(i);
                if (alreadyInDag.get(i) != null) {
                    Correctness check = checkCorrectness(pu);
                    if (check != Correctness.CORRECT) {
                        log.debug("Failed correctness check: {} : {}", pu, check);
                        errors.put(pu.hash(), check);
                        failed.set(i, true);
                    } else {
                        log.trace("Duplicate unit: {} on: {}", pu, conf.pid());
                        errors.put(pu.hash(), Correctness.DUPLICATE_UNIT);
                        failed.set(i, true);
                    }
                }
            }
            mtx.lock();
            try {
                for (int i = 0; i < preunits.size(); i++) {
                    if (!failed.get(i)) {
                        addToWaiting(preunits.get(i), source);
                    }
                }
            } finally {
                mtx.unlock();
            }
            return errors;
        }

        @Override
        public void close() {
            for (var channel : ready) {
                channel.close();
            }
        }

        private void handleReady(waitingPreUnit wp) {
            if (wp.pu.creator() == conf.pid()) {
                log.trace("Ignore ready: {} on creator: {}", wp, conf.pid());
                return;
            }
            try {
                // 1. Decode Parents
                var decoded = dag.decodeParents(wp.pu());
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
                }
                var digests = Stream.of(parents).map(e -> e == null ? (Digest) null : e.hash()).map(e -> (Digest) e)
                                    .toList();
                if (!Digest.combine(conf.digestAlgorithm(), digests.toArray(new Digest[digests.size()]))
                           .equals(wp.pu().view().controlHash())) {
                    wp.failed().set(true);
                    handleInvalidControlHash(wp.source(), wp.pu(), parents);
                    return;
                }

                // 2. Build Unit
                var freeUnit = dag.build(wp.pu(), parents);

                // 3. Check
                dag.check(freeUnit);

                // 4. Insert
                dag.insert(freeUnit);
                log.debug("Inserted: {} on: {}", freeUnit, conf.pid());
            } finally {
                remove(wp);
            }
        }

        // addPreunit as a waitingPreunit to the buffer zone.
        private void addToWaiting(PreUnit pu, short source) {
            if (waiting.containsKey(pu.hash())) {
                log.trace("Already waiting unit: {} ", pu);
                return;
            }
            var id = pu.id();
            if (waitingById.get(id) != null) {
                throw new IllegalStateException("Fork in the road"); // fork! TODO
            }
            var wp = new waitingPreUnit(pu, id, source, new AtomicInteger(), new AtomicInteger(), new ArrayList<>(),
                                        new AtomicBoolean());
            waiting.put(pu.hash(), wp);
            waitingById.put(id, wp);
            checkParents(wp);
            checkIfMissing(wp);
            if (wp.missingParents().get() > 0) {
                return;
            }
            log.trace("Unit now waiting: {} ", pu);
            sendIfReady(wp);
        }

        private Correctness checkCorrectness(PreUnit pu) {
            if ((pu.creator() >= dag.nProc()) || (pu.epoch() != dag.epoch())) {
                return Correctness.DATA_ERROR;
            }
            // TODO verify signature
            return Correctness.CORRECT;
        }

        /**
         * checkIfMissing sets the children() attribute of a newly created
         * waitingPreunit, depending on if it was missing
         */
        private void checkIfMissing(waitingPreUnit wp) {
            var mp = missing.get(wp.id());
            if (mp != null) {
                wp.children().clear();
                wp.children().addAll(mp.neededBy());
                for (var ch : wp.children()) {
                    ch.missingParents().incrementAndGet();
                    ch.waitingParents().incrementAndGet();
                }
                missing.remove(wp.id());
            } else {
                wp.children().clear();
            }
        }

        /**
         * finds out which parents of a newly created waitingPreUnit are in the dag,
         * which are waiting, and which are missing. Sets values of waitingParents() and
         * missingParents() accordingly. Additionally, returns maximal heights of dag.
         */
        private int[] checkParents(waitingPreUnit wp) {
            var epoch = wp.pu().epoch();
            var maxHeights = dag.maxView().heights();
            short creator = 0;
            for (int height : wp.pu().view().heights()) {
                if (height > maxHeights[creator++]) {
                    long parentID = id(height, creator, epoch);
                    var par = waitingById.get(parentID);
                    if (par != null) {
                        wp.waitingParents().incrementAndGet();
                        par.children().add(wp);
                    } else {
                        wp.missingParents().incrementAndGet();
                        registerMissing(parentID, wp);
                    }
                }
            }
            return maxHeights;
        }

        private void handleInvalidControlHash(long sourcePID, PreUnit witness, Unit[] parents) {
            log.warn("Invalid control hash from: {} winess: {} parents: {} on: {}", sourcePID, witness, parents,
                     conf.pid());
            var ids = new ArrayList<Long>();
            short pid = 0;
            for (var height : witness.view().heights()) {
                ids.add(PreUnit.id(height, pid++, witness.epoch()));
            }
        }

        /**
         * registerMissing registers the fact that the given waitingPreUnit needs an
         * unknown unit with the given id.
         */
        private void registerMissing(long id, waitingPreUnit wp) {
            missing.putIfAbsent(id, new missingPreUnit(new ArrayList<>(), conf.clock().instant()));
            missing.get(id).neededBy().add(wp);
        }

        /** remove waitingPreunit from the buffer zone and notify its children. */
        private void remove(waitingPreUnit wp) {
            mtx.lock();
            try {
                if (wp.failed().get()) {
                    removeFailed(wp);
                } else {
                    waiting.remove(wp.pu().hash());
                    waitingById.remove(wp.id());
                    for (var ch : wp.children()) {
                        ch.waitingParents().decrementAndGet();
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
        private void removeFailed(waitingPreUnit wp) {
            waiting.remove(wp.pu().hash());
            waitingById.remove(wp.id());
            for (var ch : wp.children()) {
                removeFailed(ch);
            }
        }

        /**
         * sendIfReady checks if a waitingPreunit is ready (has no waiting or missing
         * parents). If yes, the preunit is sent to the channel corresponding to its
         * dedicated worker.
         */
        private void sendIfReady(waitingPreUnit wp) {
            if (wp.waitingParents().get() == 0 && wp.missingParents().get() == 0) {
                log.trace("Sending unit for processing: {} ", wp);
                ready[wp.pu().creator()].submit(wp);
            }
        }

    }

    enum Correctness {
        ABIGUOUS_PARENTS, CORRECT, DATA_ERROR, DUPLICATE_PRE_UNIT, DUPLICATE_UNIT, UNKNOWN_PARENTS;
    }

    record waitingPreUnit(PreUnit pu, long id, long source, AtomicInteger missingParents, AtomicInteger waitingParents,
                          List<waitingPreUnit> children, AtomicBoolean failed) {
        public String toString() {
            return "wpu[" + pu.shortString() + "]";
        }
    }

    record missingPreUnit(List<waitingPreUnit> neededBy, java.time.Instant requested) {}

    /**
     * Checks basic correctness of a slice of preunits and then adds correct ones to
     * the buffer zone. Returned slice can have the following members: - DataError -
     * if creator or signature are wrong - DuplicateUnit, DuplicatePreunit - if such
     * a unit is already in dag/waiting - UnknownParents - in that case the preunit
     * is normally added and processed, error is returned only for log purpose.
     */
    Map<Digest, Correctness> addPreunits(short source, List<PreUnit> preunits);

    void close();

}
