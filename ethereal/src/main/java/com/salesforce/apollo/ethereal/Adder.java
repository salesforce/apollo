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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public interface Adder {

    class AdderImpl implements Adder {

        static class MissingPreUnit {
            final List<WaitingPreUnit> neededBy = new ArrayList<>();
            final Instant              requested;

            MissingPreUnit(Instant requested) {
                this.requested = requested;
            }
        }

        class WaitingPreUnit {
            private final List<WaitingPreUnit> children       = new ArrayList<>();
            private volatile boolean           failed         = false;
            private volatile int               missingParents = 0;
            private final PreUnit              pu;
            private volatile int               waitingParents = 0;

            WaitingPreUnit(PreUnit pu) {
                this.pu = pu;
            }

            public String toString() {
                return "wpu[" + pu.shortString() + "]";
            }

            boolean parentsOutput() {
                final int cMissingParents = missingParents;
                final int cWaitingParents = waitingParents;
                return cWaitingParents == 0 && cMissingParents == 0;
            }
        }

        private static final Logger log = LoggerFactory.getLogger(Adder.class);

        private final Config                      conf;
        private final Dag                         dag;
        private final Map<Long, MissingPreUnit>   missing     = new ConcurrentHashMap<>();
        private final Lock                        mtx         = new ReentrantLock(true);
        private final Map<Digest, WaitingPreUnit> waiting     = new ConcurrentHashMap<>();
        private final Map<Long, WaitingPreUnit>   waitingById = new ConcurrentHashMap<>();

        public AdderImpl(Dag dag, Config conf) {
            this.dag = dag;
            this.conf = conf;
        }

        /**
         * Checks basic correctness of a slice of preunits and then adds correct ones to
         * the buffer zone. Returned slice can have the following members: - DataError -
         * if creator or signature are wrong - DuplicateUnit, DuplicatePreunit - if such
         * a unit is already in dag/waiting - UnknownParents - in that case the preunit
         * is normally added and processed, error is returned only for log purpose.
         */
        @Override
        public Map<Digest, Correctness> addPreunits(List<PreUnit> preunits) {
            var errors = new HashMap<Digest, Correctness>();
            var failed = new ArrayList<Boolean>();
            for (int i = 0; i < preunits.size(); i++) {
                failed.add(false);
            }
            mtx.lock();
            try {
                var alreadyInDag = dag.get(preunits.stream().map(e -> e.hash()).toList());

                log.trace("Add preunits: {} already in dag: {} on: {}", preunits, alreadyInDag, conf.pid());

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
                        addToWaiting(preunits.get(i));
                    }
                }
                return errors;
            } finally {
                mtx.unlock();
            }
        }

        @Override
        public void close() {
            log.trace("Closing adder epoch: {} on: {}", dag.epoch(), conf.pid());
        }

        @Override
        public void missing(BloomFilter<Digest> have, List<PreUnit_s> missing) {
            waiting.entrySet()
                   .stream()
                   .filter(e -> !have.contains(e.getKey()))
                   .forEach(e -> missing.add(e.getValue().pu.toPreUnit_s()));
        }

        // addPreunit as a waitingPreunit to the buffer zone.
        private void addToWaiting(PreUnit pu) {
            if (waiting.containsKey(pu.hash())) {
                log.trace("Already waiting unit: {} on: {}", pu, conf.pid());
                return;
            }
            var id = pu.id();
            if (waitingById.get(id) != null) {
                throw new IllegalStateException("Fork in the road"); // fork! TODO
            }
            var wp = new WaitingPreUnit(pu);
            waiting.put(pu.hash(), wp);
            waitingById.put(id, wp);
            checkParents(wp);
            checkIfMissing(wp);
            var current = wp.missingParents;
            if (current > 0) {
                log.trace("missing parents: {} for: {} on: {}", current, wp, conf.pid());
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
                wp.children.clear();
                wp.children.addAll(mp.neededBy);
                for (var ch : wp.children) {
                    final var missingParents = ch.missingParents;
                    ch.missingParents = missingParents - 1;
                    final var waitingParents = ch.waitingParents;
                    ch.waitingParents = waitingParents + 1;
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
                        final var waitingParents = wp.waitingParents;
                        wp.waitingParents = waitingParents + 1;
                        par.children.add(wp);
                    } else {
                        final var missingParents = wp.missingParents;
                        wp.missingParents = missingParents + 1;
                        registerMissing(parentID, wp);
                    }
                }
            }
            return maxHeights;
        }

        private void handleInvalidControlHash(PreUnit witness, Unit[] parents) {
            log.debug("Invalid control hash witness: {} parents: {} on: {}", witness, parents, conf.pid());
            var ids = new ArrayList<Long>();
            short pid = 0;
            for (var height : witness.view().heights()) {
                ids.add(PreUnit.id(height, pid++, witness.epoch()));
            }
        }

        // Ye Jesu Nute
        // The waiting pre unit is ready to be added to ye DAG, may fail if parents
        // don't check out
        private void handleReady(WaitingPreUnit wp) {
            log.debug("Handle ready: {} on: {}", wp, conf.pid());
            mtx.lock();
            try {
                // 1. Decode Parents
                var decoded = dag.decodeParents(wp.pu);
                var parents = decoded.parents();
                if (decoded.inError()) {
                    wp.failed = true;
                    return;
                }
                var digests = Stream.of(parents)
                                    .map(e -> e == null ? (Digest) null : e.hash())
                                    .map(e -> (Digest) e)
                                    .toList();
                Digest calculated = Digest.combine(conf.digestAlgorithm(), digests.toArray(new Digest[digests.size()]));
                if (!calculated.equals(wp.pu.view().controlHash())) {
                    wp.failed = true;
                    handleInvalidControlHash(wp.pu, parents);
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
            final var failed = wp.failed;
            if (failed) {
                removeFailed(wp);
            } else {
                waiting.remove(wp.pu.hash());
                waitingById.remove(wp.pu.id());
                for (var ch : wp.children) {
                    final var waitingParents = ch.waitingParents;
                    ch.waitingParents = waitingParents - 1;
                    sendIfReady(ch);
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

        private void sendIfReady(WaitingPreUnit ch) {
            if (ch.parentsOutput()) {
                log.trace("Sending unit for processing: {} on: {}", ch, conf.pid());
                try {
                    handleReady(ch);
                } catch (Throwable t) {
                    log.error("Unable to handle: {} on: {}", ch.pu, conf.pid(), t);
                }
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
    Map<Digest, Correctness> addPreunits(List<PreUnit> preunits);

    void close();

    void missing(BloomFilter<Digest> have, List<PreUnit_s> missing);

}
