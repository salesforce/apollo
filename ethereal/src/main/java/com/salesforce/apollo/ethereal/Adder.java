/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.ethereal.PreUnit.id;

import java.time.Clock;
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

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Dag.AmbiguousParents;

/**
 * @author hal.hildebrand
 *
 */
public interface Adder {

    class AdderImpl implements Adder {

        private final Clock                       clock;
        private final Dag                         dag;
        private final DigestAlgorithm             digestAlgorithm;
        private final Map<Long, missingPreUnit>   missing     = new ConcurrentHashMap<>();
        private final Lock                        mtx         = new ReentrantLock();
        private final Map<Digest, waitingPreUnit> waiting     = new ConcurrentHashMap<>();
        private final Map<Long, waitingPreUnit>   waitingById = new ConcurrentHashMap<>();

        public AdderImpl(Dag dag, Clock clock, DigestAlgorithm digestAlgorithm) {
            this.dag = dag;
            this.clock = clock;
            this.digestAlgorithm = digestAlgorithm;
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
                        errors.put(pu.hash(), check);
                        failed.set(i, true);
                    } else {
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
            // TODO Auto-generated method stub

        }

        public void receive(waitingPreUnit wp) {
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
                if (!Digest.combine(digestAlgorithm, (Digest[]) Stream.of(parents).map(e -> e.hash()).toArray())
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
            } finally {
                remove(wp);
            }
        }

        // addPreunit as a waitingPreunit to the buffer zone.
        private void addToWaiting(PreUnit pu, short source) {
            if (waiting.containsKey(pu.hash())) {
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
         * checkParents finds out which parents of a newly created waitingPreUnit are in
         * the dag, which are waiting, and which are missing. Sets values of
         * waitingParents() and missingParents() accordingly. Additionally, returns
         * maximal heights of dag.
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
            missing.putIfAbsent(id, new missingPreUnit(new ArrayList<>(), clock.instant()));
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

        private void sendIfReady(waitingPreUnit ch) {
            // TODO Auto-generated method stub

        }

    }

    enum Correctness {
        ABIGUOUS_PARENTS, CORRECT, DATA_ERROR, DUPLICATE_PRE_UNIT, DUPLICATE_UNIT, UNKNOWN_PARENTS;
    }

    record waitingPreUnit(PreUnit pu, long id, long source, AtomicInteger missingParents, AtomicInteger waitingParents,
                          List<waitingPreUnit> children, AtomicBoolean failed) {}

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
