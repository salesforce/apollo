/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import static com.salesforce.apollo.membership.aleph.PreUnit.id;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.salesforce.apollo.crypto.Digest;

/**
 * 
 * Adder is a buffer zone where preunits wait to be added to dag. A preunit with
 * missing parents is waiting until all the parents are available. Then it's
 * considered 'ready' and added to per-pid channel, from where it's picked by
 * the worker. Adding a unit consists of:
 * <p>
 * a) DecodeParents b) BuildUnit c) Check d) Insert
 * 
 * @author hal.hildebrand
 *
 */
public class Adder {
    record waitingPreUnit(PreUnit pu, long id, long source, AtomicInteger missingParents, AtomicInteger waitingParents,
                          List<waitingPreUnit> children, AtomicBoolean failed) {}

    record missingPreUnit(List<waitingPreUnit> neededBy, java.time.Instant requested) {}

    private final Clock                       clock;
    private final Dag                         dag;
    private final Map<Long, missingPreUnit>   missing     = new ConcurrentHashMap<>();
    private final Map<Digest, waitingPreUnit> waiting     = new ConcurrentHashMap<>();
    private final Map<Long, waitingPreUnit>   waitingById = new ConcurrentHashMap<>();

    public Adder(Dag dag, Clock clock) {
        this.dag = dag;
        this.clock = clock;
    }

    // addPreunit as a waitingPreunit to the buffer zone.
    public void addToWaiting(PreUnit pu, short source) {
        if (!waiting.containsKey(pu.hash())) {
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
        if (wp.missingParents.get() > 0) {
            return;
        }
    }

    void handleInvalidControlHash(long sourcePID, PreUnit witness, List<Unit> parentCandidates) {
        var ids = new ArrayList<Long>();
        short pid = 0;
        for (var height : witness.view().heights()) {
            ids.add(PreUnit.id(height, pid++, witness.epoch()));
        }
    }

    void receive(waitingPreUnit wp) {
        // 1. Decode Parents
        var decoded = dag.decodeParents(wp.pu);
        var parents = decoded.parents();
        if (!Digest.combine(parents.stream().map(e -> e.hash()).toList()).equals(wp.pu.view().controlHash())) {
            wp.failed.set(true);
            handleInvalidControlHash(wp.source, wp.pu, parents);
            return;
        }

        // 2. Build Unit
        var freeUnit = dag.build(wp.pu, parents);

        // 3. Check
        dag.check(freeUnit);

        // 4. Insert
        dag.insert(freeUnit);
    }

    // checkIfMissing sets the children attribute of a newly created waitingPreunit,
    // depending on if it was missing
    private void checkIfMissing(waitingPreUnit wp) {
        var mp = missing.get(wp.id);
        if (mp != null) {
            wp.children.clear();
            wp.children.addAll(mp.neededBy());
            for (var ch : wp.children) {
                ch.missingParents.incrementAndGet();
                ch.waitingParents.incrementAndGet();
            }
            missing.remove(wp.id);
        } else {
            wp.children.clear();
        }
    }

    // checkParents finds out which parents of a newly created waitingPreUnit are in
    // the dag, which are waiting, and which are missing. Sets values of
    // waitingParents and missingParents accordingly. Additionally, returns maximal
    // heights of dag.
    private List<Integer> checkParents(waitingPreUnit wp) {
        var epoch = wp.pu.epoch();
        var maxHeights = dag.maxView().heights();
        short creator = 0;
        for (int height : wp.pu.view().heights()) {
            if (height > maxHeights.get(creator++)) {
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

    // registerMissing registers the fact that the given waitingPreUnit needs an
    // unknown unit with the given id.
    private void registerMissing(long id, waitingPreUnit wp) {
        missing.putIfAbsent(id, new missingPreUnit(new ArrayList<>(), clock.instant()));
        missing.get(id).neededBy.add(wp);
    }

}
