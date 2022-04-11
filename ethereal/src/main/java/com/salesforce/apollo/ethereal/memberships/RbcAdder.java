/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import static com.salesforce.apollo.ethereal.PreUnit.id;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class RbcAdder {

    private static final Logger log = LoggerFactory.getLogger(RbcAdder.class);

    private final Map<Digest, Set<Short>>         commits     = new HashMap<>();
    private final Config                          conf;
    private final Dag                             dag;
    private final Set<Digest>                     failed      = new TreeSet<>();
    private final Map<Long, List<WaitingPreUnit>> missing     = new HashMap<>();
    private final Map<Digest, Set<Short>>         preVotes    = new HashMap<>();
    private final PriorityQueue<Unit>             ready;
    @SuppressWarnings("unused")
    private volatile int                          round       = 0;
    private final Map<Digest, WaitingPreUnit>     waiting     = new HashMap<>();
    private final Map<Long, WaitingPreUnit>       waitingById = new HashMap<>();

    public RbcAdder(Dag dag, Config conf) {
        this.dag = dag;
        this.conf = conf;
        ready = new PriorityQueue<>(new Comparator<>() {

            @Override
            public int compare(Unit u1, Unit u2) {
                // Topologically sorted
                var comp = Integer.compare(u1.epoch(), u2.epoch());
                if (comp < 0 || comp > 0) {
                    return comp;
                }
                comp = Integer.compare(u1.height(), u2.height());
                if (comp < 0 || comp > 0) {
                    return comp;
                }
                return Short.compare(u1.creator(), u2.creator());

            }
        });
    }

    public void add(Digest digest, PreUnit_s u) {
        if (failed.contains(digest)) {
            log.trace("Failed preunit: {} on: {}", digest, conf.logLabel());
            return;
        }
        if (waiting.containsKey(digest)) {
            log.trace("Duplicate unit: {} on: {}", digest, conf.logLabel());
            return;
        }
        var alreadyInDag = dag.get(digest);
        if (alreadyInDag != null) {
            log.trace("Duplicate unit: {} on: {}", digest, conf.logLabel());
            return;
        }
        var preunit = PreUnit.from(u, conf.digestAlgorithm());

        if (preunit.creator() == conf.pid()) {
            log.debug("Self created: {} on: {}", preunit, conf.logLabel());
            return;
        }
        if (preunit.creator() >= conf.nProc() || preunit.creator() < 0) {
            failed.add(digest);
            log.debug("Invalid creator: {} > {} on: {}", preunit, conf.nProc() - 1, conf.logLabel());
            return;
        }
        if (preunit.epoch() != dag.epoch()) {
            log.error("Invalid epoch: {} expected: {}, but received: {} on: {}", preunit, dag.epoch(), preunit.epoch(),
                      conf.logLabel());
            return;
        }
        addToWaiting(preunit);
    }

    public void close() {
        log.trace("Closing adder epoch: {} on: {}", dag.epoch(), conf.logLabel());
    }

    public void commit(Digest unit, short member) {
        if (failed.contains(unit)) {
            return;
        }
        if (commits.computeIfAbsent(unit, h -> new HashSet<>()).add(member)) {
            // check commits
        }
    }

    public void have(DigestBloomFilter biff) {
        waiting.keySet().forEach(d -> biff.add(d));
        dag.have(biff);
    }

    public Collection<PreUnit_s> missing(BloomFilter<Digest> have) {
        var pus = new TreeMap<Digest, PreUnit_s>();
        dag.missing(have, pus);
        waiting.entrySet()
               .stream()
               .filter(e -> !have.contains(e.getKey()))
               .filter(e -> failed.contains(e.getKey()))
               .forEach(e -> pus.putIfAbsent(e.getKey(), e.getValue().serialized()));
        return pus.values();
    }

    public void preVote(Digest unit, short member) {
        if (failed.contains(unit)) {
            return;
        }
        if (preVotes.computeIfAbsent(unit, h -> new HashSet<>()).add(member)) {
            // check preVotes
        }
    }

    public void produce(Unit u) {
        round = u.height();
        advance();
    }

    // addPreunit as a waitingPreunit to the buffer zone.
    private void addToWaiting(PreUnit pu) {
        if (waiting.containsKey(pu.hash())) {
            log.trace("Already waiting unit: {} on: {}", pu, conf.logLabel());
            return;
        }
        var id = pu.id();
        if (waitingById.get(id) != null) {
            throw new IllegalStateException("Fork in the road on: " + conf.logLabel()); // fork! TODO
        }
        var wp = new WaitingPreUnit(pu);
        waiting.put(pu.hash(), wp);
        waitingById.put(id, wp);
        checkParents(wp);
        checkIfMissing(wp);
        if (wp.missingParents() > 0) {
            log.trace("missing parents: {} for: {} on: {}", wp.missingParents(), wp, conf.logLabel());
            return;
        }
        log.trace("Unit now waiting: {} on: {}", pu, conf.logLabel());
        sendIfReady(wp);
    }

    private void advance() {
        // TODO Auto-generated method stub

    }

    /**
     * checkIfMissing sets the children() attribute of a newly created
     * waitingPreunit, depending on if it was missing
     */
    private void checkIfMissing(WaitingPreUnit wp) {
        log.trace("Checking if missing: {} on: {}", wp, conf.logLabel());
        var neededBy = missing.get(wp.id());
        if (neededBy != null) {
            wp.clearAndAdd(neededBy);
            for (var ch : wp.children()) {
                ch.decMissing();
                ch.incWaiting();
                log.trace("Found parent {} for: {} on: {}", wp, ch, conf.logLabel());
            }
            missing.remove(wp.id());
        } else {
            wp.clearChildren();
        }
    }

    /**
     * finds out which parents of a newly created WaitingPreUnit are in the dag,
     * which are waiting, and which are missing. Sets values of waitingParents() and
     * missingParents accordingly. Additionally, returns maximal heights of dag.
     */
    private int[] checkParents(WaitingPreUnit wp) {
        var epoch = wp.epoch();
        var maxHeights = dag.maxView().heights();
        var heights = wp.pu().view().heights();
        for (short creator = 0; creator < heights.length; creator++) {
            var height = heights[creator];
            if (height > maxHeights[creator]) {
                long parentID = id(height, creator, epoch);
                var par = waitingById.get(parentID);
                if (par != null) {
                    wp.incWaiting();
                    par.addChild(wp);
                } else {
                    wp.incMissing();
                    registerMissing(parentID, wp);
                }
            }
        }
        return maxHeights;
    }

    private void handleInvalidControlHash(PreUnit witness, Unit[] parents) {
        log.debug("Invalid control hash witness: {} parents: {} on: {}", witness, parents, conf.logLabel());
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
        log.debug("Handle ready: {} on: {}", wp, conf.logLabel());
        try {
            // 1. Decode Parents
            var decoded = dag.decodeParents(wp.pu());
            var parents = decoded.parents();
            if (decoded.inError()) {
                wp.fail();
                return;
            }
            var digests = Stream.of(parents).map(e -> e == null ? (Digest) null : e.hash()).map(e -> e).toList();
            Digest calculated = Digest.combine(conf.digestAlgorithm(), digests.toArray(new Digest[digests.size()]));
            if (!calculated.equals(wp.pu().view().controlHash())) {
                wp.fail();
                handleInvalidControlHash(wp.pu(), parents);
                return;
            }

            // 2. Build Unit
            var freeUnit = dag.build(wp.pu(), parents);

            // 3. Check
            var err = dag.check(freeUnit);
            if (err != null) {
                wp.fail();
                log.warn("Failed: {} check: {} on: {}", freeUnit, err, conf.logLabel());
            } else {
                // 4. Enqueue
                ready.add(freeUnit);
                log.debug("Enqueued: {} on: {}", freeUnit, conf.logLabel());
            }
        } finally {
            remove(wp);
        }
    }

    /**
     * registerMissing registers the fact that the given WaitingPreUnit needs an
     * unknown unit with the given id.
     */
    private void registerMissing(long id, WaitingPreUnit wp) {
        missing.computeIfAbsent(id, i -> new ArrayList<>()).add(wp);
        log.trace("missing parent: {} for: {} on: {}", PreUnit.decode(id), wp, conf.logLabel());
    }

    /** remove waitingPreunit from the buffer zone and notify its children. */
    private void remove(WaitingPreUnit wp) {
        if (wp.failed()) {
            removeFailed(wp);
        } else {
            waiting.remove(wp.hash());
            waitingById.remove(wp.id());
            for (var ch : wp.children()) {
                ch.decWaiting();
                sendIfReady(ch);
            }
        }
    }

    /**
     * removeFailed removes from the buffer zone a ready preunit which we failed to
     * add, together with all its descendants.
     */
    private void removeFailed(WaitingPreUnit wp) {
        failed.add(wp.hash());
        waiting.remove(wp.hash());
        waitingById.remove(wp.id());
        for (var ch : wp.children()) {
            removeFailed(ch);
        }
    }

    private void sendIfReady(WaitingPreUnit ch) {
        if (ch.parentsOutput()) {
            log.trace("Sending unit for processing: {} on: {}", ch.pu(), conf.logLabel());
            try {
                handleReady(ch);
            } catch (Throwable t) {
                log.error("Unable to handle: {} on: {}", ch.pu(), conf.logLabel(), t);
            }
        }
    }
}
