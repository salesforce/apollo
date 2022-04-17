/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import static com.salesforce.apollo.ethereal.PreUnit.id;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ethereal.proto.Commit;
import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesfoce.apollo.ethereal.proto.PreVote;
import com.salesfoce.apollo.ethereal.proto.SignedCommit;
import com.salesfoce.apollo.ethereal.proto.SignedPreVote;
import com.salesfoce.apollo.ethereal.proto.Update.Builder;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.Signer;
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
    public interface ChRbc {

        void commit(SignedCommit commit);

        void prevote(SignedPreVote preVote);

    }

    /**
     * State progression:
     * 
     * PROPOSED -> PREVOTED -> WAITING_FOR_PARENTS -> PARENTS_OUT ->
     *
     */
    public enum State {
        COMMITTED, FAILED, OUTPUT, PARENTS_OUT, PREVOTED, PROPOSED, WAITING_FOR_PARENTS;
    }

    private static final Logger log = LoggerFactory.getLogger(RbcAdder.class);

    public static SignedCommit commit(final Long id, final Digest hash, final short pid, Signer signer) {
        final var commit = Commit.newBuilder().setUnit(id).setSource(pid).setHash(hash.toDigeste()).build();
        signer.sign(commit.toByteString());
        return SignedCommit.newBuilder()
                           .setCommit(commit)
                           .setSignature(signer.sign(commit.toByteString()).toSig())
                           .build();
    }

    public static SignedPreVote prevote(final Long id, final Digest hash, final short pid, Signer signer) {
        final var prevote = PreVote.newBuilder().setUnit(id).setSource(pid).setHash(hash.toDigeste()).build();
        signer.sign(prevote.toByteString());
        return SignedPreVote.newBuilder()
                            .setVote(prevote)
                            .setSignature(signer.sign(prevote.toByteString()).toSig())
                            .build();
    }

    private final Map<Digest, Set<Short>>         commits     = new TreeMap<>();
    private final Config                          conf;
    private final Dag                             dag;
    private final Set<Digest>                     failed      = new TreeSet<>();
    private int                                   maxSize     = 100 * 1024 * 1024;
    private final Map<Long, List<WaitingPreUnit>> missing     = new TreeMap<>();
    private final Map<Digest, Set<Short>>         preVotes    = new TreeMap<>();
    private volatile int                          round       = 0;
    private final int                             threshold;
    private final Map<Digest, WaitingPreUnit>     waiting     = new TreeMap<>();
    private final Map<Long, WaitingPreUnit>       waitingById = new TreeMap<>();

    private final Map<Digest, WaitingPreUnit> waitingForRound = new TreeMap<>();

    public RbcAdder(Dag dag, Config conf, int threshold) {
        this.dag = dag;
        this.conf = conf;
        this.threshold = threshold;
    }

    public void close() {
        log.trace("Closing adder epoch: {} on: {}", dag.epoch(), conf.logLabel());
    }

    /**
     * A commit was received
     * 
     * @param digest - the digest of the unit
     * @param member - the index of the member
     * @param chRbc  - the comm endpoint to issue further messages
     */
    public void commit(Digest digest, short member, ChRbc chRbc) {
        if (failed.contains(digest)) {
            return;
        }
        if (dag.contains(digest)) {
            return; // no need
        }
        final Set<Short> committed = commits.computeIfAbsent(digest, h -> new HashSet<>());
        if (!committed.add(member)) {
            return;
        }
        if (committed.size() <= threshold) {
            return;
        }
        var wpu = waiting.get(digest);

        // Check for existing proposal
        if (wpu == null) {
            return;
        }

        switch (wpu.state()) {
        case PREVOTED:
            if (committed.size() > threshold) {
                commit(wpu, chRbc);
            }
            // intentional fall through
        case COMMITTED:
            if (committed.size() > 2 * threshold) {
                wpu.setState(State.OUTPUT);
                output(wpu, chRbc);
            }
            break;
        default:
            break;
        }
    }

    public Config config() {
        return conf;
    }

    public void have(DigestBloomFilter biff) {
        waiting.keySet().forEach(d -> biff.add(d));
        dag.have(biff);
    }

    /**
     * Update the gossip builder with the missing units filtered by the supplied
     * bloom filter indicating units already known
     */
    public void missing(BloomFilter<Digest> have, Builder builder) {
        var pus = new TreeMap<Digest, PreUnit_s>();
        dag.missing(have, pus);
        waiting.entrySet()
               .stream()
               .filter(e -> !have.contains(e.getKey()))
               .filter(e -> failed.contains(e.getKey()))
               .forEach(e -> pus.putIfAbsent(e.getKey(), e.getValue().serialized()));
        pus.values().forEach(pu -> builder.addMissing(pu));
    }

    /**
     * A preVote was received
     * 
     * @param digest - the digest of the unit
     * @param member - the index of the member
     * @param chRbc  - the comm endpoint to issue further messages
     */
    public void preVote(Digest digest, short member, ChRbc chRbc) {
        if (failed.contains(digest)) {
            return;
        }
        if (dag.contains(digest)) {
            return; // no need
        }
        final Set<Short> prepared = preVotes.computeIfAbsent(digest, h -> new HashSet<>());
        if (!prepared.add(member)) {
            return;
        }
        // We only care if the # of prevotes is >= 2*f + 1
        if (prepared.size() <= 2 * threshold) {
            return;
        }
        // We only care if we've gotten the proposal
        var wpu = waiting.get(digest);
        if (wpu == null) {
            return;
        }

        if (wpu.state() == State.PREVOTED) {
            waitingById.put(wpu.id(), wpu);
            checkParents(wpu);
            checkIfMissing(wpu);
            if (wpu.parentsOutput()) { // optimization
                commit(wpu, chRbc);
            } else {
                wpu.setState(State.WAITING_FOR_PARENTS);
            }
        }
    }

    public void produce(Unit u, ChRbc chRbc) {
        assert u.creator() == config().pid();
        round = u.height();
        dag.insert(u);
        final var wpu = new WaitingPreUnit(u.toPreUnit(), u.toPreUnit_s());
        prevote(wpu, chRbc);
        commit(wpu, chRbc);
        advance(chRbc);
    }

    /**
     * A unit has been proposed.
     * 
     * @param digest - the digest identifying the unit
     * @param u      - the serialized preUnit
     * @param chRbc  - the comm endpoint to issue further messages
     */
    public void propose(Digest digest, PreUnit_s u, ChRbc chRbc) {
        if (failed.contains(digest)) {
            log.trace("Failed preunit: {} on: {}", digest, conf.logLabel());
            return;
        }
        if (waiting.containsKey(digest)) {
            log.trace("Duplicate unit: {} on: {}", digest, conf.logLabel());
            return;
        }
        if (dag.contains(digest)) {
            log.trace("Duplicate unit: {} on: {}", digest, conf.logLabel());
            return;
        }
        if (u.toByteString().size() > maxSize) {
            failed.add(digest);
            log.debug("Invalid size: {} > {} id: {} on: {}", u.toByteString().size(), maxSize, u.getId(),
                      conf.logLabel());
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
        final var wpu = new WaitingPreUnit(preunit, u);
        waiting.put(digest, wpu);
        if (round >= preunit.height()) {
            prevote(wpu, chRbc);
        } else {
            waitingForRound.put(digest, wpu);
        }
    }

    // Advance the state of the RBC by one round
    private void advance(ChRbc chRbc) {
        var iterator = waitingForRound.entrySet().iterator();
        while (iterator.hasNext()) {
            var e = iterator.next();
            if (e.getValue().height() < round) {
                assert e.getValue().state() == State.PROPOSED;
                e.getValue().setState(State.PREVOTED);
                iterator.remove();
                prevote(e.getValue(), chRbc);
            }
        }
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

    private void commit(WaitingPreUnit wpu, ChRbc chRbc) {
        wpu.setState(State.COMMITTED);
        chRbc.commit(commit(wpu.id(), wpu.hash(), config().pid(), config().signer()));
        commit(wpu.hash(), conf.pid(), chRbc);
    }

    private boolean decodeParents(WaitingPreUnit wp) {
        var decoded = dag.decodeParents(wp.pu());
        if (decoded.inError()) {
            removeFailed(wp);
            return false;
        }
        var parents = decoded.parents();
        var digests = Stream.of(parents).map(e -> e == null ? (Digest) null : e.hash()).map(e -> e).toList();
        Digest calculated = Digest.combine(conf.digestAlgorithm(), digests.toArray(new Digest[digests.size()]));
        if (!calculated.equals(wp.pu().view().controlHash())) {
            removeFailed(wp);
            log.debug("Invalid control hash witness: {} parents: {} on: {}", wp, parents, conf.logLabel());
            return false;
        }
        var freeUnit = dag.build(wp.pu(), parents);

        var err = dag.check(freeUnit);
        if (err != null) {
            removeFailed(wp);
            log.warn("Failed: {} check: {} on: {}", freeUnit, err, conf.logLabel());
        }
        wp.setDecoded(freeUnit);
        return true;
    }

    private void output(WaitingPreUnit wpu, ChRbc chRbc) {
        if (!decodeParents(wpu)) {
            removeFailed(wpu);
            return;
        }
        wpu.setState(State.OUTPUT);

        log.trace("Inserting unit: {} on: {}", wpu.decoded(), conf.logLabel());

        dag.insert(wpu.decoded());

        remove(wpu);

        for (var ch : wpu.children()) {
            ch.decWaiting();
            if (wpu.state() == State.WAITING_FOR_PARENTS && wpu.parentsOutput()) {
                if (preVotes.getOrDefault(wpu.hash(), Collections.emptySet()).size() > 2 * threshold + 1) {
                    commit(wpu, chRbc);
                }
            }
        }
    }

    private void prevote(WaitingPreUnit wpu, ChRbc chRbc) {
        wpu.setState(State.PREVOTED);
        chRbc.prevote(prevote(wpu.id(), wpu.hash(), conf.pid(), conf.signer()));
        preVote(wpu.hash(), conf.pid(), chRbc);
    }

    /**
     * registerMissing registers the fact that the given WaitingPreUnit needs an
     * unknown unit with the given id.
     */
    private void registerMissing(long id, WaitingPreUnit wp) {
        missing.computeIfAbsent(id, i -> new ArrayList<>()).add(wp);
        log.trace("missing parent: {} for: {} on: {}", PreUnit.decode(id), wp, conf.logLabel());
    }

    private void remove(WaitingPreUnit wpu) {
        preVotes.remove(wpu.hash());
        commits.remove(wpu.hash());
        waiting.remove(wpu.hash());
        waitingForRound.remove(wpu.hash());
        waitingById.remove(wpu.id());
    }

    /**
     * removeFailed removes from the buffer zone the preunit which we failed to add,
     * together with all its descendants.
     */
    private void removeFailed(WaitingPreUnit wp) {
        wp.setState(State.FAILED);
        failed.add(wp.hash());
        remove(wp);
        for (var ch : wp.children()) {
            removeFailed(ch);
        }
    }
}
