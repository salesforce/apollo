/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.rbc;

import static com.salesforce.apollo.ethereal.PreUnit.id;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ethereal.proto.Commit;
import com.salesfoce.apollo.ethereal.proto.Have;
import com.salesfoce.apollo.ethereal.proto.Missing;
import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesfoce.apollo.ethereal.proto.PreVote;
import com.salesfoce.apollo.ethereal.proto.SignedCommit;
import com.salesfoce.apollo.ethereal.proto.SignedPreVote;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.ethereal.Adder.Correctness;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class ChRbcAdder {

    /**
     * State progression:
     * 
     * PROPOSED -> WAITING_FOR_PARENTS -> PREVOTED -> COMMITTED -> OUTPUT
     * 
     * FAILED can occur at each state transition
     *
     */
    public enum State {
        COMMITTED, FAILED, OUTPUT, PREVOTED, PROPOSED, WAITING_FOR_PARENTS;
    }

    public record Signed<T> (Digest hash, T signed) {}

    private static final Logger log = LoggerFactory.getLogger(ChRbcAdder.class);

    public static Signed<SignedCommit> commit(final Long id, final Digest hash, final short pid, Signer signer,
                                              DigestAlgorithm algo) {
        final var commit = Commit.newBuilder().setUnit(id).setSource(pid).setHash(hash.toDigeste()).build();
        signer.sign(commit.toByteString());
        JohnHancock signature = signer.sign(commit.toByteString());
        return new Signed<>(signature.toDigest(algo),
                            SignedCommit.newBuilder().setCommit(commit).setSignature(signature.toSig()).build());
    }

    public static Signed<SignedPreVote> prevote(final Long id, final Digest hash, final short pid, Signer signer,
                                                DigestAlgorithm algo) {
        final var prevote = PreVote.newBuilder().setUnit(id).setSource(pid).setHash(hash.toDigeste()).build();
        signer.sign(prevote.toByteString());
        JohnHancock signature = signer.sign(prevote.toByteString());
        return new Signed<>(signature.toDigest(algo),
                            SignedPreVote.newBuilder().setVote(prevote).setSignature(signature.toSig()).build());
    }

    private final Map<Digest, Set<Short>>    commits         = new ConcurrentSkipListMap<>();
    private final Config                     conf;
    private final Dag                        dag;
    private final int                        epoch;
    private final Set<Digest>                failed          = new ConcurrentSkipListSet<>();
    private final ReentrantLock              lock            = new ReentrantLock();
    private final int                        maxSize;
    private final Map<Long, List<Waiting>>   missing         = new ConcurrentSkipListMap<>();
    private final Map<Digest, Set<Short>>    prevotes        = new ConcurrentSkipListMap<>();
    private volatile int                     round           = 0;
    private final Map<Digest, SignedCommit>  signedCommits   = new ConcurrentHashMap<>();
    private final Map<Digest, SignedPreVote> signedPrevotes  = new ConcurrentHashMap<>();
    private final int                        threshold;
    private final Map<Digest, Waiting>       waiting         = new ConcurrentSkipListMap<>();
    private final Map<Long, Waiting>         waitingById     = new ConcurrentSkipListMap<>();
    private final Map<Digest, Waiting>       waitingForRound = new ConcurrentSkipListMap<>();

    public ChRbcAdder(int epoch, Dag dag, int maxSize, Config conf, int threshold) {
        this.epoch = epoch;
        this.dag = dag;
        this.conf = conf;
        this.threshold = threshold;
        this.maxSize = maxSize;
    }

    public void close() {
        log.trace("Closing adder epoch: {} on: {}", dag.epoch(), conf.logLabel());
    }

    public String dump() {
        lock.lock();
        try {
            var buff = new StringBuffer();
            buff.append("pid: ").append(conf.pid()).append('\n');
            buff.append('\t').append("round: ").append(round).append('\n');
            buff.append('\t').append("failed: ").append(failed).append('\n');
            buff.append('\t').append("missing: ").append(missing).append('\n');
            buff.append('\t').append("waiting: ").append(waiting).append('\n');
            buff.append('\t').append("waiting for round: ").append(waitingForRound);
            return buff.toString();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Answer the Update from the receiver's state, based on the suppled Gossip
     */
    public Missing gossip(Missing missing) {
        assert missing.getEpoch() == epoch : "Gossip from incorrect epoch: " + missing.getEpoch() + " expected: "
        + epoch + " on: " + conf.logLabel();

        log.trace("Receiving gossip epoch: {} on: {}", epoch, conf.logLabel());
        final var builder = Missing.newBuilder().setHaves(have());
        ChRbcAdder.this.update(missing.getHaves(), builder);
        return builder.build();
    }

    public void produce(Unit u) {
        if (u.epoch() != epoch) {
            throw new IllegalStateException("incorrect epoch: " + u + " only accepting: " + epoch);
        }
        if (dag.contains(u.hash())) {
            log.trace("Produced duplicated unit: {} on: {}", u, conf.logLabel());
            return;
        }
        lock.lock();
        try {
            assert u.creator() == conf.pid();
            log.trace("Producing unit: {} on: {}", u, conf.logLabel());
            round = u.height();
            dag.insert(u);
            final var wpu = new Waiting(u.toPreUnit(), u.toPreUnit_s());
            checkIfMissing(wpu);
            prevote(wpu);
            commit(wpu);
            advance();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Provide the missing state from the receiver state from the supplied update.
     * 
     * @param haves        - the have state of the partner
     * @param currentEpoch - the current epoch of the orderer
     * 
     * @return Missing based on the current state and the haves of the receiver
     */
    public Missing updateFor(Have haves, int currentEpoch) {
        assert haves.getEpoch() == epoch : "Update from incorrect epoch: " + haves.getEpoch() + " expected: " + epoch
        + " on: " + conf.logLabel();
        final var builder = Missing.newBuilder();
        builder.setEpoch(epoch);
        ChRbcAdder.this.update(haves, builder);
        if (epoch == currentEpoch) {
            builder.setHaves(have());
        } else {
            builder.setHaves(Have.newBuilder().setEpoch(epoch).build());
        }
        return builder.build();
    }

    /**
     * Update the commit, prevote and unit state from the supplied update
     */
    public void updateFrom(Missing update) {
        assert update.getEpoch() == epoch : "Update from incorrect epoch: " + update.getEpoch() + " expected: " + epoch
        + " on: " + conf.logLabel();
        update.getPrevotesList().forEach(pv -> {
            final var hash = Digest.from(pv.getVote().getHash());
            if (failed.contains(hash)) {
                return;
            }
            final var signature = JohnHancock.from(pv.getSignature());
            var validated = new AtomicBoolean();
            signedPrevotes.computeIfAbsent(signature.toDigest(conf.digestAlgorithm()), h -> {
                validated.set(validate(pv));
                if (validated.get()) {
                    return pv;
                } else {
                    removeFailed(hash);
                    return null;
                }
            });
            if (validated.get()) {
                preVote(Digest.from(pv.getVote().getHash()), (short) pv.getVote().getSource());
            }
        });
        update.getCommitsList().forEach(c -> {
            final var hash = Digest.from(c.getCommit().getHash());
            if (failed.contains(hash)) {
                return;
            }
            final var signature = JohnHancock.from(c.getSignature());
            final var digest = signature.toDigest(conf.digestAlgorithm());
            var validated = new AtomicBoolean();
            signedCommits.computeIfAbsent(digest, h -> {
                validated.set(validate(c));
                if (validated.get()) {
                    return c;
                } else {
                    removeFailed(hash);
                    return null;
                }
            });
            if (validated.get()) {
                commit(Digest.from(c.getCommit().getHash()), (short) c.getCommit().getSource());
            }
        });
        update.getUnitsList().forEach(u -> {
            final var signature = JohnHancock.from(u.getSignature());
            final var digest = signature.toDigest(conf.digestAlgorithm());
            if (!failed.contains(digest)) {
                propose(digest, u);
            }
        });
    }

    /**
     * exposed for testing
     * 
     * @return the DAG for this adder
     */
    Dag getDag() {
        return dag;
    }

    // Advance the state of the RBC by one round
    private void advance() {
        var iterator = waitingForRound.entrySet().iterator();
        while (iterator.hasNext()) {
            var e = iterator.next();
            if (e.getValue().height() - 1 <= round) {
                assert e.getValue().state() == State.PROPOSED : "expected PROPOSED, found: " + e.getValue() + " on: "
                + conf.logLabel();
                iterator.remove();
                prevote(e.getValue());
            } else {
                log.trace("Waiting for round unit: {} on: {}", e.getValue(), conf.logLabel());
            }
        }
    }

    /**
     * checkIfMissing sets the children() attribute of a newly created
     * waitingPreunit, depending on if it was missing
     */
    private void checkIfMissing(Waiting wp) {
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
    private int[] checkParents(Waiting wp) {
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
                    if (!dag.contains(parentID)) {
                        wp.incMissing();
                        registerMissing(parentID, wp);
                    }
                }
            }
        }
        return maxHeights;
    }

    /**
     * A commit was received
     * 
     * @param digest - the digest of the unit
     * @param member - the index of the member
     */
    private void commit(Digest digest, short member) {
        lock.lock();
        try {
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
                    commit(wpu);
                }
                // intentional fall through
            case COMMITTED:
                if (committed.size() > 2 * threshold) {
                    output(wpu);
                }
                break;
            default:
                break;
            }
        } finally {
            lock.unlock();
        }
    }

    private void commit(Waiting wpu) {
        wpu.setState(State.COMMITTED);
        Signed<SignedCommit> sc = commit(wpu.id(), wpu.hash(), conf.pid(), conf.signer(), conf.digestAlgorithm());
        signedCommits.put(sc.hash(), sc.signed());
        commit(wpu.hash(), conf.pid());
        log.trace("Committing unit: {} on: {}", wpu, conf.logLabel());
    }

    private boolean decodeParents(Waiting wp) {
        var decoded = dag.decodeParents(wp.pu());
        if (decoded.inError()) {
            if (decoded.classification() != Correctness.DUPLICATE_UNIT) {
                removeFailed(wp);
            }
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

    public Have have() {
        return Have.newBuilder()
                   .setEpoch(epoch)
                   .setHaveCommits(haveCommits())
                   .setHavePreVotes(havePreVotes())
                   .setHaveUnits(haveUnits())
                   .build();
    }

    private void have(DigestBloomFilter biff) {
        lock.lock();
        try {
            waiting.keySet().forEach(d -> biff.add(d));
            dag.have(biff, epoch);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Answer the bloom filter with the commits the receiver has
     */
    private Biff haveCommits() {
        final var config = conf;
        var biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(), config.epochLength() * config.nProc() * 2,
                                         config.fpr());
        signedCommits.keySet().forEach(h -> biff.add(h));
        return biff.toBff();
    }

    /**
     * Answer the bloom filter with the prevotes the receiver has
     */
    private Biff havePreVotes() {
        final var config = conf;
        var biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(), config.epochLength() * config.nProc() * 2,
                                         config.fpr());
        signedPrevotes.keySet().forEach(h -> biff.add(h));
        return biff.toBff();
    }

    private Biff haveUnits() {
        var biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(),
                                         conf.epochLength() * conf.numberOfEpochs() * conf.nProc() * 2, conf.fpr());
        have(biff);
        return biff.toBff();
    }

    /**
     * Update the gossip builder with the missing units filtered by the supplied
     * bloom filter indicating units already known
     */
    private void missing(BloomFilter<Digest> have, Missing.Builder builder) {
        lock.lock();
        try {
            var pus = new TreeMap<Digest, PreUnit_s>();
            dag.missing(have, pus);
            waiting.entrySet()
                   .stream()
                   .filter(e -> !have.contains(e.getKey()))
                   .filter(e -> failed.contains(e.getKey()))
                   .forEach(e -> pus.putIfAbsent(e.getKey(), e.getValue().serialized()));
            pus.values().forEach(pu -> builder.addUnits(pu));
        } finally {
            lock.unlock();
        }
    }

    private void output(Waiting wpu) {
        boolean valid = wpu.height() == 0 || wpu.height() - 1 <= round;
        assert valid : wpu + " is not <= " + round;
        if (!decodeParents(wpu)) {
            return;
        }
        wpu.setState(State.OUTPUT);
        remove(wpu);

        log.trace("Inserting unit: {} on: {}", wpu.decoded(), conf.logLabel());

        dag.insert(wpu.decoded());

        for (var ch : wpu.children()) {
            ch.decWaiting();
            if (ch.state() == State.WAITING_FOR_PARENTS && ch.parentsOutput()) {
                if (prevotes.getOrDefault(ch.hash(), Collections.emptySet()).size() > 2 * threshold + 1) {
                    commit(ch);
                }
            }
        }
    }

    private void prevote(Waiting wpu) {
        wpu.setState(State.PREVOTED);
        Signed<SignedPreVote> spv = prevote(wpu.id(), wpu.hash(), conf.pid(), conf.signer(), conf.digestAlgorithm());
        signedPrevotes.put(spv.hash(), spv.signed());
        preVote(wpu.hash(), conf.pid());
        log.trace("Prevoting unit: {} on: {}", wpu, conf.logLabel());
    }

    /**
     * A preVote was received
     * 
     * @param digest - the digest of the unit
     * @param member - the index of the member
     */
    private void preVote(Digest digest, short member) {
        lock.lock();
        try {
            if (failed.contains(digest)) {
                return;
            }
            if (dag.contains(digest)) {
                return; // no need
            }
            final Set<Short> prepared = prevotes.computeIfAbsent(digest, h -> new HashSet<>());
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
                if (wpu.parentsOutput()) {
                    commit(wpu);
                } else {
                    wpu.setState(State.WAITING_FOR_PARENTS);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * A unit has been proposed.
     * 
     * @param digest - the digest identifying the unit
     * @param u      - the serialized preUnit
     * @param chRbc  - the comm endpoint to issue further messages
     */
    private void propose(Digest digest, PreUnit_s u) {
        lock.lock();
        try {
            if (failed.contains(digest)) {
                log.trace("Failed preunit: {} on: {}", digest, conf.logLabel());
                return;
            }
            var wpu = waiting.get(digest);
            if (wpu != null) {
                log.trace("proposed duplicate unit: {} on: {}", wpu, conf.logLabel());
                return;
            }
            final var existing = dag.get(digest);
            if (existing != null) {
//                log.trace("proposed already output unit: {} on: {}", existing, conf.logLabel());
                return;
            }
            if (u.toByteString().size() > maxSize) {
                failed.add(digest);
                log.trace("Invalid size: {} > {} id: {} on: {}", u.toByteString().size(), maxSize, u.getId(),
                          conf.logLabel());
                return;
            }
            final var decoded = PreUnit.decode(u.getId());
            if (decoded.creator() == conf.pid()) {
                return;
            }
            if (decoded.epoch() != epoch) {
                throw new IllegalStateException("incorrect epoch: " + decoded.epoch() + " only accepting: " + epoch);
            }
            var preunit = PreUnit.from(u, conf.digestAlgorithm());

            if (preunit.creator() >= conf.nProc() || preunit.creator() < 0) {
                failed.add(digest);
                log.debug("Invalid creator: {} > {} on: {}", preunit, conf.nProc() - 1, conf.logLabel());
                return;
            }
            if (preunit.epoch() != dag.epoch()) {
                log.error("Invalid epoch: {} expected: {}, but received: {} on: {}", preunit, dag.epoch(),
                          preunit.epoch(), conf.logLabel());
                return;
            }
            wpu = new Waiting(preunit, u);
            waiting.put(digest, wpu);
            if (preunit.height() == 0 || preunit.height() - 1 <= round) {
                prevote(wpu);
            } else {
                waitingForRound.put(digest, wpu);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * registerMissing registers the fact that the given WaitingPreUnit needs an
     * unknown unit with the given id.
     */
    private void registerMissing(long id, Waiting wp) {
        missing.computeIfAbsent(id, i -> new ArrayList<>()).add(wp);
        log.trace("missing parent: {} for: {} on: {}", PreUnit.decode(id), wp, conf.logLabel());
    }

    private void remove(Waiting wpu) {
        prevotes.remove(wpu.hash());
        commits.remove(wpu.hash());
        waiting.remove(wpu.hash());
        waitingForRound.remove(wpu.hash());
        waitingById.remove(wpu.id());
    }

    /**
     * removeFailed removes from the buffer zone the preunit which we failed to add,
     * together with all its descendants.
     */
    private void removeFailed(Digest hash) {
        var wp = waiting.remove(hash);
        if (wp != null) {
            removeFailed(wp);
        } else {
            prevotes.remove(hash);
            commits.remove(hash);
            waiting.remove(hash);
        }
    }

    private void removeFailed(Waiting wp) {
        wp.setState(State.FAILED);
        log.warn("Failed: {} on: {}", wp, conf.logLabel());
        failed.add(wp.hash());
        remove(wp);
        for (var ch : wp.children()) {
            removeFailed(ch);
        }
    }

    /**
     * Provide the missing state from the receiver based on the supplied haves
     */
    private void update(Have have, Missing.Builder builder) {
        final var cbf = BloomFilter.from(have.getHaveCommits());
        signedCommits.entrySet().forEach(e -> {
            if (!cbf.contains(e.getKey())) {
                builder.addCommits(e.getValue());
            }
        });
        final var pbf = BloomFilter.from(have.getHavePreVotes());
        signedPrevotes.entrySet().forEach(e1 -> {
            if (!pbf.contains(e1.getKey())) {
                builder.addPrevotes(e1.getValue());
            }
        });
        final BloomFilter<Digest> pubf = BloomFilter.from(have.getHaveUnits());
        missing(pubf, builder);
    }

    private boolean validate(SignedCommit c) {
        // TODO Auto-generated method stub
        return true;
    }

    private boolean validate(SignedPreVote pv) {
        // TODO Auto-generated method stub
        return true;
    }
}
