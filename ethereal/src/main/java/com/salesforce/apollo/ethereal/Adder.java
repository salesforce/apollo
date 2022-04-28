/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.ethereal.PreUnit.id;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class Adder {

    /**
     * PROPOSED -> WAITING_ON_ROUND -> PREVOTED -> WAITING_FOR_PARENTS -> COMMITTED
     * -> OUTPUT
     * 
     * FAILED can occur at each state transition
     *
     */
    public enum State {
        COMMITTED, FAILED, OUTPUT, PREVOTED, PROPOSED, WAITING_FOR_PARENTS, WAITING_ON_ROUND;
    }

    public record Signed<T> (Digest hash, T signed) {}

    private static final Logger log = LoggerFactory.getLogger(Adder.class);

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

    private final List<DigestBloomFilter>    cmts            = new ArrayList<>();
    private final Map<Digest, Set<Short>>    commits         = new ConcurrentSkipListMap<>();
    private final Config                     conf;
    private final Dag                        dag;
    private final int                        epoch;
    private final Set<Digest>                failed;
    private final ReentrantLock              lock            = new ReentrantLock();
    private final int                        maxSize;
    private final Map<Long, List<Waiting>>   missing         = new ConcurrentSkipListMap<>();
    private final Map<Digest, Set<Short>>    prevotes        = new ConcurrentSkipListMap<>();
    private final List<DigestBloomFilter>    prevs           = new ArrayList<>();
    private volatile int                     round           = 0;
    private final Map<Digest, SignedCommit>  signedCommits   = new ConcurrentHashMap<>();
    private final Map<Digest, SignedPreVote> signedPrevotes  = new ConcurrentHashMap<>();
    private final int                        threshold;
    private final List<DigestBloomFilter>    unts            = new ArrayList<>();
    private final Map<Digest, Waiting>       waiting         = new ConcurrentSkipListMap<>();
    private final Map<Long, Waiting>         waitingById     = new ConcurrentSkipListMap<>();
    private final Map<Digest, Waiting>       waitingForRound = new ConcurrentSkipListMap<>();

    public Adder(int epoch, Dag dag, int maxSize, Config conf, Set<Digest> failed) {
        this.epoch = epoch;
        this.dag = dag;
        this.conf = conf;
        this.failed = failed;
        this.threshold = Dag.threshold(conf.nProc());
        this.maxSize = maxSize;
        for (int i = 0; i < 3; i++) {
            prevs.add(new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(),
                                            conf.epochLength() * conf.numberOfEpochs() * conf.nProc() * 2, conf.fpr()));
            cmts.add(new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(),
                                           conf.epochLength() * conf.numberOfEpochs() * conf.nProc() * 2, conf.fpr()));
            unts.add(new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(),
                                           conf.epochLength() * conf.numberOfEpochs() * conf.nProc() * 2, conf.fpr()));
        }
    }

    public void close() {
        log.trace("Closing adder epoch: {} on: {}", dag.epoch(), conf.logLabel());
        waiting.clear();
        waitingById.clear();
        waitingForRound.clear();
        signedCommits.clear();
        signedPrevotes.clear();
        prevotes.clear();
        missing.clear();
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

    public Have have() {
        lock.lock();
        try {
            return Have.newBuilder()
                       .setEpoch(epoch)
                       .setHaveCommits(haveCommits())
                       .setHavePreVotes(havePreVotes())
                       .setHaveUnits(haveUnits())
                       .build();
        } finally {
            lock.unlock();
        }
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
            round = u.height();
            log.trace("Producing unit: {} on: {}", u, conf.logLabel());
            final var wpu = new Waiting(u.toPreUnit(), u.toPreUnit_s());
            for (var unt : unts) {
                unt.add(wpu.hash());
            }
            checkIfMissing(wpu);
            checkParents(wpu);
            prevote(wpu);
            commit(wpu);
            output(wpu);
            advance();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Provide the missing state from the receiver state from the supplied update.
     * 
     * @param haves - the have state of the partner
     * 
     * @return Missing based on the current state and the haves of the receiver
     */
    public Missing updateFor(Have haves) {
        assert haves.getEpoch() == epoch : "Have from incorrect epoch: " + haves.getEpoch() + " expected: " + epoch
        + " on: " + conf.logLabel();
        lock.lock();
        try {
            final var builder = Missing.newBuilder();
            builder.setEpoch(epoch);
            Adder.this.update(haves, builder);
            return builder.setHaves(have()).build();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Update the commit, prevote and unit state from the supplied update
     */
    public void updateFrom(Missing update) {
        assert update.getEpoch() == epoch : "Update from incorrect epoch: " + update.getEpoch() + " expected: " + epoch
        + " on: " + conf.logLabel();
        lock.lock();
        try {
            update.getUnitsList().forEach(u -> {
                final var signature = JohnHancock.from(u.getSignature());
                final var digest = signature.toDigest(conf.digestAlgorithm());
                if (!failed.contains(digest)) {
                    log.trace("propose: {} : {} on: {}", digest, PreUnit.decode(u.getId()), conf.logLabel());
                    propose(digest, u);
                }
            });
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
                        for (var pvts : prevs) {
                            pvts.add(h);
                        }
                        return pv;
                    } else {
                        return null;
                    }
                });
                if (validated.get()) {
                    prevote(Digest.from(pv.getVote().getHash()), (short) pv.getVote().getSource());
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
                        for (var cmt : cmts) {
                            cmt.add(h);
                        }
                        return c;
                    } else {
                        return null;
                    }
                });
                if (validated.get()) {
                    commit(Digest.from(c.getCommit().getHash()), (short) c.getCommit().getSource());
                }
            });
        } finally {
            lock.unlock();
        }
    }

    /**
     * A commit was received
     * 
     * @param digest - the digest of the unit
     * @param member - the index of the member
     */
    void commit(Digest digest, short member) {
        if (failed.contains(digest)) {
            return;
        }
        if (dag.contains(digest)) {
            return; // already output
        }
        final Set<Short> committed = commits.computeIfAbsent(digest, h -> new HashSet<>());
        var wpu = waiting.get(digest);

        if (!committed.add(member)) {
            log.trace("Already committed: {} wpu: {} count: {} on: {}", digest, wpu, committed.size(), conf.logLabel());
            return;
        }
        log.trace("Committed: {} wpu: {} count: {} on: {}", digest, wpu, committed.size(), conf.logLabel());

        if (committed.size() <= threshold) {
            return;
        }

        // Check for existing proposal
        if (wpu == null) {
            log.trace("Committed, but no proposal: {} count: {} on: {}", digest, committed.size(), conf.logLabel());
            return;
        }

        switch (wpu.state()) {
        case PREVOTED:
            if (committed.size() > threshold) {
                log.trace("Committing: {} on: {}", wpu, conf.logLabel());
                commit(wpu);
            }
            break;
        case COMMITTED:
            if (committed.size() > 2 * threshold) {
                log.trace("Outputting: {} on: {}", wpu, conf.logLabel());
                output(wpu);
            }
            break;
        default:
            log.trace("No commit action: {} count: {} on: {}", wpu, committed.size(), conf.logLabel());
            break;
        }
    }

    Map<Digest, Set<Short>> getCommits() {
        return commits;
    }

    Dag getDag() {
        return dag;
    }

    Map<Long, List<Waiting>> getMissing() {
        return missing;
    }

    Map<Digest, Set<Short>> getPrevotes() {
        return prevotes;
    }

    Map<Digest, SignedCommit> getSignedCommits() {
        return signedCommits;
    }

    Map<Digest, SignedPreVote> getSignedPrevotes() {
        return signedPrevotes;
    }

    Map<Digest, Waiting> getWaiting() {
        return waiting;
    }

    Map<Long, Waiting> getWaitingById() {
        return waitingById;
    }

    Map<Digest, Waiting> getWaitingForRound() {
        return waitingForRound;
    }

    /**
     * A preVote was received
     * 
     * @param digest - the digest of the unit
     * @param member - the index of the member
     */
    void prevote(Digest digest, short member) {
        if (failed.contains(digest)) {
            return;
        }
        if (dag.contains(digest)) {
            return; // already output
        }
        final Set<Short> prepared = prevotes.computeIfAbsent(digest, h -> new HashSet<>());
        if (!prepared.add(member)) {
            return;
        }
        var wpu = waiting.get(digest);
        log.trace("Prevoted: {} wpu: {} count: {} on: {}", digest, wpu, prepared.size(), conf.logLabel());

        // We only care if the # of prevotes is >= 2*f + 1
        if (prepared.size() <= 2 * threshold) {
            return;
        }
        // We only care if we've gotten the proposal
        if (wpu == null) {
            log.trace("Prevoted, but no proposal: {} count: {} on: {}", digest, prepared.size(), conf.logLabel());
            return;
        }

        switch (wpu.state()) {
        case PREVOTED:
            waitingById.put(wpu.id(), wpu);
            checkParents(wpu);
            checkIfMissing(wpu);
            if (wpu.parentsOutput()) {
                commit(wpu);
            } else {
                wpu.setState(State.WAITING_FOR_PARENTS);
                log.trace("Waiting for parents: {} on: {}", wpu, conf.logLabel());
            }
            break;
        case WAITING_FOR_PARENTS:
            if (wpu.parentsOutput()) {
                commit(wpu);
            }
            break;
        default:
            log.trace("No prevote action: {} prevote count: {} on: {}", wpu, prepared.size(), conf.logLabel());
            break;
        }
    }

    /**
     * A unit has been proposed.
     * 
     * @param digest - the digest identifying the unit
     * @param u      - the serialized preUnit
     */
    void propose(Digest digest, PreUnit_s u) {
        if (failed.contains(digest)) {
            log.trace("Failed preunit: {} on: {}", digest, conf.logLabel());
            return;
        }
        var wpu = waiting.get(digest);
        if (wpu != null) {
            return;
        }
        final var existing = dag.get(digest);
        if (existing != null) {
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
            log.trace("Invalid epoch: {} expected {} unit: {} on: {}", decoded.epoch(), epoch, maxSize, decoded,
                      conf.logLabel());
        }
        var preunit = PreUnit.from(u, conf.digestAlgorithm());

        if (preunit.creator() >= conf.nProc() || preunit.creator() < 0) {
            failed.add(digest);
            log.debug("Invalid creator: {} > {} on: {}", preunit, conf.nProc() - 1, conf.logLabel());
            return;
        }
        for (var unt : unts) {
            unt.add(digest);
        }
        wpu = new Waiting(preunit, u);
        waiting.put(digest, wpu);
        for (var unt : unts) {
            unt.add(digest);
        }

        if (preunit.height() - 1 > round) {
            wpu.setState(State.WAITING_ON_ROUND);
            log.trace("Proposed, waiting: {} current round: {} on: {}", wpu, round, conf.logLabel());
            waitingForRound.put(digest, wpu);
            return;
        }

        log.trace("Proposed: {} on: {}", wpu, conf.logLabel());
        prevote(wpu);
    }

    // Advance the state of the RBC by one round
    private void advance() {
        var iterator = waitingForRound.entrySet().iterator();
        while (iterator.hasNext()) {
            var e = iterator.next();
            if (e.getValue().height() - 1 <= round) {
                iterator.remove();
                log.trace("Advanced: {} clearing round: {} on: {}", e.getValue(), round, conf.logLabel());
                prevote(e.getValue());
            } else {
                log.trace("Waiting for round: {} current: {} on: {}", e.getValue(), round, conf.logLabel());
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
                    log.trace("Waiting: {} for parent: {} on: {}", wp, par, conf.logLabel());
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

    private void commit(Waiting wpu) {
        try {
            wpu.setState(State.COMMITTED);
            Signed<SignedCommit> sc = commit(wpu.id(), wpu.hash(), conf.pid(), conf.signer(), conf.digestAlgorithm());
            signedCommits.put(sc.hash(), sc.signed());
            log.trace("Committing unit: {} on: {}", wpu, conf.logLabel());
            commit(wpu.hash(), conf.pid());
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private boolean decodeParents(Waiting wp) {
        var decoded = dag.decodeParents(wp.pu());
        if (decoded.inError()) {
            switch (decoded.classification()) {
            case CORRECT:
                return true;
            case DUPLICATE_PRE_UNIT:
            case DUPLICATE_UNIT:
            case UNKNOWN_PARENTS:
                return false;
            case ABIGUOUS_PARENTS:
            case COMPLIANCE_ERROR:
            case DATA_ERROR:
                removeFailed(wp);
                return false;
            default:
                break;
            }
            ;
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

    /**
     * Answer the bloom filter with the commits the receiver has
     */
    private Biff haveCommits() {
        return cmts.get(Utils.bitStreamEntropy().nextInt(cmts.size())).toBff();
    }

    /**
     * Answer the bloom filter with the prevotes the receiver has
     */
    private Biff havePreVotes() {
        return prevs.get(Utils.bitStreamEntropy().nextInt(prevs.size())).toBff();
    }

    private Biff haveUnits() {
        return unts.get(Utils.bitStreamEntropy().nextInt(unts.size())).toBff();
    }

    /**
     * Update the gossip builder with the missing units filtered by the supplied
     * bloom filter indicating units already known
     */
    private void missing(BloomFilter<Digest> have, Missing.Builder builder) {
        var pus = new TreeMap<Digest, PreUnit_s>();
        dag.missing(have, pus, epoch);
        waiting.entrySet()
               .stream()
               .filter(e -> !have.contains(e.getKey()))
               .filter(e -> !failed.contains(e.getKey()))
               .forEach(e -> pus.putIfAbsent(e.getKey(), e.getValue().serialized()));
        pus.values().forEach(pu -> builder.addUnits(pu));
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
                log.trace("Parents output, committing: {} parent: {} on: {}", ch, wpu, conf.logLabel());
                wpu.setState(State.COMMITTED);
                commit(ch);
            } else {
                log.trace("Continuing to wait for remaining parents: {} on: {}", ch, conf.logLabel());
            }
        }
    }

    private void prevote(Waiting wpu) {
        wpu.setState(State.PREVOTED);
        Signed<SignedPreVote> spv = prevote(wpu.id(), wpu.hash(), conf.pid(), conf.signer(), conf.digestAlgorithm());
        signedPrevotes.put(spv.hash(), spv.signed());
        log.trace("Prevoting unit: {} on: {}", wpu, conf.logLabel());
        prevote(wpu.hash(), conf.pid());
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
