/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Body;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.Certification;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Header;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.JoinTransaction;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.User;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.Consortium.Result;
import com.salesforce.apollo.consortium.Consortium.Timers;
import com.salesforce.apollo.consortium.TickScheduler.Timer;
import com.salesforce.apollo.consortium.TransactionSimulator.EvaluatedTransaction;
import com.salesforce.apollo.consortium.comms.ConsortiumClientCommunications;
import com.salesforce.apollo.consortium.fsm.Transitions;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * Context for the state machine. These are the leaf actions driven by the FSM.
 *
 */
public class CollaboratorContext {

    private static final Logger log = LoggerFactory.getLogger(CollaboratorContext.class);

    public static Map<HashKey, HashKey> gapsOf(Map<HashKey, CertifiedBlock> hashed, HashKey lastBlock) {
        Map<HashKey, HashKey> missing = new HashMap<>();
        hashed.entrySet().forEach(e -> {
            HashKey p = new HashKey(e.getValue().getBlock().getHeader().getPrevious());
            if (!lastBlock.equals(p)) {
                if (!hashed.containsKey(p)) {
                    missing.put(e.getKey(), p);
                }
            }
        });
        return missing;
    }

    public static long height(Block block) {
        return block.getHeader().getHeight();
    }

    public static long height(CertifiedBlock cb) {
        return height(cb.getBlock());
    }

    public static List<Long> noGaps(Collection<CertifiedBlock> blocks, Map<Long, CurrentBlock> cache) {
        Map<HashKey, CertifiedBlock> hashed = blocks.stream()
                                                    .collect(Collectors.toMap(cb -> new HashKey(
                                                            Conversion.hashOf(cb.getBlock().toByteString())),
                                                                              cb -> cb));

        return noGaps(hashed, cache);
    }

    public static List<Long> noGaps(Map<HashKey, CertifiedBlock> hashed, Map<Long, CurrentBlock> cache) {
        List<Long> ordered = hashed.values()
                                   .stream()
                                   .map(cb -> cb.getBlock().getHeader().getHeight())
                                   .sorted()
                                   .collect(Collectors.toList());
        return LongStream.range(0, ordered.size() - 1)
                         .flatMap(idx -> LongStream.range(ordered.get((int) idx) + 1, ordered.get((int) (idx + 1))))
                         .filter(h -> !cache.containsKey(h))
                         .boxed()
                         .collect(Collectors.toList());
    }

    private final NavigableMap<Long, CurrentBlock>     blockCache        = new ConcurrentSkipListMap<>();
    private final Consortium                           consortium;
    private final AtomicLong                           currentConsensus  = new AtomicLong(-1);
    private final AtomicInteger                        currentRegent     = new AtomicInteger(0);
    private final Map<Integer, Map<Member, StopData>>  data              = new ConcurrentHashMap<>();
    @SuppressWarnings("unused")
    private final Deque<CertifiedBlock>                decided           = new ArrayDeque<>();
    private final AtomicLong                           lastBlock         = new AtomicLong(-1);
    private final AtomicInteger                        nextRegent        = new AtomicInteger(-1);
    private final ProcessedBuffer                      processed;
    private final TransactionSimulator                 simulator;
    private final Set<EnqueuedTransaction>             stopMessages      = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Map<Integer, Sync>                   sync              = new ConcurrentHashMap<>();
    private final Map<Timers, Timer>                   timers            = new ConcurrentHashMap<>();
    private final Map<HashKey, EnqueuedTransaction>    toOrder           = new ConcurrentHashMap<>();
    private final Map<Integer, Set<Member>>            wantRegencyChange = new ConcurrentHashMap<>();
    private final Map<HashKey, CertifiedBlock.Builder> workingBlocks     = new ConcurrentHashMap<>();

    CollaboratorContext(Consortium consortium) {
        this.consortium = consortium;
        Parameters params = consortium.getParams();
        processed = new ProcessedBuffer(params.processedBufferSize);
        simulator = new TransactionSimulator(params.maxBatchByteSize, this, params.maxBatchSize, params.validator);
    }

    public void awaitGenesis() {
        schedule(Timers.AWAIT_GENESIS, () -> {
            consortium.getTransitions().missingGenesis();
        }, consortium.getParams().viewTimeout);
    }

    public void cancel(Timers t) {
        Timer timer = timers.remove(t);
        if (timer != null) {
            log.trace("Cancelling timer: {} on: {}", t, consortium.getMember());
            timer.cancel();
        } else {
            log.trace("No timer to cancel: {} on: {}", t, consortium.getMember());
        }
    }

    public void changeRegency(List<EnqueuedTransaction> transactions) {
        nextRegent(currentRegent() + 1);
        log.info("Starting change of regent from: {} to: {} on: {}", currentRegent(), nextRegent(),
                 consortium.getMember());
        toOrder.values().forEach(t -> t.cancel());
        Stop.Builder data = Stop.newBuilder()
                                .setContext(consortium.viewContext().getId().toByteString())
                                .setNextRegent(nextRegent());
        transactions.forEach(eqt -> data.addTransactions(eqt.getTransaction()));
        Stop stop = data.build();
        consortium.publish(stop);
        consortium.getTransitions().deliverStop(stop, consortium.getMember());
    }

    public void delay(Message message, Member from) {
        consortium.delay(message, from);
    }

    public void deliverBlock(Block block, Member from) {
        Member regent = consortium.viewContext().getRegent(currentRegent());
        if (!regent.equals(from)) {
            log.debug("Ignoring block from non regent: {} actual: {} on: {}", from, regent, getMember());
            return;
        }
        if (block.getBody().getType() == BodyType.GENESIS) {
            deliverGenesisBlock(block, from);
            return;
        }
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
//        long previous = lastBlock();
//        if (block.getHeader().getHeight() <= previous) {
//            log.debug("Rejecting block proposal: {} from {} on: {} not next block: {} last: {}", hash, from,
//                      consortium.getMember(), block.getHeader().getHeight(), previous);
//            return;
//        }
        workingBlocks.computeIfAbsent(hash, k -> {
            Validate validation = consortium.viewContext().generateValidation(hash, block);
            if (validation == null) {
                log.debug("Rejecting block proposal: {}, cannot validate from: {} on: {}", hash, from,
                          consortium.getMember());
                return null;
            }
            lastBlock(block.getHeader().getHeight());
            processToOrder(block);
            consortium.publish(validation);
            return CertifiedBlock.newBuilder()
                                 .setBlock(block)
                                 .addCertifications(Certification.newBuilder()
                                                                 .setId(validation.getId())
                                                                 .setSignature(validation.getSignature()));
        });
    }

    public void deliverStop(Stop data, Member from) {
        if (data.getNextRegent() != currentRegent() + 1) {
            log.trace("Ignoring stop: {} current: {} from {} on: {}", data.getNextRegent(), currentRegent(), from,
                      consortium.getMember());
            return;
        }
        if (sync.containsKey(data.getNextRegent())) {
            log.trace("Ignoring stop, already sync'd: {} from {} on: {}", data.getNextRegent(), from,
                      consortium.getMember());
            return;
        }
        Set<Member> votes = wantRegencyChange.computeIfAbsent(data.getNextRegent(),
                                                              k -> Collections.newSetFromMap(new ConcurrentHashMap<>()));

        if (votes.size() >= consortium.viewContext().majority()) {
            log.trace("Ignoring stop, already established: {} from {} on: {} requesting: {}", data.getNextRegent(),
                      from, consortium.getMember(), votes.stream().map(m -> m.getId()).collect(Collectors.toList()));
            return;
        }
        log.debug("Delivering stop: {} current: {} votes: {} from {} on: {}", data.getNextRegent(), currentRegent(),
                  votes.size(), from, consortium.getMember());
        votes.add(from);
        if (votes.size() >= consortium.viewContext().majority()) {
            log.debug("Majority acheived, stop: {} current: {} votes: {} from {} on: {}", data.getNextRegent(),
                      currentRegent(), votes.size(), from, consortium.getMember());
            data.getTransactionsList()
                .stream()
                .map(tx -> new EnqueuedTransaction(Consortium.hashOf(tx), tx))
                .peek(eqt -> stopMessages.add(eqt))
                .filter(eqt -> !processed.contains(eqt.getHash()))
                .forEach(eqt -> {
                    toOrder.putIfAbsent(eqt.getHash(), eqt);
                });
            consortium.getTransitions().startRegencyChange(stopMessages.stream().collect(Collectors.toList()));
            consortium.getTransitions().establishNextRegent();
        } else {
            log.debug("Majority not acheived, stop: {} current: {} votes: {} from {} on: {}", data.getNextRegent(),
                      currentRegent(), votes.size(), from, consortium.getMember());
        }
    }

    public boolean isRegent(int regency) {
        Member regent = getRegent(regency);
        return consortium.getMember().equals(regent);
    }

    public void deliverStopData(StopData stopData, Member from) {
        int elected = stopData.getCurrentRegent();
        Map<Member, StopData> regencyData = data.computeIfAbsent(elected,
                                                                 k -> new ConcurrentHashMap<Member, StopData>());
        int majority = consortium.viewContext().majority();

        Member regent = getRegent(elected);
        if (!consortium.getMember().equals(regent)) {
            log.trace("ignoring StopData from {} on: {}, incorrect regent: {} not this member: {}", from,
                      consortium.getMember(), elected, regent);
            return;
        }
        if (nextRegent() != elected) {
            log.trace("ignoring StopData from {} on: {}, incorrect regent: {} not next regent: {})", from,
                      consortium.getMember(), elected, nextRegent());
            return;
        }
        if (sync.containsKey(elected)) {
            log.trace("ignoring StopData from {} on: {}, already synchronized for regency: {}", from,
                      consortium.getMember(), elected);
            return;
        }

        Map<HashKey, CertifiedBlock> hashed;
        List<HashKey> hashes = new ArrayList<>();
        hashed = stopData.getBlocksList().stream().collect(Collectors.toMap(cb -> {
            HashKey hash = new HashKey(Conversion.hashOf(cb.getBlock().toByteString()));
            hashes.add(hash);
            return hash;
        }, cb -> cb));
        List<Long> gaps = noGaps(hashed, blockCache);
        if (!gaps.isEmpty()) {
            log.trace("ignoring StopData: {} from {} on: {} gaps in log: {}", elected, from, consortium.getMember(),
                      gaps);
            return;
        }

        log.debug("Delivering StopData: {} from {} on: {}", elected, from, consortium.getMember());
        regencyData.put(from, stopData);
        if (regencyData.size() >= majority) {
            consortium.getTransitions().synchronize(elected, regencyData);
        } else {
            log.trace("accepted StopData: {} votes: {} from {} on: {}", elected, regencyData.size(), from,
                      consortium.getMember());
        }
    }

    public void deliverSync(Sync syncData, Member from) {
        int cReg = syncData.getCurrentRegent();
        if (nextRegent() != cReg) {
            log.trace("Rejecting Sync from: {} on: {} expected regent: {} provided: {}", from, consortium.getMember(),
                      cReg, nextRegent());
            return;
        }
        Member regent = getRegent(cReg);
        if (!regent.equals(from)) {
            log.trace("Rejecting Sync from: {} invalid regent on: {} expected regent: {}", from, consortium.getMember(),
                      regent);
            return;
        }
        if (sync.get(cReg) != null) {
            log.trace("Rejecting Sync from: {} regent: {} on: {} consensus already proved", from, cReg,
                      consortium.getMember());
            return;
        }

        if (!validate(from, syncData, cReg)) {
            log.trace("Rejecting Sync from: {} regent: {} on: {} cannot verify Sync log", from, cReg,
                      consortium.getMember());
            return;
        }
        if (!validate(regent, syncData, cReg)) {
            log.error("Invalid Sync from: {} regent: {} current: {} on: {}", from, cReg, currentRegent(),
                      consortium.getMember());
        }
        log.debug("Delivering Sync from: {} regent: {} current: {} on: {}", from, cReg, currentRegent(),
                  consortium.getMember());
        currentRegent(nextRegent());
        sync.put(cReg, syncData);
        synchronize(syncData, regent);
        consortium.getTransitions().syncd();
        resolveRegentStatus();
    }

    public void deliverValidate(Validate v) {
        HashKey hash = new HashKey(v.getHash());
        CertifiedBlock.Builder certifiedBlock = workingBlocks.get(hash);
        if (certifiedBlock == null) {
            log.debug("No working block to validate: {} on: {}", hash, consortium.getMember());
            return;
        }
        HashKey memberID = new HashKey(v.getId());
        if (consortium.viewContext().validate(certifiedBlock.getBlock(), v)) {
            certifiedBlock.addCertifications(Certification.newBuilder()
                                                          .setId(v.getId())
                                                          .setSignature(v.getSignature()));
            log.debug("Adding block validation: {} from: {} on: {} count: {}", hash, memberID, consortium.getMember(),
                      certifiedBlock.getCertificationsCount());
        } else {
            log.debug("Failed block validation: {} from: {} on: {}", hash, memberID, consortium.getMember());
        }
    }

    public void drainBlocks() {
        cancel(Timers.FLUSH_BATCH);
        generate();
        scheduleFlush();
    }

    public void establishGenesisView() {
        ViewContext newView = new ViewContext(Consortium.GENESIS_VIEW_ID, consortium.getParams().context,
                consortium.getMember(), consortium.nextViewConsensusKey(), Collections.emptyList(),
                consortium.entropy());
        newView.activeAll();
        consortium.viewChange(newView);
        if (consortium.viewContext().isMember()) {
            currentRegent(-1);
            nextRegent(-2);
            consortium.pause();
            consortium.joinMessageGroup(newView);
            consortium.getTransitions().generateView();
            consortium.resume();
        }
    }

    public void establishNextRegent() {
        if (currentRegent() == nextRegent()) {
            log.trace("Regent already established on {}", consortium.getMember());
            return;
        }
        currentRegent(nextRegent());
        reschedule();
        Member leader = getRegent(currentRegent());
        StopData stopData = buildStopData(currentRegent());
        if (stopData == null) {
            return;
        }
        if (consortium.getMember().equals(leader)) {
            consortium.getTransitions().synchronizingLeader();
            consortium.getTransitions().deliverStopData(stopData, consortium.getMember());
        } else {
            ConsortiumClientCommunications link = consortium.linkFor(leader);
            if (link == null) {
                log.warn("Cannot get link to leader: {} on: {}", leader, consortium.getMember());
            } else {
                try {
                    log.trace("Sending StopData: {} regent: {} on: {}", currentRegent(), leader,
                              consortium.getMember());
                    link.stopData(stopData);
                } catch (Throwable e) {
                    log.warn("Error sending stop data: {} to: {} on: {}", currentRegent(), leader,
                             consortium.getMember());
                } finally {
                    link.release();
                }
            }
        }
    }

    public void generateBlocks() {
        nextBatch();
        generate();
        scheduleFlush();
    }

    public void generateView() {
        if (consortium.getCurrent() == null) {
            log.trace("Generating genesis view on: {}", consortium.getMember());
            generateGenesisView();
        } else {
            log.trace("Generating view on: {}", consortium.getMember());
            generateNextView();
        }
    }

    public Member getMember() {
        return consortium.getMember();
    }

    public void initializeConsensus() {
        CurrentBlock current = consortium.getCurrent();
        currentConsensus(current != null ? current.getBlock().getHeader().getHeight() : 0);
        simulator.start();
    }

    public void join() {
        log.debug("Petitioning to join view: {} on: {}", consortium.viewContext().getId(),
                  consortium.getParams().member);

        Join voteForMe = Join.newBuilder()
                             .setMember(consortium.getCurrent() == null
                                     ? consortium.viewContext().getView(consortium.getParams().signature.get())
                                     : consortium.getNextView())
                             .setContext(consortium.viewContext().getId().toByteString())
                             .build();
        List<Result> votes = consortium.viewContext()
                                       .streamRandomRing()
                                       .filter(m -> !m.equals(consortium.getParams().member))
                                       .map(c -> {
                                           ConsortiumClientCommunications link = consortium.linkFor(c);
                                           if (link == null) {
                                               log.warn("Cannot get link for: {} on: {}", c.getId(),
                                                        consortium.getMember());
                                               return null;
                                           }
                                           JoinResult vote;
                                           try {
                                               vote = link.join(voteForMe);
                                           } catch (Throwable e) {
                                               log.trace("Unable to poll vote from: {}:{} on {}", c, e.getMessage(),
                                                         consortium.getMember());
                                               return null;
                                           }

                                           log.trace("One vote to join: {} : {} from: {} on: {}",
                                                     consortium.getParams().member, consortium.viewContext().getId(), c,
                                                     consortium.getMember());
                                           return new Consortium.Result(c, vote);
                                       })
                                       .filter(r -> r != null)
                                       .filter(r -> r.vote.isInitialized())
                                       .limit(consortium.viewContext().majority())
                                       .collect(Collectors.toList());

        if (votes.size() < consortium.viewContext().majority()) {
            log.debug("Did not gather votes necessary to join consortium needed: {} got: {} on: {}",
                      consortium.viewContext().majority(), votes.size(), consortium.getMember());
            return;
        }
        JoinTransaction.Builder txn = JoinTransaction.newBuilder()
                                                     .setRegency(currentRegent())
                                                     .setMember(voteForMe.getMember());
        for (Result vote : votes) {
            txn.addCertification(Certification.newBuilder()
                                              .setId(vote.member.getId().toByteString())
                                              .setSignature(vote.vote.getSignature()));
        }
        JoinTransaction joinTxn = txn.build();

        HashKey txnHash;
        try {
            txnHash = consortium.submit(true, h -> {
            }, joinTxn);
        } catch (TimeoutException e) {
            return;
        }
        log.debug("Successfully petitioned: {} to join view: {} on: {}", txnHash, consortium.viewContext().getId(),
                  consortium.getParams().member);
    }

    public void joinView() {
        joinView(0);
    }

    public void processCheckpoint(CurrentBlock next) {
        Checkpoint body = checkpointBody(next.getBlock());
        if (body == null) {
            return;
        }
        body.getTransactionsList().forEach(txn -> {
            HashKey hash = new HashKey(txn.getHash());
            finalized(hash);
        });
        accept(next);
        log.info("Processed checkpoint block: {} on: {}", next.getHash(), consortium.getMember());
    }

    public void processGenesis(CurrentBlock next) {
        Genesis body = genesisBody(next.getBlock());
        if (body == null) {
            return;
        }
        accept(next);
        cancelToTimers();
        toOrder.clear();
        consortium.getSubmitted().clear();
        consortium.getTransitions().genesisAccepted();
        reconfigure(body.getInitialView(), true);
        log.info("Processed genesis block: {} on: {}", next.getHash(), consortium.getMember());
    }

    public void processReconfigure(CurrentBlock next) {
        Reconfigure body = reconfigureBody(next.getBlock());
        if (body == null) {
            return;
        }
        accept(next);
        reconfigure(body, false);
        log.info("Processed reconfigure block: {} on: {}", next.getHash(), consortium.getMember());
    }

    public void processUser(CurrentBlock next) {
        User body = userBody(next.getBlock());
        if (body == null) {
            return;
        }
        body.getTransactionsList().forEach(txn -> {
            HashKey hash = new HashKey(txn.getHash());
            finalized(hash);
        });
        accept(next);
        log.info("Processed user block: {} on: {}", next.getHash(), consortium.getMember());
    }

    public void receive(ReplicateTransactions transactions, Member from) {
        transactions.getTransactionsList().forEach(txn -> receive(txn, true));
    }

    public void receive(Transaction txn) {
        receive(txn, false);
    }

    public boolean receive(Transaction txn, boolean replicated) {
        EnqueuedTransaction transaction = new EnqueuedTransaction(Consortium.hashOf(txn), txn);
        if (processed.contains(transaction.getHash())) {
            return false;
        }
        AtomicBoolean added = new AtomicBoolean(); // this is stupid
        toOrder.computeIfAbsent(transaction.getHash(), k -> {
            if (txn.getJoin()) {
                JoinTransaction join;
                try {
                    join = txn.getBatch(0).unpack(JoinTransaction.class);
                } catch (InvalidProtocolBufferException e) {
                    log.debug("Cannot deserialize join on: {}", consortium.getMember(), e);
                    return null;
                }
                HashKey memberID = new HashKey(join.getMember().getId());
                log.trace("Join txn:{} received on: {} self join: {}", transaction.getHash(), consortium.getMember(),
                          consortium.getMember().getId().equals(memberID));
            } else {
                log.trace("Client txn:{} received on: {} ", transaction.getHash(), consortium.getMember());
            }
            schedule(transaction, replicated);
            added.set(true);
            return transaction;
        });
        return added.get();
    }

    public void receiveJoin(Transaction txn) {
        if (!txn.getJoin()) {
            return;
        }
        receive(txn);
        reduceJoinTransactions();
    }

    public void receiveJoins(ReplicateTransactions transactions, Member from) {
        receive(transactions, from);
        reduceJoinTransactions();
    }

    public void resolveRegentStatus() {
        Member regent = getRegent(nextRegent());
        log.debug("Regent: {} on: {}", regent, consortium.getMember());
        if (consortium.getMember().equals(regent)) {
            consortium.getTransitions().becomeLeader();
        } else {
            consortium.getTransitions().becomeFollower();
        }
    }

    public void shutdown() {
        consortium.stop();
    }

    public void stopSimulation() {
        simulator.stop();
    }

    public void synchronize(int elected, Map<Member, StopData> regencyData) {
        log.trace("Start synchronizing: {} votes: {} on: {}", elected, regencyData.size(), consortium.getMember());
        try {
            Sync synch = buildSync(elected, regencyData);
            if (synch != null) {
                log.debug("Synchronizing new regent: {} on: {} voting: {}", elected, consortium.getMember(),
                         regencyData.keySet().stream().map(e -> e.getId()).collect(Collectors.toList()));
                consortium.getTransitions().synchronizingLeader();
                consortium.publish(synch);
                consortium.getTransitions().deliverSync(synch, consortium.getMember());
            } else {
                log.error("Cannot generate Sync regent: {} on: {} voting: {}", elected, consortium.getMember(),
                          regencyData.keySet().stream().map(e -> e.getId()).collect(Collectors.toList()));
            }
        } catch (RuntimeException e) {
            throw e;
        }
    }

    public void totalOrderDeliver() {
        log.trace("Attempting total ordering of working blocks: {} current consensus: {} on: {}", workingBlocks.size(),
                  currentConsensus(), consortium.getMember());
        List<HashKey> published = new ArrayList<>();
        workingBlocks.entrySet()
                     .stream()
                     .peek(e -> log.trace("TO Consider: {}:{} on: {}", e.getKey(),
                                          e.getValue().getCertificationsCount(), consortium.getMember()))
                     .filter(e -> e.getValue().getCertificationsCount() >= consortium.viewContext().majority())
                     .sorted((a, b) -> Long.compare(a.getValue().getBlock().getHeader().getHeight(),
                                                    b.getValue().getBlock().getHeader().getHeight()))
                     .filter(e -> e.getValue().getBlock().getHeader().getHeight() >= currentConsensus() + 1)
                     .forEach(e -> {
                         if (e.getValue().getBlock().getHeader().getHeight() == currentConsensus() + 1) {
                             currentConsensus(e.getValue().getBlock().getHeader().getHeight());
                             log.info("Totally ordering block: {} height: {} on: {}", e.getKey(),
                                      e.getValue().getBlock().getHeader().getHeight(), consortium.getMember());
                             consortium.getParams().consensus.apply(e.getValue().build());
                             published.add(e.getKey());
                         }
                     });
        published.forEach(h -> workingBlocks.remove(h));
    }

    void clear() {
        currentRegent(-1);
        nextRegent(-2);
        currentConsensus(-1);
        timers.values().forEach(e -> e.cancel());
        timers.clear();
        cancelToTimers();
        toOrder.clear();
        lastBlock(-1);
        consortium.getScheduler().cancelAll();
        workingBlocks.clear();
    }

    public int currentRegent() {
        return currentRegent.get();
    }

    void drainPending() {
        consortium.getTransitions().drainPending();
    }

    Map<HashKey, EnqueuedTransaction> getToOrder() {
        return toOrder;
    }

    void reconfigure(Reconfigure view, boolean genesis) {
        consortium.pause();
        ViewContext newView = new ViewContext(view, consortium.getParams().context, consortium.getMember(),
                genesis ? consortium.viewContext().getConsensusKey() : consortium.nextViewConsensusKey(),
                consortium.entropy());
        currentRegent(genesis ? 2 : 0);
        nextRegent(-1);
        consortium.viewChange(newView);
        resolveStatus();
    }

    private void accept(CurrentBlock next) {
        workingBlocks.remove(next.getHash());
        consortium.setCurrent(next);
        blockCache.put(next.getBlock().getHeader().getHeight(), next);
    }

    private StopData buildStopData(int currentRegent) {
        Map<HashKey, CertifiedBlock> decided;
        decided = workingBlocks.entrySet()
                               .stream()
                               .filter(e -> e.getValue().getCertificationsCount() >= consortium.viewContext()
                                                                                               .majority())
                               .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().build()));
        List<Long> gaps = noGaps(decided, blockCache);
        if (!gaps.isEmpty()) {
            log.debug("Have no valid log on: {} gaps: {}", consortium.getMember(), gaps);
        }
        StopData.Builder builder = StopData.newBuilder().setCurrentRegent(currentRegent);
        builder.setContext(consortium.viewContext().getId().toByteString());
        decided.entrySet().forEach(e -> {
            builder.addBlocks(e.getValue());
        });
        return builder.build();
    }

    private Sync buildSync(int elected, Map<Member, StopData> regencyData) {
        List<CertifiedBlock> blocks = new ArrayList<>();
        Map<HashKey, CertifiedBlock> hashed = new HashMap<>();
        regencyData.values().stream().flatMap(sd -> sd.getBlocksList().stream()).forEach(cb -> {
            hashed.put(new HashKey(Conversion.hashOf(cb.getBlock().toByteString())), cb);
        });
        List<Long> gaps = noGaps(hashed, blockCache);
        if (!gaps.isEmpty()) {
            log.debug("Gaps in sync log, trimming to valid log on: {} gaps: {}", consortium.getMember(), gaps);
            AtomicLong next = new AtomicLong(-1);
            blocks = hashed.values().stream().sorted((a, b) -> Long.compare(height(a), height(b))).filter(cb -> {
                if (next.get() < 0) {
                    next.set(height(cb) + 1);
                    return true;
                } else {
                    long height = height(cb);
                    return next.compareAndSet(height - 1, height);
                }
            }).collect(Collectors.toList());
        } else {
            blocks = hashed.values()
                           .stream()
                           .sorted((a, b) -> Long.compare(height(a), height(b)))
                           .collect(Collectors.toList());
        }
        return Sync.newBuilder()
                   .setCurrentRegent(elected)
                   .setContext(consortium.viewContext().getId().toByteString())
                   .addAllBlocks(blocks)
                   .build();
    }

    private void cancelToTimers() {
        toOrder.values().forEach(eqt -> eqt.cancel());
    }

    private Checkpoint checkpointBody(Block block) {
        Checkpoint body;
        try {
            body = Checkpoint.parseFrom(consortium.getBody(block));
        } catch (IOException e) {
            log.debug("Protocol violation on: {}.  Cannot decode checkpoint body: {}", consortium.getMember(), e);
            return null;
        }
        return body;
    }

    private long currentConsensus() {
        return currentConsensus.get();
    }

    private void currentConsensus(long l) {
        currentConsensus.set(l);
    }

    private void currentRegent(int c) {
        int prev = currentRegent.getAndSet(c);
        if (prev != c) {
            assert c == prev || c > prev || c == -1 : "whoops: " + prev + " -> " + c;
            log.trace("Current regency set to: {} previous: {} on: {} ", c, prev, consortium.getMember());
        }
    }

    private void deliverGenesisBlock(final Block block, Member from) {
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        if (block.getHeader().getHeight() != 0) {
            log.debug("Rejecting genesis block proposal: {} height: {} from {}, not block height 0", hash,
                      block.getHeader().getHeight(), from);
        }

        if (block.getBody().getType() != BodyType.GENESIS) {
            log.error("Failed on {} [{}] prev: [{}] delivering genesis block: {} invalid body: {}",
                      consortium.getMember(), consortium.fsm().prettyPrint(consortium.fsm().getCurrentState()),
                      consortium.fsm().prettyPrint(consortium.fsm().getPreviousState()), hash,
                      block.getBody().getType());
            return;
        }
        workingBlocks.computeIfAbsent(hash, k -> {
            final Genesis genesis = genesisBody(block);
            if (genesis == null) {
                return null;
            }
            cancelToTimers();
            toOrder.clear();
            Validate validation = consortium.viewContext().generateValidation(hash, block);
            if (validation == null) {
                log.error("Cannot validate generated genesis: {} on: {}", hash, consortium.getMember());
                return null;
            }
            lastBlock(block.getHeader().getHeight());
            consortium.setViewContext(consortium.viewContext().cloneWith(genesis.getInitialView().getViewList()));
            consortium.publish(validation);
            return CertifiedBlock.newBuilder()
                                 .setBlock(block)
                                 .addCertifications(Certification.newBuilder()
                                                                 .setId(validation.getId())
                                                                 .setSignature(validation.getSignature()));
        });
    }

    private void finalized(HashKey hash) {
        EnqueuedTransaction removed = toOrder.remove(hash);
        if (removed != null) {
            removed.cancel();
            processed.add(removed.getHash());
            consortium.finalized(removed);
        }
        SubmittedTransaction submittedTxn = consortium.getSubmitted().remove(hash);
        if (submittedTxn != null) {
            if (submittedTxn.onCompletion != null) {
                log.debug("Completing {} txn: {} on: {}", submittedTxn.submitted.getJoin() ? "JOIN" : "USER", hash,
                         consortium.getMember());
                ForkJoinPool.commonPool().execute(() -> submittedTxn.onCompletion.accept(hash));
            }
        } else {
            log.debug("Processing txn: {} on: {}", hash, consortium.getMember());
        }
    }

    private void firstTimeout(EnqueuedTransaction transaction) {
        assert !transaction.isTimedOut() : "this should be unpossible";
        transaction.setTimedOut(true);
        ReplicateTransactions.Builder builder = ReplicateTransactions.newBuilder()
                                                                     .addTransactions(transaction.getTransaction());

        Parameters params = consortium.getParams();
        long span = transaction.getTransaction().getJoin() ? params.joinTimeout.toMillis()
                : params.submitTimeout.toMillis();
        toOrder.values()
               .stream()
               .filter(eqt -> eqt.getDelay() <= span)
               .limit(99)
               .peek(eqt -> eqt.cancel())
               .peek(eqt -> eqt.setTimedOut(true))
               .map(eqt -> eqt.getTransaction())
               .forEach(txn -> builder.addTransactions(txn));
        log.debug("Replicating from: {} count: {} first timeout on: {}", transaction.getHash(),
                  builder.getTransactionsCount(), consortium.getMember());
        ReplicateTransactions transactions = builder.build();
        transaction.setTimer(consortium.getScheduler()
                                       .schedule(Timers.TRANSACTION_TIMEOUT_2, () -> secondTimeout(transaction),
                                                 transaction.getTransaction().getJoin() ? params.joinTimeout
                                                         : params.submitTimeout));
        consortium.publish(transactions);
    }

    private void generate() {
        boolean generated = generateNextBlock();
        while (generated) {
            generated = generateNextBlock();
        }
    }

    private void generateGenesisBlock() {
        reduceJoinTransactions();
        assert toOrder.size() >= consortium.viewContext().majority() : "Whoops";
        log.debug("Generating genesis on {} join transactions: {}", consortium.getMember(), toOrder.size());
        byte[] nextView = new byte[32];
        consortium.entropy().nextBytes(nextView);
        Reconfigure.Builder genesisView = Reconfigure.newBuilder()
                                                     .setCheckpointBlocks(256)
                                                     .setId(ByteString.copyFrom(nextView))
                                                     .setTolerance(consortium.viewContext().majority());
        toOrder.values().forEach(join -> {
            JoinTransaction txn;
            try {
                txn = join.getTransaction().getBatch(0).unpack(JoinTransaction.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("Cannot generate genesis, unable to parse Join txnL {} on: {}", join.getHash(),
                          consortium.getMember());
                consortium.getTransitions().fail();
                return;
            }
            processed.add(join.getHash());
            genesisView.addTransactions(ExecutedTransaction.newBuilder()
                                                           .setHash(join.getHash().toByteString())
                                                           .setTransaction(join.getTransaction())
                                                           .build());
            genesisView.addView(txn.getMember());
        });
        toOrder.values().forEach(e -> e.cancel());
        toOrder.clear();
        Body genesisBody = Body.newBuilder()
                               .setType(BodyType.GENESIS)
                               .setContents(Consortium.compress(Genesis.newBuilder()
                                                                       .setGenesisData(ByteString.copyFrom(consortium.getGenesisData()))
                                                                       .setInitialView(genesisView)
                                                                       .build()
                                                                       .toByteString()))
                               .build();
        Block block = Block.newBuilder()
                           .setHeader(Header.newBuilder()
                                            .setHeight(0)
                                            .setBodyHash(ByteString.copyFrom(Conversion.hashOf(genesisBody.toByteString())))
                                            .build())
                           .setBody(genesisBody)
                           .build();
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        log.info("Genesis block: {} generated on {}", hash, consortium.getMember());
        workingBlocks.computeIfAbsent(hash, k -> {
            lastBlock(0);
            Validate validation = consortium.viewContext().generateValidation(hash, block);
            if (validation == null) {
                log.error("Cannot validate generated genesis block: {} on: {}", hash, consortium.getMember());
            }
            CertifiedBlock.Builder builder = CertifiedBlock.newBuilder().setBlock(block);
            builder.addCertifications(Certification.newBuilder()
                                                   .setId(validation.getId())
                                                   .setSignature(validation.getSignature()));

            consortium.setViewContext(consortium.viewContext().cloneWith(genesisView.getViewList()));
            consortium.publish(block);
            consortium.publish(validation);
            return builder;
        });
    }

    private void generateGenesisView() {
        if (!selfJoinRecorded()) {
            log.trace("Join transaction not found on: {}, rescheduling", consortium.getMember());
            rescheduleGenesis();
            return;
        }
        int txns = toOrder.size();
        if (txns >= consortium.viewContext().activeCardinality()) {
            generateGenesisBlock();
        } else {
            log.trace("Genesis group has not formed, rescheduling, have: {} want: {} on: {}", txns,
                      consortium.viewContext().activeCardinality(), consortium.getMember());
            rescheduleGenesis();
        }
    }

    private boolean generateNextBlock() {
        final long currentHeight = lastBlock();
        final CurrentBlock currentBlock = blockCache.get(currentHeight);
        final long thisHeight = currentHeight + 1;
        if (currentBlock == null) {
            log.debug("Cannot generate next block: {} on: {}, as previous block for height: {} not found", thisHeight,
                      consortium.getMember(), currentHeight);
            return false;
        }

        if (simulator.peek() == null) {
//            log.trace("No transactions to generate block on: {}", consortium.getMember());
            return false;
        }

        User.Builder user = User.newBuilder();
        int processedBytes = 0;
        List<HashKey> processed = new ArrayList<>();

        while (simulator.peek() != null && processed.size() <= consortium.getParams().maxBatchByteSize
                && processedBytes <= consortium.getParams().maxBatchByteSize) {
            EvaluatedTransaction txn = simulator.poll();
            if (txn != null) {
                processedBytes += txn.getSerializedSize();
                user.addTransactions(ExecutedTransaction.newBuilder()
                                                        .setHash(txn.transaction.getHash().toByteString())
                                                        .setTransaction(txn.transaction.getTransaction()))
                    .addResponses(txn.result);
                processed.add(txn.transaction.getHash());
            }
        }

        if (processed.size() == 0) {
            log.debug("No transactions to generate block on: {}", consortium.getMember());
            return false;
        }
        log.debug("Generating next block on: {} height: {} transactions: {}", consortium.getMember(), thisHeight,
                 processed.size());

        Body body = Body.newBuilder()
                        .setType(BodyType.USER)
                        .setContents(Consortium.compress(user.build().toByteString()))
                        .build();

        Block block = Block.newBuilder()
                           .setHeader(Header.newBuilder()
                                            .setPrevious(currentBlock.getHash().toByteString())
                                            .setHeight(thisHeight)
                                            .setBodyHash(ByteString.copyFrom(Conversion.hashOf(body.toByteString())))
                                            .build())
                           .setBody(body)
                           .build();
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));

        CertifiedBlock.Builder builder = workingBlocks.computeIfAbsent(hash, k -> CertifiedBlock.newBuilder()
                                                                                                .setBlock(block));
        blockCache.put(thisHeight, new CurrentBlock(hash, block));
        consortium.publish(block);
        lastBlock(thisHeight);

        Validate validation = consortium.viewContext().generateValidation(hash, block);
        if (validation == null) {
            log.debug("Cannot generate validation for block: {} on: {}", hash, consortium.getMember());
            return false;
        }
        builder.addCertifications(Certification.newBuilder()
                                               .setId(validation.getId())
                                               .setSignature(validation.getSignature()));
        consortium.publish(validation);

        log.info("Generated next block: {} height: {} on: {} txns: {}", hash, thisHeight, consortium.getMember(),
                 user.getTransactionsCount());
        return true;
    }

    private void generateNextView() {
        // TODO Auto-generated method stub

    }

    private Genesis genesisBody(Block block) {
        Genesis body;
        try {
            body = Genesis.parseFrom(consortium.getBody(block));
        } catch (IOException e) {
            log.debug("Protocol violation on {}.  Cannot decode genesis body: {}", consortium.getMember(), e);
            return null;
        }
        return body;
    }

    private Member getRegent(int regent) {
        return consortium.viewContext().getRegent(regent);
    }

    private void joinView(int attempt) {
        boolean selfJoin = selfJoinRecorded();
        int majority = consortium.viewContext().majority();
        if (attempt < 20) {
            if (selfJoin && toOrder.size() == consortium.viewContext().activeCardinality()) {
                log.trace("View formed, attempt: {} on: {} have: {} require: {} self join: {}", attempt,
                          consortium.getMember(), toOrder.size(), majority, selfJoin);
                consortium.getTransitions().formView();
                return;
            }
        }

        if (attempt >= 20) {
            if (selfJoin && toOrder.size() >= majority) {
                log.trace("View formed, attempt: {} on: {} have: {} require: {} self join: {}", attempt,
                          consortium.getMember(), toOrder.size(), majority, selfJoin);
                consortium.getTransitions().formView();
                return;
            }
        }

        log.trace("View has not been formed, attempt: {} rescheduling on: {} have: {} require: {} self join: {}",
                  attempt, consortium.getMember(), toOrder.size(), majority, selfJoin);
        join();
        schedule(Timers.AWAIT_VIEW_MEMBERS, () -> joinView(attempt + 1), consortium.getParams().viewTimeout);
    }

    private long lastBlock() {
        return lastBlock.get();
    }

    private void lastBlock(long l) {
        lastBlock.set(l);
    }

    private void nextBatch() {
        if (toOrder.isEmpty()) {
//            log.debug("No transactions available to batch on: {}:{}", consortium.getMember(), toOrder.size());
            return;
        }
        List<EnqueuedTransaction> batch = toOrder.values()
                                                 .stream()
                                                 .limit(simulator.available())
                                                 .map(eqt -> simulator.add(eqt) ? eqt : null)
                                                 .filter(eqt -> eqt != null)
                                                 .peek(eqt -> eqt.cancel())
                                                 .collect(Collectors.toList());
        batch.forEach(eqt -> {
            eqt.cancel();
            toOrder.remove(eqt.getHash());
            processed.add(eqt.getHash());
        });
        if (!batch.isEmpty()) {
            log.debug("submitting batch: {} for simulation on: {}", batch.size(), consortium.getMember());
        }
    }

    public int nextRegent() {
        return nextRegent.get();
    }

    private void nextRegent(int n) {
        nextRegent.set(n);
    }

    private void processToOrder(Block block) {
        switch (block.getBody().getType()) {
        case CHECKPOINT: {
            Checkpoint body = checkpointBody(block);
            processToOrder(body.getTransactionsList());
            break;
        }
        case GENESIS: {
            Genesis body = genesisBody(block);
            processToOrder(body.getInitialView().getTransactionsList());
            break;
        }
        case RECONFIGURE: {
            Reconfigure body = reconfigureBody(block);
            processToOrder(body.getTransactionsList());
            break;
        }
        case USER: {
            User body = userBody(block);
            processToOrder(body.getTransactionsList());
            break;
        }
        case UNRECOGNIZED:
            break;
        default:
            break;

        }
    }

    private void processToOrder(List<ExecutedTransaction> transactions) {
        transactions.forEach(et -> {
            HashKey hash = new HashKey(et.getHash());
            EnqueuedTransaction p = toOrder.remove(hash);
            if (p != null) {
                p.cancel();
                processed.add(hash);
            }
        });
    }

    private Reconfigure reconfigureBody(Block block) {
        Reconfigure body;
        try {
            body = Reconfigure.parseFrom(consortium.getBody(block));
        } catch (IOException e) {
            log.debug("Protocol violation on: {}.  Cannot decode reconfiguration body: {}", consortium.getMember(), e);
            return null;
        }
        return body;
    }

    private void reduceJoinTransactions() {
        Map<HashKey, EnqueuedTransaction> reduced = new HashMap<>(); // Member ID -> join txn
        toOrder.forEach((h, eqt) -> {
            try {
                JoinTransaction join = eqt.getTransaction().getBatch(0).unpack(JoinTransaction.class);
                EnqueuedTransaction prev = reduced.put(new HashKey(join.getMember().getId()), eqt);
                if (prev != null) {
                    prev.cancel();
                }
            } catch (InvalidProtocolBufferException e) {
                log.error("Failure deserializing ToOrder txn Join on {}", consortium.getMember(), e);
            }
        });
//            log.trace("Reduced joins on: {} current: {} prev: {}", getMember(), reduced.size(), toOrder.size());
        toOrder.clear();
        reduced.values().forEach(eqt -> toOrder.put(eqt.getHash(), eqt));
    }

    private void reschedule() {
        toOrder.values().forEach(eqt -> schedule(eqt, false));
    }

    private void rescheduleGenesis() {
        schedule(Timers.AWAIT_GROUP, () -> {
            boolean selfJoin = selfJoinRecorded();
            if (selfJoin && toOrder.size() >= consortium.viewContext().majority()) {
                generateGenesisBlock();
            } else {
                log.trace("Genesis group has not formed, rescheduling, have: {} want: {} self join: {} on: {}",
                          toOrder.size(), consortium.viewContext().majority(), selfJoin, consortium.getMember());
                rescheduleGenesis();
            }
        }, consortium.getParams().viewTimeout);
    }

    private void resolveStatus() {
        Member regent = getRegent(currentRegent());
        if (consortium.viewContext().isViewMember()) {
            if (consortium.getMember().equals(regent)) {
                log.debug("becoming leader on: {}", consortium.getMember());
                consortium.getTransitions().becomeLeader();
            } else {
                log.debug("becoming follower on: {} regent: {}", consortium.getMember(), regent);
                consortium.getTransitions().becomeFollower();
            }
        } else if (consortium.viewContext().isMember()) {
            log.debug("becoming joining member on: {} regent: {}", consortium.getMember(), regent);
            consortium.getTransitions().joinAsMember();
        } else {
            log.debug("becoming client on: {} regent: {}", consortium.getMember(), regent);
            consortium.getTransitions().becomeClient();
        }
    }

    private Timer schedule(EnqueuedTransaction eqt, boolean replicated) {
        eqt.cancel();
        Parameters params = consortium.getParams();
        Duration delta = eqt.getTransaction().getJoin() ? params.joinTimeout : params.submitTimeout;
//        log.info("scheduling transaction: {} for: {} ms first: {} on: {}", eqt.getHash(), delta.toMillis(), !replicated,
//                 consortium.getMember());
        Timer timer = consortium.getScheduler().schedule(Timers.TRANSACTION_TIMEOUT_1, () -> {
            if (replicated) {
                eqt.setTimedOut(true);
                secondTimeout(eqt);
            } else {
                eqt.setTimedOut(false);
                firstTimeout(eqt);
            }
        }, delta);
        eqt.setTimer(timer);
        return timer;
    }

    private void schedule(Timers label, Runnable a, Duration delta) {
        Transitions timerState = consortium.fsm().getCurrentState();
        Runnable action = () -> {
            timers.remove(label);
            Transitions currentState = consortium.fsm().getCurrentState();
            if (timerState.equals(currentState)) {
                a.run();
            } else {
                log.debug("discarding timer for: {} scheduled on: {} but timed out: {} on: {}", label, timerState,
                          currentState, consortium.getMember());
            }
        };
        timers.computeIfAbsent(label, k -> {
            log.trace("Setting timer for: {} duration: {} ms on: {}", label, delta.toMillis(), consortium.getMember());
            return consortium.getScheduler().schedule(k, action, delta);
        });
    }

    private void scheduleFlush() {
        schedule(Timers.FLUSH_BATCH, () -> generateBlocks(), consortium.getParams().maxBatchDelay);
    }

    private void secondTimeout(EnqueuedTransaction transaction) {
        log.debug("Second timeout for: {} on: {}", transaction.getHash(), consortium.getMember());
        Parameters params = consortium.getParams();
        long span = transaction.getTransaction().getJoin() ? params.joinTimeout.toMillis()
                : params.submitTimeout.toMillis();
        List<EnqueuedTransaction> timedOut = toOrder.values()
                                                    .stream()
                                                    .filter(eqt -> eqt.getDelay() <= span)
                                                    .limit(99)
                                                    .peek(eqt -> eqt.cancel())
                                                    .collect(Collectors.toList());
        consortium.getTransitions().startRegencyChange(timedOut);
    }

    private boolean selfJoinRecorded() {
        HashKey id = consortium.getMember().getId();
        return toOrder.values().stream().map(eqt -> eqt.getTransaction()).map(t -> {
            try {
                return t.getBatch(0).unpack(JoinTransaction.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("Cannot generate genesis, unable to parse Join txn on: {}", consortium.getMember());
                return null;
            }
        }).filter(jt -> jt != null).anyMatch(jt -> id.equals(new HashKey(jt.getMember().getId())));
    }

    private void synchronize(Sync syncData, Member regent) {
        CurrentBlock current = consortium.getCurrent();
        final long currentHeight = current != null ? height(current.getBlock()) : -1;
        workingBlocks.clear();
        syncData.getBlocksList()
                .stream()
                .sorted((a, b) -> Long.compare(height(a), height(b)))
                .filter(cb -> height(cb) > currentHeight)
                .forEach(cb -> {
                    HashKey hash = new HashKey(Conversion.hashOf(cb.getBlock().toByteString()));
                    workingBlocks.put(hash, cb.toBuilder());
                    blockCache.put(cb.getBlock().getHeader().getHeight(), new CurrentBlock(hash, cb.getBlock()));
                    lastBlock(height(cb));
                    processToOrder(cb.getBlock());
                });
        log.debug("Synchronized from: {} to: {} working blocks: {} on: {}", currentHeight, lastBlock(),
                 workingBlocks.size(), consortium.getMember());
        if (getMember().equals(regent)) {
            totalOrderDeliver();
        }
    }

    private User userBody(Block block) {
        User body;
        try {
            body = User.parseFrom(consortium.getBody(block));
        } catch (IOException e) {
            log.debug("Protocol violation on: {}.  Cannot decode reconfiguration body", consortium.getMember(), e);
            return null;
        }
        return body;
    }

    private boolean validate(Member regent, Sync sync, int regency) {
        Map<HashKey, CertifiedBlock> hashed;
        List<HashKey> hashes = new ArrayList<>();
        hashed = sync.getBlocksList()
                     .stream()
                     .filter(cb -> consortium.getViewContext().validate(cb))
                     .collect(Collectors.toMap(cb -> {
                         HashKey hash = new HashKey(Conversion.hashOf(cb.getBlock().toByteString()));
                         hashes.add(hash);
                         return hash;
                     }, cb -> cb));
        List<Long> gaps = noGaps(hashed, blockCache);
        if (!gaps.isEmpty()) {
            log.debug("Rejecting Sync from: {} regent: {} on: {} gaps in Sync log: {}", regent, regency,
                      consortium.getMember(), gaps);
            return false;
        }
        return true;
    }
}
