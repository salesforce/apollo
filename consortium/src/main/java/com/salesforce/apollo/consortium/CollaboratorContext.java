/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Body;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.Certification;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.CertifiedLog;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Header;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.JoinTransaction;
import com.salesfoce.apollo.consortium.proto.Proof;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.User;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesforce.apollo.consortium.Consortium.CurrentSync;
import com.salesforce.apollo.consortium.Consortium.Result;
import com.salesforce.apollo.consortium.Consortium.Timers;
import com.salesforce.apollo.consortium.TickScheduler.Timer;
import com.salesforce.apollo.consortium.TransactionSimulator.EvaluatedTransaction;
import com.salesforce.apollo.consortium.comms.ConsortiumClientCommunications;
import com.salesforce.apollo.consortium.fsm.Transitions;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * Context for the state machine. These are the leaf actions driven by the FSM.
 *
 */
public class CollaboratorContext {
    private static final Logger                        log               = LoggerFactory.getLogger(CollaboratorContext.class);
    private final NavigableMap<Long, CurrentBlock>     blockCache        = new ConcurrentSkipListMap<>();
    private final Consortium                           consortium;
    private volatile long                              currentConsensus  = -1;
    private volatile int                               currentRegent     = 0;
    private final Map<Integer, CurrentSync>            data              = new HashMap<>();
    private final Deque<CertifiedBlock>                decided           = new ArrayDeque<>();
    private volatile long                              lastBlock         = -1;
    private volatile int                               nextRegent        = -1;
    private final TransactionSimulator                 simulator;
    private final Deque<EnqueuedTransaction>           stopMessages      = new ArrayDeque<>();
    private volatile boolean                           stopped           = false;
    private final ProcessedBuffer                      processed;
    private final Map<Integer, Sync>                   sync              = new HashMap<>();
    private final Map<Timers, Timer>                   timers            = new ConcurrentHashMap<>();
    private final Map<HashKey, EnqueuedTransaction>    toOrder           = new ConcurrentHashMap<>();
    private final Map<Integer, Set<Member>>            wantRegencyChange = new HashMap<>();
    private final Map<HashKey, CertifiedBlock.Builder> workingBlocks     = new ConcurrentHashMap<>();

    CollaboratorContext(Consortium consortium) {
        this.consortium = consortium;
        processed = new ProcessedBuffer(consortium.getParams().processedBufferSize);
        simulator = new TransactionSimulator(consortium.getParams().maxBatchByteSize, this,
                consortium.getParams().validator);
    }

    public void awaitGenesis() {
        schedule(Timers.AWAIT_GENESIS, () -> {
            consortium.getTransitions().missingGenesis();
        }, consortium.getParams().getViewTimeoutTicks());
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
        if (nextRegent() == currentRegent()) {
            log.trace("Not changing regent as next/cur are identical: {} on: {}", currentRegent(),
                      consortium.getMember());
            return;
        }
        stopped = true;
        nextRegent(currentRegent() + 1);
        log.info("Starting change of regent from: {} to: {} on: {}", currentRegent(), nextRegent(),
                 consortium.getMember());
        toOrder.values().forEach(t -> t.cancel());
        Stop.Builder data = Stop.newBuilder().setNextRegent(nextRegent());
        transactions.forEach(eqt -> data.addTransactions(eqt.getTransaction()));
        Stop stop = data.build();
        consortium.publish(stop);
        consortium.getTransitions().deliverStop(stop, consortium.getMember());
    }

    public void deliverBlock(Block block, Member from) {
        if (block.getBody().getType() == BodyType.GENESIS) {
            deliverGenesisBlock(block, from);
            return;
        }
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        long previous = lastBlock();
        if (block.getHeader().getHeight() <= previous) {
            log.debug("Rejecting block proposal: {} from {} on: {} not next block: {} last: {}", hash, from,
                      consortium.getMember(), block.getHeader().getHeight(), previous);
            return;
        }
        workingBlocks.computeIfAbsent(hash, k -> {
            Validate validation = consortium.viewContext().generateValidation(hash, block);
            if (validation == null) {
                log.debug("Rejecting block proposal: {}, cannot validate from {} on: {}", hash, from,
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
        Set<Member> votes = wantRegencyChange.computeIfAbsent(data.getNextRegent(), k -> new HashSet<>());

        if (votes.size() >= consortium.viewContext().majority()) {
            log.trace("Ignoring stop, already established: {} from {} on: {} requesting: {}", data.getNextRegent(),
                      from, consortium.getMember(), votes.stream().map(m -> m.getId()).collect(Collectors.toList()));
            return;
        }
        log.debug("Delivering stop: {} from {} on: {}", data.getNextRegent(), from, consortium.getMember());
        votes.add(from);
        if (votes.size() >= consortium.viewContext().majority()) {
            consortium.getTransitions().startRegencyChange(stopMessages.stream().collect(Collectors.toList()));
            data.getTransactionsList()
                .stream()
                .map(tx -> new EnqueuedTransaction(Consortium.hashOf(tx), tx))
                .peek(eqt -> stopMessages.add(eqt))
                .filter(eqt -> !processed.contains(eqt.getHash()))
                .forEach(eqt -> {
                    toOrder.putIfAbsent(eqt.getHash(), eqt);
                });
            consortium.getTransitions().establishNextRegent();
        }
    }

    public void deliverStopData(StopData stopData, Member from) {
        int elected = stopData.getProof().getCurrentRegent();
        Member regent = getRegent(elected);
        if (!consortium.getMember().equals(regent)) {
            log.trace("ignoring StopData from {} on: {}, incorrect regent: {} (not this member)", from,
                      consortium.getMember(), regent);
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
        CurrentSync regencyData = data.computeIfAbsent(elected, k -> new Consortium.CurrentSync());

        if (regencyData.proofs.size() >= consortium.viewContext().majority()) {
            log.trace("ignoring StopData from {} on: {}, already majority for regency: {}", from,
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
        if (hashed.size() != stopData.getProof().getBlocksCount()) {
            log.trace("Ignoring StopData: {} from {} on: {} invalid proof", elected, from, consortium.getMember());
            return;
        }
        if (!Consortium.noGaps(hashed, new HashKey(stopData.getProof().getRoot()))) {
            log.trace("Ignoring StopData: {} from {} on: {} gaps in log", elected, from, consortium.getMember());
            return;
        }

        log.debug("Delivering StopData: {} from {} on: {}", elected, from, consortium.getMember());
        Set<HashKey> unresolved = hashes.stream()
                                        .filter(h -> !hashed.containsKey(h))
                                        .filter(h -> regencyData.blocks.containsKey(h))
                                        .collect(Collectors.toSet());
        if (unresolved.isEmpty() || unresolved.contains(consortium.getvState().getCurrent().getHash())) {
            regencyData.proofs.add(stopData.getProof());
            regencyData.signatures.add(stopData.getSignature());
            regencyData.blocks.putAll(hashed);
        }

        if (regencyData.proofs.size() >= consortium.viewContext().majority()) {
            Sync synch = buildSync(elected, regencyData);
            if (synch != null) {
                consortium.getTransitions().synchronizingLeader();
                consortium.getTransitions().deliverSync(synch, consortium.getMember());
                consortium.publish(synch);
            }
        }
    }

    public void deliverSync(Sync syncData, Member from) {
        CertifiedLog sLog = syncData.getLog();

        int cReg = sLog.getCurrentRegent();
        if (nextRegent() != cReg) {
            log.trace("Rejecting Sync from: {} on: {} expected regent: {} provided: {}", from, consortium.getMember(),
                      cReg, nextRegent());
            return;
        }
        Member regent = getRegent(cReg);
        if (!regent.equals(from)) {
            log.trace("Rejecting Sync from invalid regent: {} on: {} expected regent: {}", from, consortium.getMember(),
                      regent);
            return;
        }
        if (sync.get(cReg) != null) {
            log.trace("Rejecting Sync from: {} regent: {} on: {} consensus already proved", from, cReg,
                      consortium.getMember());
            return;
        }

        if (!validate(from, sLog, cReg)) {
            log.trace("Rejecting Sync from: {} regent: {} on: {} cannot verify Sync log", from, cReg,
                      consortium.getMember());
            return;
        }
        if (!SigningUtils.verify(from, syncData.getSignature().toByteArray(),
                                 Conversion.hashOf(syncData.getLog().toByteString()))) {
            log.trace("Rejecting Sync from: {} regent: {} on: {} cannot verify Sync log signature", from, cReg,
                      consortium.getMember());
            return;
        }
        log.debug("Delivering Sync from: {} regent: {} on: {}", from, cReg, consortium.getMember());
        sync.put(cReg, syncData);
        consortium.getTransitions().syncd();
        resolveRegentStatus();
    }

    public void drainBlocks() {
        cancel(Timers.FLUSH_BATCH);
        if (!simulator.isEmpty()) {
            generate();
            generateBlocks();
        }
    }

    public void establishGenesisView() {
        ViewContext newView = new ViewContext(Consortium.GENESIS_VIEW_ID, consortium.getParams().context,
                consortium.getMember(), consortium.nextViewConsensusKey(), Collections.emptyList(),
                consortium.entropy());
        consortium.viewChange(newView);
        if (consortium.viewContext().isMember()) {
            currentRegent(-1);
            nextRegent(-2);
            consortium.getvState().pause();
            consortium.joinMessageGroup(newView);
            consortium.getTransitions().generateView();
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
            consortium.getTransitions().deliverStopData(stopData, consortium.getMember());
        } else {
            ConsortiumClientCommunications link = consortium.linkFor(leader);
            if (link == null) {
                log.warn("Cannot get link to leader: {} on: {}", leader, consortium.getMember());
            } else {
                try {
                    link.stop(stopData);
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
        log.trace("starting block timeout: {} on: {}", consortium.getParams().getMaxBatchDelayTicks(),
                  consortium.getMember());
        scheduleIfAbsent(Timers.FLUSH_BATCH, () -> generateBlocks(), consortium.getParams().getMaxBatchDelayTicks());
    }

    public void generateView() {
        if (consortium.getvState().getCurrent() == null) {
            log.trace("Generating genesis view on: {}", consortium.getMember());
            generateGenesisView();
        } else {
            log.trace("Generating view on: {}", consortium.getMember());
            generateNextView();
        }
    }

    public void initializeConsensus() {
        currentConsensus(consortium.getvState().getCurrent().getBlock().getHeader().getHeight());
    }

    public void join() {
        log.debug("Petitioning to join view: {} on: {}", consortium.viewContext().getId(),
                  consortium.getParams().member);

        Join voteForMe = Join.newBuilder()
                             .setMember(consortium.getvState().getCurrent() == null
                                     ? consortium.viewContext().getView(consortium.getParams().signature.get())
                                     : consortium.getvState().getNextView())
                             .setContext(consortium.viewContext().getId().toByteString())
                             .build();
        List<Result> votes = consortium.viewContext()
                                       .streamRandomRing()
                                       .filter(m -> !m.equals(consortium.getParams().member))
                                       .map(c -> {
                                           ConsortiumClientCommunications link = consortium.linkFor(c);
                                           if (link == null) {
                                               log.warn("Cannot get link for {}", c.getId());
                                               return null;
                                           }
                                           JoinResult vote;
                                           try {
                                               vote = link.join(voteForMe);
                                           } catch (Throwable e) {
                                               log.trace("Unable to poll vote from: {}:{}", c, e.getMessage());
                                               return null;
                                           }

                                           log.trace("One vote to join: {} : {} from: {}",
                                                     consortium.getParams().member, consortium.viewContext().getId(),
                                                     c);
                                           return new Consortium.Result(c, vote);
                                       })
                                       .filter(r -> r != null)
                                       .filter(r -> r.vote.isInitialized())
                                       .limit(consortium.viewContext().majority())
                                       .collect(Collectors.toList());

        if (votes.size() < consortium.viewContext().majority()) {
            log.debug("Did not gather votes necessary to join consortium needed: {} got: {}",
                      consortium.viewContext().majority(), votes.size());
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

    public void nextView() {
    }

    public void processCheckpoint(CurrentBlock next) {
        Checkpoint body = checkpointBody(next.getBlock());
        if (body == null) {
            return;
        }
        log.info("Processing checkpoint block: {} on: {}", next.getHash(), consortium.getMember());
        body.getTransactionsList().forEach(txn -> {
            HashKey hash = new HashKey(txn.getHash());
            finalized(hash);
        });
        accept(next);
    }

    public void processGenesis(CurrentBlock next) {
        Genesis body = genesisBody(next.getBlock());
        if (body == null) {
            return;
        }
        log.info("Processing genesis block: {} on: {}", next.getHash(), consortium.getMember());
        accept(next);
        Reconfigure reconfigure = body.getInitialView();
        cancelToTimers();
        toOrder.clear();
        consortium.getSubmitted().clear();
        consortium.getTransitions().genesisAccepted();
        reconfigure(reconfigure, true);
    }

    public void processReconfigure(CurrentBlock next) {
        Reconfigure body = reconfigureBody(next.getBlock());
        if (body == null) {
            return;
        }
        log.info("Processing reconfigure block: {} on: {}", next.getHash(), consortium.getMember());
        accept(next);
        reconfigure(body, false);
    }

    public void processUser(CurrentBlock next) {
        User body = userBody(next.getBlock());
        if (body == null) {
            return;
        }
        log.info("Processing user block: {} on: {}", next.getHash(), consortium.getMember());
        body.getTransactionsList().forEach(txn -> {
            HashKey hash = new HashKey(txn.getHash());
            finalized(hash);
        });
        accept(next);
    }

    public void receive(ReplicateTransactions transactions, Member from) {
        for (Transaction txn : transactions.getTransactionsList()) {
            receive(txn);
        }
    }

    public void receive(Transaction txn) {
        EnqueuedTransaction transaction = new EnqueuedTransaction(Consortium.hashOf(txn), txn);
        if (processed.contains(transaction.getHash())) {
            return;
        }
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
                if (join.getRegency() != currentRegent()) {
                    log.debug("Discarding join: {} from: {} invalid regency: {} current: {} on: {}",
                              Consortium.hashOf(txn), memberID, join.getRegency(), currentRegent(),
                              consortium.getMember());
                    return null;
                }
                log.trace("Join txn:{} received on: {} self join: {}", transaction.getHash(), consortium.getMember(),
                          consortium.getMember().getId().equals(memberID));
            } else {
                log.trace("Client txn:{} received on: {} ", transaction.getHash(), consortium.getMember());
            }
            transaction.setTimer(schedule(transaction));
            return transaction;
        });
    }

    public void receiveJoin(Transaction txn) {
        if (!txn.getJoin()) {
            return;
        }
        receive(txn);
        reduceJoinTransactions();
    }

    public void receiveJoins(ReplicateTransactions transactions, Member from) {
        transactions.getTransactionsList().stream().filter(txn -> txn.getJoin()).forEach(txn -> receive(txn));
        reduceJoinTransactions();
    }

    public void reschedule(List<EnqueuedTransaction> transactions) {
        transactions.forEach(eqt -> {
            eqt.setTimedOut(false);
            eqt.setTimer(schedule(eqt));
        });
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

    public void validate(Validate v) {
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

    void clear() {
        currentRegent(0);
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

    int currentRegent() {
        final int c = currentRegent;
        return c;
    }

    void drainPending() {
        consortium.getTransitions().drainPending();
    }

    Map<HashKey, EnqueuedTransaction> getToOrder() {
        return toOrder;
    }

    private void accept(CurrentBlock next) {
        workingBlocks.remove(next.getHash());
        consortium.getvState().setCurrent(next);
        blockCache.put(next.getBlock().getHeader().getHeight(), next);
    }

    private StopData buildStopData(int currentRegent) {
        Map<HashKey, CertifiedBlock> decided;
        decided = workingBlocks.entrySet()
                               .stream()
                               .filter(e -> e.getValue().getCertificationsCount() >= consortium.viewContext()
                                                                                               .majority())
                               .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().build()));

        CurrentBlock current = consortium.getvState().getCurrent();
        HashKey last = HashKey.ORIGIN;
        if (current != null) {
            last = current.getHash();
        }
        if (!Consortium.noGaps(decided, last)) {
            log.debug("Have no valid log on: {}", consortium.getMember());
            return null;
        }
        Proof.Builder proofBuilder = Proof.newBuilder()
                                          .setCurrentRegent(currentRegent)
                                          .setContext(consortium.viewContext().getId().toByteString())
                                          .setId(consortium.getMember().getId().toByteString())
                                          .setRoot(last.toByteString());
        StopData.Builder builder = StopData.newBuilder();
        builder.setContext(consortium.viewContext().getId().toByteString());
        decided.entrySet().forEach(e -> {
            proofBuilder.addBlocks(e.getKey().toByteString());
            builder.addBlocks(e.getValue());
        });
        Proof proof = proofBuilder.build();
        byte[] signed = SigningUtils.sign(consortium.getParams().signature.get(),
                                          Conversion.hashOf(proof.toByteString()));
        if (signed == null) {
            log.error("Cannot sign StopData hashes on: {}", consortium.getMember());
            consortium.getTransitions().fail();
            return null;
        }
        builder.setProof(proof).setSignature(ByteString.copyFrom(signed));

        return builder.build();
    }

    private Sync buildSync(int elected, CurrentSync regencyData) {
        CurrentBlock current = consortium.getvState().getCurrent();
        HashKey hash = current != null ? current.getHash() : HashKey.ORIGIN;
        if (!Consortium.noGaps(regencyData.blocks, hash)) {
            log.debug("Cannot build sync on {}, gaps in log", consortium.getMember());
            return null;
        }
        CertifiedLog.Builder logBuilder = CertifiedLog.newBuilder().setCurrentRegent(elected);
        ArrayList<CertifiedBlock> blocks = new ArrayList<>(regencyData.blocks.values());
        blocks.forEach(cb -> logBuilder.addBlocks(cb));
        logBuilder.addAllProofs(regencyData.proofs);
        logBuilder.addAllSignatures(regencyData.signatures);
        CertifiedLog l = logBuilder.build();
        byte[] signed = SigningUtils.sign(consortium.getParams().signature.get(), Conversion.hashOf(l.toByteString()));
        if (signed == null) {
            log.error("Cannot sign Sync on {}", consortium.getMember());
            return null;
        }
        return Sync.newBuilder().setLog(l).setSignature(ByteString.copyFrom(signed)).build();
    }

    private void cancelToTimers() {
        toOrder.values().forEach(eqt -> eqt.cancel());
    }

    private boolean certify(CertifiedLog certifiedLog, Map<HashKey, CertifiedBlock> hashed, int regency,
                            Member regent) {
        for (int i = 0; i < certifiedLog.getProofsCount(); i++) {
            Proof p = certifiedLog.getProofs(i);
            ByteString sig = certifiedLog.getSignatures(i);
            HashKey memberID = new HashKey(p.getId());
            Member member = consortium.viewContext().getMember(memberID);
            if (member == null) {
                log.debug("Rejecting Sync from: {} regent: {} on: {} invalid member: {}", regent, regency,
                          consortium.getMember(), memberID);
            }
            if (p.getBlocksList()
                 .stream()
                 .filter(h -> !hashed.containsKey(new HashKey(h)))
                 .findFirst()
                 .orElse(null) != null) {
                log.debug("Rejecting Sync from: {} regent: {} on: {} invalid proof from: {}", regent, regency,
                          consortium.getMember(), memberID);
                return false;
            }
            if (!SigningUtils.verify(member, sig.toByteArray(), Conversion.hashOf(p.toByteString()))) {
                log.debug("Rejecting Sync from: {} regent: {} on: {} invalid signature of: {}", regent, regency,
                          consortium.getMember(), memberID);
            }
        }
        return true;
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
        final long c = currentConsensus;
        return c;
    }

    private void currentConsensus(long l) {
        this.currentConsensus = l;
    }

    private void currentRegent(int currentRegent) {
        log.trace("Current regency set to: {} previous: {} on: {} ", currentRegent, this.currentRegent,
                  consortium.getMember());
        this.currentRegent = currentRegent;
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
            processToOrder(genesis.getInitialView().getTransactionsList());
            Validate validation = consortium.viewContext().generateValidation(hash, block);
            if (validation == null) {
                log.error("Cannot validate generated genesis: {} on: {}", hash, consortium.getMember());
                return null;
            }
            lastBlock(block.getHeader().getHeight());
            consortium.getvState()
                      .setViewContext(consortium.viewContext().cloneWith(genesis.getInitialView().getViewList()));
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
        if (submittedTxn != null && submittedTxn.onCompletion != null) {
            log.info("Completing txn: {} on: {}", hash, consortium.getMember());
            ForkJoinPool.commonPool().execute(() -> submittedTxn.onCompletion.accept(hash));
        } else {
            log.debug("Processing txn: {} on: {}", hash, consortium.getMember());
        }
    }

    private void firstTimeout(EnqueuedTransaction transaction) {
        assert !transaction.isTimedOut() : "this should be unpossible";
        transaction.setTimedOut(true);
        log.debug("Replicating transaction: {} first timeout on: {}", transaction.getHash(), consortium.getMember());
        ReplicateTransactions.Builder builder = ReplicateTransactions.newBuilder()
                                                                     .addTransactions(transaction.getTransaction());
        toOrder.values()
               .stream()
               .filter(eqt -> eqt.getDelay() < consortium.viewContext().toleranceLevel())
               .peek(eqt -> eqt.cancel())
               .map(eqt -> eqt.getTransaction())
               .forEach(txn -> builder.addTransactions(txn));

        consortium.publish(builder.build());
        transaction.setTimer(consortium.getScheduler()
                                       .schedule(Timers.TRANSACTION_TIMEOUT_2, () -> secondTimeout(transaction),
                                                 consortium.viewContext().toleranceLevel()));
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
        log.debug("Generating genesis on {} join transactions: {}", consortium.getMember(), toOrder.keySet());
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
                               .setContents(consortium.compress(Genesis.newBuilder()
                                                                       .setGenesisData(ByteString.copyFrom(consortium.genesisData))
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

            consortium.getvState().setViewContext(consortium.viewContext().cloneWith(genesisView.getViewList()));
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
        if (toOrder.size() == consortium.viewContext().activeCardinality()) {
            generateGenesisBlock();
        } else {
            log.trace("Genesis group has not formed, rescheduling: {} want: {}", toOrder.size(),
                      consortium.viewContext().cardinality());
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
            log.trace("No transactions to generate block on: {}", consortium.getMember());
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
        log.info("Generating next block on: {} height: {} transactions: {}", consortium.getMember(), thisHeight,
                 processed);

        Body body = Body.newBuilder()
                        .setType(BodyType.USER)
                        .setContents(consortium.compress(user.build().toByteString()))
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
        if (attempt < 20 && selfJoin && toOrder.size() == consortium.viewContext().toleranceLevel()) {
            log.trace("View formed on: {} have: {} require: {} self join: {}", consortium.getMember(), toOrder.size(),
                      consortium.viewContext().majority(), selfJoin);
            consortium.getTransitions().formView();
        } else if (attempt >= 20 && selfJoin && toOrder.size() >= consortium.viewContext().majority()) {
            log.trace("View formed on: {} have: {} require: {} self join: {}", consortium.getMember(), toOrder.size(),
                      consortium.viewContext().majority(), selfJoin);
            consortium.getTransitions().formView();
        } else {
            log.trace("View has not been formed, rescheduling on: {} have: {} require: {} self join: {}",
                      consortium.getMember(), toOrder.size(), consortium.viewContext().majority(), selfJoin);
            join();
            schedule(Timers.AWAIT_VIEW_MEMBERS, () -> joinView(attempt + 1),
                     consortium.getParams().getJoinTimeoutTicks());
        }
    }

    private long lastBlock() {
        final long c = lastBlock;
        return c;
    }

    private void lastBlock(long lastBlock) {
        this.lastBlock = lastBlock;
    }

    private void nextBatch() {
        if (toOrder.isEmpty()) {
//            log.debug("No transactions available to batch on: {}:{}", consortium.getMember(), toOrder.size());
            return;
        }
        List<EnqueuedTransaction> batch = toOrder.values()
                                                 .stream()
                                                 .limit(Math.min(consortium.getParams().maxBatchByteSize,
                                                                 simulator.available()))
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
            log.info("submitting batch: {} for simulation on: {} txns: {}", batch.size(), consortium.getMember(),
                     batch.stream().map(eqt -> eqt.getHash()).collect(Collectors.toList()));
        }
    }

    private int nextRegent() {
        final int c = nextRegent;
        return c;
    }

    private void nextRegent(int nextRegent) {
        this.nextRegent = nextRegent;
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

    private void reconfigure(Reconfigure view, boolean genesis) {
        ViewContext newView = new ViewContext(view, consortium.getParams().context, consortium.getMember(),
                genesis ? consortium.viewContext().getConsensusKey() : consortium.nextViewConsensusKey(),
                consortium.entropy());
        currentRegent(genesis ? 1 : 0);
        nextRegent(-1);
        consortium.viewChange(newView);
        resolveStatus();
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
        toOrder.values().forEach(eqt -> {
            eqt.setTimedOut(false);
            eqt.setTimer(schedule(eqt));
        });
    }

    private void rescheduleGenesis() {
        schedule(Timers.AWAIT_GROUP, () -> {
            if (selfJoinRecorded() && toOrder.size() >= consortium.viewContext().majority()) {
                generateGenesisBlock();
            } else {
                log.trace("Genesis group has not formed, rescheduling: {} want: {}", toOrder.size(),
                          consortium.viewContext().majority());
                rescheduleGenesis();
            }
        }, consortium.getParams().getViewTimeoutTicks());
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

    private Timer schedule(EnqueuedTransaction eqt) {
        return consortium.getScheduler()
                         .schedule(Timers.TRANSACTION_TIMEOUT_1, () -> firstTimeout(eqt),
                                   eqt.getTransaction().getJoin() ? consortium.getParams().getJoinTimeoutTicks()
                                           : consortium.getParams().getSubmitTimeoutTicks());
    }

    private void schedule(Timers label, Runnable a, int delta) {
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
        Messenger messenger = consortium.getvState().getMessenger();
        int current = messenger == null ? 0 : messenger.getRound();
        Timer previous = timers.put(label, consortium.getScheduler().schedule(label, action, current + delta));
        if (previous != null) {
            log.trace("Cancelling previous timer for: {} on: {}", label, consortium.getMember());
            previous.cancel();
        }
        log.debug("Setting timer for: {} on: {}", label, consortium.getMember());
    }

    private void scheduleIfAbsent(Timers label, Runnable a, int delta) {
        Transitions timerState = consortium.fsm().getCurrentState();
        Runnable action = () -> {
            timers.remove(label);
            Transitions currentState = consortium.fsm().getCurrentState();
            if (timerState.equals(currentState)) {
                a.run();
            } else {
                log.debug("discarding timer scheduled on: {} but timed out: {} on: {}", timerState, currentState,
                          consortium.getMember());
            }
        };
        Messenger messenger = consortium.getvState().getMessenger();
        int current = messenger == null ? 0 : messenger.getRound();
        timers.computeIfAbsent(label, k -> {
            log.trace("Setting timer for: {} on: {}", label, consortium.getMember());
            return consortium.getScheduler().schedule(k, action, current + delta);
        });
    }

    private void secondTimeout(EnqueuedTransaction transaction) {
        log.debug("Second timeout for: {} on: {}", transaction.getHash(), consortium.getMember());
        List<EnqueuedTransaction> timedOut = toOrder.values()
                                                    .stream()
                                                    .filter(eqt -> eqt.getDelay() < 10)
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

    private boolean validate(Member regent, CertifiedLog certifiedLog, int regency) {
        Map<HashKey, CertifiedBlock> hashed;
        List<HashKey> hashes = new ArrayList<>();
        hashed = certifiedLog.getBlocksList().stream().collect(Collectors.toMap(cb -> {
            HashKey hash = new HashKey(Conversion.hashOf(cb.getBlock().toByteString()));
            hashes.add(hash);
            return hash;
        }, cb -> cb));

        CurrentBlock current = consortium.getvState().getCurrent();
        HashKey hash = current != null ? current.getHash() : HashKey.ORIGIN;
        if (!Consortium.noGaps(hashed, hash)) {
            log.debug("Rejecting Sync from: {} regent: {} on: {} gaps in Sync log", regent, regency,
                      consortium.getMember());
            return false;
        }

        if (!certify(certifiedLog, hashed, regency, regent)) {
            return false;
        }

        if (certifiedLog.getProofsCount() < consortium.viewContext().majority()) {
            log.debug("Rejecting Sync from: {} regent: {} on: {} consensus not proved: {} required: {}", regent,
                      regency, consortium.getMember(), certifiedLog.getProofsCount(),
                      consortium.viewContext().majority());
            return false;
        }
        Set<HashKey> unresolved = hashes.stream().filter(h -> !hashed.containsKey(h)).collect(Collectors.toSet());
        if (unresolved.isEmpty() || unresolved.contains(hash)) {
            return true;
        }
        return false;
    }
}
