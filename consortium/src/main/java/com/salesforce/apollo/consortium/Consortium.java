/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.SigningUtils.generateKeyPair;
import static com.salesforce.apollo.consortium.SigningUtils.sign;
import static com.salesforce.apollo.consortium.SigningUtils.validateGenesis;
import static com.salesforce.apollo.consortium.SigningUtils.verify;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.protobuf.Any;
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
import com.salesfoce.apollo.consortium.proto.Persist;
import com.salesfoce.apollo.consortium.proto.Proof;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.Stop.Builder;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.TotalOrdering;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.TransactionOrBuilder;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesfoce.apollo.consortium.proto.User;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesfoce.apollo.consortium.proto.ViewMember;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.TickScheduler.Timer;
import com.salesforce.apollo.consortium.TransactionSimulator.EvaluatedTransaction;
import com.salesforce.apollo.consortium.comms.ConsortiumClientCommunications;
import com.salesforce.apollo.consortium.comms.ConsortiumServerCommunications;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
import com.salesforce.apollo.consortium.fsm.Transitions;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.MemberOrder;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.membership.messaging.Messenger.MessageChannelHandler.Msg;
import com.salesforce.apollo.protocols.BbBackedInputStream;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Consortium {

    /**
     * Context for the state machine. These are the leaf actions driven by the FSM.
     *
     */
    @SuppressWarnings("unused")
    public class CollaboratorContext {
        private final NavigableMap<Long, CurrentBlock>     blockCache        = new ConcurrentSkipListMap<>();
        private volatile int                               currentConsensus  = -1;
        private volatile int                               currentRegent;
        private final Map<Integer, CurrentSync>            data              = new HashMap<>();
        private final Deque<CertifiedBlock>                decided           = new ArrayDeque<>();
        private volatile long                              lastBlock         = -1;
        private volatile int                               nextRegent;
        private final TransactionSimulator                 simulator;
        private final Deque<EnqueuedTransaction>           stopMessages      = new ArrayDeque<>();
        private volatile boolean                           stopped           = false;
        private final Map<Integer, Sync>                   sync              = new HashMap<>();
        private final Map<Timers, Timer>                   timers            = new ConcurrentHashMap<>();
        private final Map<HashKey, EnqueuedTransaction>    toOrder           = new HashMap<>();
        private final Map<Integer, Set<Member>>            wantRegencyChange = new HashMap<>();
        private final Map<HashKey, CertifiedBlock.Builder> workingBlocks     = new HashMap<>();

        public CollaboratorContext() {
            simulator = new TransactionSimulator(params.maxBatchByteSize, this, params.validator);
        }

        public void awaitGenesis() {
            schedule(Timers.AWAIT_GENESIS, () -> {
                transitions.missingGenesis();
            }, params.context.toleranceLevel());
        }

        public void becomeLeader() {
            scheduleBlockTimeout();
        }

        public void cancel(Timers t) {
            Timer timer = timers.remove(t);
            if (timer != null) {
                log.trace("Cancelling timer: {} on: {}", t, getMember());
                timer.cancel();
            } else {
                log.trace("No timer to cancel: {} on: {}", t, getMember());
            }
        }

        public void changeRegency(List<EnqueuedTransaction> transactions) {
            if (nextRegent != currentRegent) {
                return;
            }
            stopped = true;
            nextRegent = currentRegent + 1;
            toOrder.values().forEach(t -> t.cancel());
            Builder data = Stop.newBuilder().setNextRegent(nextRegent);
            transactions.forEach(eqt -> data.addTransactions(eqt.getTransaction()));
            Stop stop = data.build();
            deliver(stop);
            transitions.deliverStop(stop, getMember());
        }

        public void deliverBlock(Block block, Member from) {
            HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
            long previous = lastBlock;
            if (block.getHeader().getHeight() <= previous) {
                log.debug("Rejecting block proposal: {} from {} on: {} not next block: {} last: {}", hash, from,
                          getMember(), block.getHeader().getHeight(), previous);
                return;
            }
            workingBlocks.computeIfAbsent(hash, k -> {
                Validate validation = viewContext().generateValidation(hash, block);
                if (validation == null) {
                    log.debug("Rejecting block proposal: {}, cannot validate from {} on: {}", hash, from, getMember());
                    return null;
                }
                lastBlock = block.getHeader().getHeight();
                deliver(validation);
                return CertifiedBlock.newBuilder()
                                     .setBlock(block)
                                     .addCertifications(Certification.newBuilder()
                                                                     .setId(validation.getId())
                                                                     .setSignature(validation.getSignature()));
            });
        }

        public void deliverGenesisBlock(final Block block, Member from) {
            HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
            final long previous = lastBlock;
            if (block.getHeader().getHeight() != previous + 1) {
                log.debug("Rejecting genesis block proposal: {} from {}, not block height {} + 1", hash, from,
                          previous);
            }

            if (block.getBody().getType() != BodyType.GENESIS) {
                log.error("Failed on {} [{}] prev: [{}] delivering genesis block: {} invalid body: {}", getMember(),
                          fsm.prettyPrint(fsm.getCurrentState()), fsm.prettyPrint(fsm.getPreviousState()), hash,
                          block.getBody().getType());
                return;
            }
            workingBlocks.computeIfAbsent(hash, k -> {
                final Genesis genesis;
                try {
                    final InputStream body = getBody(block);
                    genesis = Genesis.parseFrom(body);
                } catch (IOException e) {
                    log.error("Cannot deserialize genesis block: {} from {} on: {}", hash, from, getMember(), e);
                    return null;
                }
                vState.setViewContext(viewContext().cloneWith(genesis.getInitialView().getViewList()));
                Validate validation = viewContext().generateValidation(hash, block);
                if (validation == null) {
                    log.error("Cannot validate generated genesis: {} on: {}", hash, getMember());
                    return null;
                }
                lastBlock = block.getHeader().getHeight();
                deliver(validation);
                return CertifiedBlock.newBuilder()
                                     .setBlock(block)
                                     .addCertifications(Certification.newBuilder()
                                                                     .setId(validation.getId())
                                                                     .setSignature(validation.getSignature()));
            });
        }

        public void deliverStop(Stop data, Member from) {
            if (data.getNextRegent() != currentRegent + 1) {
                log.info("Ignoring stop: {} current: {} from {} on: {}", data.getNextRegent(), currentRegent, from,
                         getMember());
                return;
            }
            log.info("Delivering stop: {} from {} on: {}", data.getNextRegent(), from, getMember());
            List<EnqueuedTransaction> enqueued = data.getTransactionsList()
                                                     .stream()
                                                     .map(tx -> new EnqueuedTransaction(hashOf(tx), tx))
                                                     .peek(eqt -> stopMessages.add(eqt))
                                                     .collect(Collectors.toList());
            Set<Member> votes = wantRegencyChange.computeIfAbsent(data.getNextRegent(), k -> new HashSet<>());
            votes.add(from);
            if (votes.size() > params.context.toleranceLevel()) {
                transitions.startRegencyChange(stopMessages.stream().collect(Collectors.toList()));
                enqueued.forEach(eqt -> {
                    toOrder.put(eqt.getHash(), eqt);
                });
                if (votes.size() > 2 * params.context.toleranceLevel()) {
                    transitions.establishNextRegent();
                }
            }
        }

        public void deliverStopData(StopData stopData, Member from) {
            int elected = stopData.getProof().getCurrentRegent();
            Member regent = getRegent(elected);
            if (!getMember().equals(regent)) {
                log.info("ignoring StopData from {} on: {}, incorrect regent: {} (not this member)", from, getMember(),
                         regent);
                return;
            }
            CurrentSync regencyData = data.computeIfAbsent(elected, k -> new CurrentSync());
            Map<HashKey, CertifiedBlock> hashed;
            List<HashKey> hashes = new ArrayList<>();
            hashed = stopData.getBlocksList().stream().collect(Collectors.toMap(cb -> {
                HashKey hash = new HashKey(Conversion.hashOf(cb.getBlock().toByteString()));
                hashes.add(hash);
                return hash;
            }, cb -> cb));
            if (hashed.size() != stopData.getProof().getBlocksCount()) {
                log.info("Ignoring StopData from {} on: {} invalid proof", from, getMember());
                return;
            }
            if (!noGaps(hashed, new HashKey(stopData.getProof().getRoot()))) {
                log.info("Ignoring StopData from {} on: {} gaps in log", from, getMember());
                return;
            }
            Set<HashKey> unresolved = hashes.stream()
                                            .filter(h -> !hashed.containsKey(h))
                                            .filter(h -> regencyData.blocks.containsKey(h))
                                            .collect(Collectors.toSet());
            if (unresolved.isEmpty() || unresolved.contains(vState.getCurrent().getHash())) {
                regencyData.proofs.add(stopData.getProof());
                regencyData.signatures.add(stopData.getSignature());
                regencyData.blocks.putAll(hashed);
            }

            if (regencyData.proofs.size() >= viewContext().cardinality() - viewContext().toleranceLevel()) {
                Sync synch = buildSync(elected, regencyData);
                if (synch != null) {
                    transitions.deliverSync(synch, getMember());
                    deliver(synch);
                }
            }
        }

        public void deliverSync(Sync syncData, Member from) {
            CertifiedLog sLog = syncData.getLog();

            int cReg = sLog.getCurrentRegency();
            if (nextRegent != cReg) {
                log.info("Rejecting Sync from: {} on: {} expected regent: {} provided: {}", from, getMember(), cReg,
                         nextRegent);
                return;
            }
            Member regent = getRegent(cReg);
            if (!regent.equals(from)) {
                log.info("Rejecting Sync from invalid regent: {} on: {} expected regent: {}", from, getMember(),
                         regent);
                return;
            }
            if (sync.get(cReg) != null) {
                log.info("Rejecting Sync from: {} regent: {} on: {} consensus already proved", from, cReg, getMember());
                return;
            }

            if (!validate(from, sLog, cReg)) {
                log.info("Rejecting Sync from: {} regent: {} on: {} cannot verify Sync log", from, cReg, getMember());
                return;
            }
            if (!SigningUtils.verify(from, syncData.getSignature().toByteArray(),
                                     Conversion.hashOf(syncData.getLog().toByteString()))) {
                log.info("Rejecting Sync from: {} regent: {} on: {} cannot verify Sync log signature", from, cReg,
                         getMember());
                return;
            }
            sync.put(cReg, syncData);
            transitions.syncd();
        }

        public void deliverTotalOrdering(TotalOrdering msg, Member from) {
            HashKey hash = new HashKey(msg.getHash());
            CertifiedBlock.Builder cb = workingBlocks.remove(hash);
            if (cb == null) {
                log.info("No block matching total order of: {} next cid: {} from: {} on: {}", hash,
                         msg.getNextConsensusId(), from, getMember());
            }
        }

        public void drainBlocks() {
            cancel(Timers.FLUSH_BATCH);
            if (!simulator.isEmpty()) {
                boolean generated = generateNextBlock();
                while (generated) {
                    generated = generateNextBlock();
                }
                scheduleBlockTimeout();
            }
        }

        public void enterView() {
            if (viewContext().isViewMember()) {
                transitions.joinAsMember();
            } else {
                transitions.join();
            }
        }

        public void establishGenesisView() {
            ViewContext newView = new ViewContext(GENESIS_VIEW_ID, params.context, getMember(), nextViewConsensusKey(),
                    Collections.emptyList(), entropy());
            viewChange(newView);
            if (viewContext().isMember()) {
                vState.pause();
                joinMessageGroup(newView);
                transitions.join();
            }
        }

        public void establishNextRegent() {
            if (currentRegent == nextRegent) {
                log.trace("Regent already established on {}", getMember());
                return;
            }
            currentRegent = nextRegent;
            reschedule();
            Member leader = getRegent(currentRegent);
            StopData stopData = buildStopData(currentRegent);
            if (stopData == null) {
                return;
            }
            if (getMember().equals(leader)) {
                transitions.deliverStopData(stopData, getMember());
            } else {
                ConsortiumClientCommunications link = linkFor(leader);
                if (link == null) {
                    log.warn("Cannot get link to leader: {} on: {}", leader, getMember());
                } else {
                    try {
                        link.stop(stopData);
                    } finally {
                        link.release();
                    }
                }
            }
        }

        public void generateGenesis() {
            if (toOrder.size() == viewContext().cardinality()) {
                generateGenesisBlock();
            } else {
                log.trace("Genesis group has not formed, rescheduling: {} want: {}", toOrder.size(),
                          viewContext().cardinality());
                rescheduleGenesis();
            }
        }

        public void join() {
            log.debug("Petitioning to join view: {} on: {}", viewContext().getId(), params.member);

            Join voteForMe = Join.newBuilder()
                                 .setMember(vState.getNextView())
                                 .setContext(viewContext().getId().toByteString())
                                 .build();
            List<Result> votes = viewContext().streamRandomRing().filter(m -> !m.equals(params.member)).map(c -> {
                ConsortiumClientCommunications link = linkFor(c);
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

                log.trace("One vote to join: {} : {} from: {}", params.member, viewContext().getId(), c);
                return new Result(c, vote);
            })
                                              .filter(r -> r != null)
                                              .filter(r -> r.vote.isInitialized())
                                              .limit(params.context.toleranceLevel() + 1)
                                              .collect(Collectors.toList());

            if (votes.size() <= params.context.toleranceLevel()) {
                log.debug("Did not gather votes necessary to join consortium needed: {} got: {}",
                          params.context.toleranceLevel() + 1, votes.size());
                return;
            }
            JoinTransaction.Builder txn = JoinTransaction.newBuilder().setMember(voteForMe.getMember());
            for (Result vote : votes) {
                txn.addCertification(Certification.newBuilder()
                                                  .setId(vote.member.getId().toByteString())
                                                  .setSignature(vote.vote.getSignature()));
            }
            JoinTransaction joinTxn = txn.build();
            try {
                submit(true, h -> {
                }, Any.pack(joinTxn));
            } catch (TimeoutException e) {
                transitions.fail();
                return;
            }
            log.info("Successfully petitioned to joined view: {} on: {}", viewContext().getId(), params.member);
        }

        public void joinView() {
            if (toOrder.size() > viewContext().toleranceLevel()) {
                transitions.formView();
            } else {
                join();
                log.trace("View has not been formed, rescheduling on: {} have: {} require: {}", getMember(),
                          toOrder.size(), viewContext().toleranceLevel() + 1);
                schedule(Timers.AWAIT_VIEW_MEMBERS, () -> joinView(), viewContext().timeToLive());
            }
        }

        public void nextView() {
        }

        public void processCheckpoint(CurrentBlock next) {
            Checkpoint body;
            try {
                body = Checkpoint.parseFrom(getBody(next.getBlock()));
            } catch (IOException e) {
                log.debug("Protocol violation on: {}.  Cannot decode checkpoint body: {}", getMember(), e);
                return;
            }
            body.getTransactionsList().forEach(txn -> {
                HashKey hash = new HashKey(txn.getHash());
                finalized(hash);
            });
            accept(next);
        }

        public void processGenesis(CurrentBlock next) {
            Genesis body;
            try {
                body = Genesis.parseFrom(getBody(next.getBlock()));
            } catch (IOException e) {
                log.debug("Protocol violation on {}.  Cannot decode genesis body: {}", getMember(), e);
                return;
            }
            accept(next);
            Reconfigure reconfigure = body.getInitialView();
            reconfigure.getTransactionsList().forEach(txn -> Consortium.this.finalize(txn));
            cancelToTimers();
            toOrder.clear();
            submitted.clear();
            transitions.genesisAccepted();
            reconfigure(reconfigure);
        }

        public void processReconfigure(CurrentBlock next) {
            Reconfigure body;
            try {
                body = Reconfigure.parseFrom(getBody(next.getBlock()));
            } catch (IOException e) {
                log.debug("Protocol violation on: {}.  Cannot decode reconfiguration body: {}", getMember(), e);
                return;
            }
            body.getTransactionsList().forEach(txn -> {
                HashKey hash = new HashKey(txn.getHash());
                finalized(hash);
            });
            accept(next);
            reconfigure(body);
        }

        public void processUser(CurrentBlock next) {
            User body;
            try {
                body = User.parseFrom(getBody(next.getBlock()));
            } catch (IOException e) {
                log.debug("Protocol violation on: {}.  Cannot decode reconfiguration body: {}", getMember(),
                          next.getHash(), e);
                return;
            }
            body.getTransactionsList().forEach(txn -> {
                HashKey hash = new HashKey(txn.getHash());
                finalized(hash);
                SubmittedTransaction submittedTxn = submitted.get(hash);
                if (submittedTxn != null && submittedTxn.onCompletion != null) {
                    log.info("Completing txn: {} on: {}", hash, getMember());
                    ForkJoinPool.commonPool().execute(() -> submittedTxn.onCompletion.accept(hash));
                } else {
                    log.debug("Processing txn: {} on: {}", hash, getMember());
                }
            });
            accept(next);
        }

        public void quiesce() {
            log.debug("Quiescing group messaging on: {}", getMember());
            vState.pause();
        }

        public void receive(ReplicateTransactions transactions, Member from) {
            for (Transaction txn : transactions.getTransactionsList()) {
                receive(txn);
            }
        }

        public void receive(Transaction txn) {
            EnqueuedTransaction transaction = new EnqueuedTransaction(hashOf(txn), txn);
            if (toOrder.put(transaction.getHash(), transaction) == null) {
                transaction.setTimer(schedule(transaction));
            }
        }

        public void scheduleBlockTimeout() {
            scheduleIfAbsent(Timers.FLUSH_BATCH, () -> {
                if (!simulator.isEmpty()) {
                    boolean generated = generateNextBlock();
                    while (generated) {
                        generated = generateNextBlock();
                    }
                    scheduleBlockTimeout();
                }
            }, params.maxBatchDelay);
        }

        public void shutdown() {
            stop();
        }

        public void totalOrderDeliver() {
            log.debug("Attempting total ordering of working blocks: {} on: {}", workingBlocks.size(), getMember());
            List<HashKey> published = new ArrayList<>();
            workingBlocks.entrySet()
                         .stream()
                         .peek(e -> log.trace("TO Consider: {}:{} on: {}", e.getKey(),
                                              e.getValue().getCertificationsCount(), getMember()))
                         .filter(e -> e.getValue().getCertificationsCount() > params.context.toleranceLevel())
                         .sorted((a, b) -> Long.compare(a.getValue().getBlock().getHeader().getHeight(),
                                                        b.getValue().getBlock().getHeader().getHeight()))
                         .forEach(e -> {
                             log.info("Totally ordering block: {} height: {} on: {}", e.getKey(),
                                      e.getValue().getBlock().getHeader().getHeight(), getMember());
                             params.consensus.apply(((CertifiedBlock.Builder) e.getValue()).build());
                             published.add(e.getKey());
                             deliver(TotalOrdering.newBuilder().setHash(e.getKey().toByteString()).build());
                         });
            published.forEach(h -> workingBlocks.remove(h));
        }

        public void validate(Validate v) {
            HashKey hash = new HashKey(v.getHash());
            CertifiedBlock.Builder certifiedBlock = workingBlocks.get(hash);
            if (certifiedBlock == null) {
                log.debug("No working block to validate: {} on: {}", hash, getMember());
                return;
            }
            HashKey memberID = new HashKey(v.getId());
            if (viewContext().validate(certifiedBlock.getBlock(), v)) {
                certifiedBlock.addCertifications(Certification.newBuilder()
                                                              .setId(v.getId())
                                                              .setSignature(v.getSignature()));
                log.debug("Adding block validation: {} from: {} on: {} count: {}", hash, memberID, getMember(),
                          certifiedBlock.getCertificationsCount());
            } else {
                log.debug("Failed block validation: {} from: {} on: {}", hash, memberID, getMember());
            }
        }

        void drainPending() {
            transitions.drainPending();
        }

        private void accept(CurrentBlock next) {
            workingBlocks.remove(next.getHash());
            vState.setCurrent(next);
            blockCache.put(next.getBlock().getHeader().getHeight(), next);
        }

        private StopData buildStopData(int currentRegent) {
            Map<HashKey, CertifiedBlock> decided;
            decided = workingBlocks.entrySet()
                                   .stream()
                                   .filter(e -> e.getValue().getCertificationsCount() <= toleranceLevel())
                                   .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().build()));

            CurrentBlock current = vState.getCurrent();
            HashKey last = HashKey.ORIGIN;
            if (current != null) {
                last = current.getHash();
            }
            if (!noGaps(decided, last)) {
                log.info("Have no valid log on: {}", getMember());
                return null;
            }
            Proof.Builder proofBuilder = Proof.newBuilder()
                                              .setCurrentRegent(currentRegent)
                                              .setContext(viewContext().getId().toByteString())
                                              .setId(getMember().getId().toByteString())
                                              .setRoot(last.toByteString());
            StopData.Builder builder = StopData.newBuilder();
            builder.setContext(viewContext().getId().toByteString());
            decided.entrySet().forEach(e -> {
                proofBuilder.addBlocks(e.getKey().toByteString());
                builder.addBlocks(e.getValue());
            });
            Proof proof = proofBuilder.build();
            byte[] signed = SigningUtils.sign(params.signature.get(), Conversion.hashOf(proof.toByteString()));
            if (signed == null) {
                log.error("Cannot sign StopData hashes on: {}", getMember());
                transitions.fail();
                return null;
            }
            builder.setProof(proof).setSignature(ByteString.copyFrom(signed));

            return builder.build();
        }

        private Sync buildSync(int elected, CurrentSync regencyData) {
            CurrentBlock current = vState.getCurrent();
            HashKey hash = current != null ? current.getHash() : HashKey.ORIGIN;
            if (!noGaps(regencyData.blocks, hash)) {
                log.info("Cannot build sync on {}, gaps in log", getMember());
                return null;
            }
            CertifiedLog.Builder logBuilder = CertifiedLog.newBuilder().setCurrentRegency(elected);
            ArrayList<CertifiedBlock> blocks = new ArrayList<>(regencyData.blocks.values());
            blocks.forEach(cb -> logBuilder.addBlocks(cb));
            logBuilder.addAllProofs(regencyData.proofs);
            logBuilder.addAllSignatures(regencyData.signatures);
            CertifiedLog l = logBuilder.build();
            byte[] signed = SigningUtils.sign(params.signature.get(), Conversion.hashOf(l.toByteString()));
            if (signed == null) {
                log.error("Cannot sign Sync on {}", getMember());
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
                Member member = viewContext().getMember(memberID);
                if (member == null) {
                    log.info("Rejecting Sync from: {} regent: {} on: {} invalid member: {}", regent, regency,
                             getMember(), memberID);
                }
                if (p.getBlocksList()
                     .stream()
                     .filter(h -> !hashed.containsKey(new HashKey(h)))
                     .findFirst()
                     .orElse(null) != null) {
                    log.info("Rejecting Sync from: {} regent: {} on: {} invalid proof from: {}", regent, regency,
                             getMember(), memberID);
                    return false;
                }
                if (!SigningUtils.verify(member, sig.toByteArray(), Conversion.hashOf(p.toByteString()))) {
                    log.info("Rejecting Sync from: {} regent: {} on: {} invalid signature of: {}", regent, regency,
                             getMember(), memberID);
                }
            }
            return true;
        }

        private void clear() {
            timers.values().forEach(e -> e.cancel());
            timers.clear();
            cancelToTimers();
            toOrder.clear();
            lastBlock = -1;
            scheduler.cancelAll();
            workingBlocks.clear();
        }

        private void finalized(HashKey hash) {
            EnqueuedTransaction removed = toOrder.remove(hash);
            if (removed != null) {
                removed.cancel();
            }
        }

        private void firstTimeout(EnqueuedTransaction transaction) {
            assert !transaction.isTimedOut() : "this should be unpossible";
            transaction.setTimedOut(true);
            log.info("Replicating transaction: {} first timeout on: {}", transaction.getHash(), getMember());
            ReplicateTransactions.Builder builder = ReplicateTransactions.newBuilder()
                                                                         .addTransactions(transaction.getTransaction());
            toOrder.values()
                   .stream()
                   .filter(eqt -> eqt.getDelay() < toleranceLevel())
                   .peek(eqt -> eqt.cancel())
                   .map(eqt -> eqt.getTransaction())
                   .forEach(txn -> builder.addTransactions(txn));

            deliver(builder.build());
            transaction.setTimer(scheduler.schedule(Timers.TRANSACTION_TIMEOUT_2, () -> secondTimeout(transaction),
                                                    viewContext().toleranceLevel()));
        }

        private void generateGenesisBlock() {
            Block block = Consortium.this.generateGenesis(toOrder, genesisData);
            HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
            workingBlocks.computeIfAbsent(hash, k -> {
                lastBlock = 0;
                deliver(block);
                Validate validation = generateValidationFromNextView(hash, block);
                if (validation == null) {
                    log.error("Cannot validate generated genesis block: {} on: {}", hash, getMember());
                }
                CertifiedBlock.Builder builder = CertifiedBlock.newBuilder().setBlock(block);
                builder.addCertifications(Certification.newBuilder()
                                                       .setId(validation.getId())
                                                       .setSignature(validation.getSignature()));
                deliver(validation);
                return builder;
            });
        }

        private boolean generateNextBlock() {
            final long currentHeight = lastBlock;
            final CurrentBlock currentBlock = blockCache.get(currentHeight);
            final long thisHeight = currentHeight + 1;

            if (currentBlock == null) {
                log.debug("Cannot generate next block: {} on: {}, as previous block for height: {} not found",
                          thisHeight, getMember(), currentHeight);
                return false;
            }

            EvaluatedTransaction txn = simulator.poll();
            if (txn == null) {
                log.trace("No transactions to generate block on: {}", getMember());
                return false;
            }

            User.Builder user = User.newBuilder();
            int processedBytes = 0;
            List<HashKey> processed = new ArrayList<>();

            do {
                processedBytes += txn.getSerializedSize();
                user.addTransactions(ExecutedTransaction.newBuilder()
                                                        .setHash(txn.transaction.getHash().toByteString())
                                                        .setTransaction(txn.transaction.getTransaction()))
                    .addResponses(txn.result);
                processed.add(txn.transaction.getHash());
                txn = simulator.poll();
            } while (txn != null && processed.size() <= params.maxBatchByteSize
                    && processedBytes <= params.maxBatchByteSize);

            if (processed.size() == 0) {
                log.debug("No transactions to generate block on: {}", getMember());
                return false;
            }
            log.debug("Generating next block on: {} height: {} transactions: {}", getMember(), thisHeight,
                      processed.size());

            Body body = Body.newBuilder()
                            .setType(BodyType.USER)
                            .setContents(compress(user.build().toByteString()))
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
            deliver(block);
            lastBlock = thisHeight;

            Validate validation = viewContext().generateValidation(hash, block);
            if (validation == null) {
                log.debug("Cannot generate validation for block: {} on: {}", hash, getMember());
                return false;
            }
            builder.addCertifications(Certification.newBuilder()
                                                   .setId(validation.getId())
                                                   .setSignature(validation.getSignature()));
            deliver(validation);

            log.info("Generated next block: {} height: {} on: {} txns: {}", hash, thisHeight, getMember(),
                     user.getTransactionsCount());
            return true;
        }

        private Member getRegent(int regent) {
            return viewContext().getRegent(regent);
        }

        private void reconfigure(Reconfigure view) {
            ViewContext newView = new ViewContext(view, params.context, getMember(), nextViewConsensusKey(), entropy());
            viewChange(newView);
            if (newView.isMember()) { // cohort member
                log.debug("reconfiguring, becoming joining member: {}", params.member);
                transitions.join();
            } else { // you are all my puppets
                log.debug("reconfiguring, becoming client: {}", params.member);
                transitions.becomeClient();
            }
        }

        private void reschedule() {
            toOrder.values().forEach(eqt -> {
                eqt.setTimedOut(false);
                eqt.setTimer(schedule(eqt));
            });
        }

        private void rescheduleGenesis() {
            schedule(Timers.AWAIT_GROUP, () -> {
                if (toOrder.size() > params.context.toleranceLevel()) {
                    generateGenesisBlock();
                } else {
                    log.trace("Genesis group has not formed, rescheduling: {} want: {}", toOrder.size(),
                              params.context.toleranceLevel());
                    rescheduleGenesis();
                }
            }, viewContext().toleranceLevel());
        }

        private Timer schedule(EnqueuedTransaction eqt) {
            return scheduler.schedule(Timers.TRANSACTION_TIMEOUT_1, () -> firstTimeout(eqt),
                                      5 * viewContext().timeToLive());
        }

        private void schedule(Timers label, Runnable a, int delta) {
            Transitions timerState = fsm.getCurrentState();
            Runnable action = () -> {
                timers.remove(label);
                Transitions currentState = fsm.getCurrentState();
                if (timerState.equals(currentState)) {
                    a.run();
                } else {
                    log.info("discarding timer scheduled on: {} but timed out: {} on: {}", timerState, currentState,
                             getMember());
                }
            };
            Messenger messenger = vState.getMessenger();
            int current = messenger == null ? 0 : messenger.getRound();
            Timer previous = timers.put(label, scheduler.schedule(label, action, current + delta));
            if (previous != null) {
                log.trace("Cancelling previous timer for: {} on: {}", label, getMember());
                previous.cancel();
            }
            log.trace("Setting timer for: {} on: {}", label, getMember());
        }

        private void scheduleIfAbsent(Timers label, Runnable a, int delta) {
            Transitions timerState = fsm.getCurrentState();
            Runnable action = () -> {
                timers.remove(label);
                Transitions currentState = fsm.getCurrentState();
                if (timerState.equals(currentState)) {
                    a.run();
                } else {
                    log.info("discarding timer scheduled on: {} but timed out: {} on: {}", timerState, currentState,
                             getMember());
                }
            };
            Messenger messenger = vState.getMessenger();
            int current = messenger == null ? 0 : messenger.getRound();
            timers.computeIfAbsent(label, k -> {
                log.trace("Setting timer for: {} on: {}", label, getMember());
                return scheduler.schedule(k, action, current + delta);
            });
        }

        private void secondTimeout(EnqueuedTransaction transaction) {
            log.info("Second timeout for: {} on: {}", transaction.getHash(), getMember());
            List<EnqueuedTransaction> timedOut = toOrder.values()
                                                        .stream()
                                                        .filter(eqt -> eqt.getDelay() < viewContext().timeToLive())
                                                        .peek(eqt -> eqt.cancel())
                                                        .collect(Collectors.toList());
            transitions.startRegencyChange(timedOut);
        }

        private boolean validate(Member regent, CertifiedLog certifiedLog, int regency) {
            Map<HashKey, CertifiedBlock> hashed;
            List<HashKey> hashes = new ArrayList<>();
            hashed = certifiedLog.getBlocksList().stream().collect(Collectors.toMap(cb -> {
                HashKey hash = new HashKey(Conversion.hashOf(cb.getBlock().toByteString()));
                hashes.add(hash);
                return hash;
            }, cb -> cb));

            CurrentBlock current = vState.getCurrent();
            HashKey hash = current != null ? current.getHash() : HashKey.ORIGIN;
            if (!noGaps(hashed, hash)) {
                log.info("Rejecting Sync from: {} regent: {} on: {} gaps in Sync log", regent, regency, getMember());
                return false;
            }

            if (!certify(certifiedLog, hashed, regency, regent)) {
                return false;
            }

            if (certifiedLog.getProofsCount() <= toleranceLevel()) {
                log.info("Rejecting Sync from: {} regent: {} on: {} consensus not proved: {} required: {}", regent,
                         regency, getMember(), certifiedLog.getProofsCount(), toleranceLevel());
                return false;
            }
            Set<HashKey> unresolved = hashes.stream().filter(h -> !hashed.containsKey(h)).collect(Collectors.toSet());
            if (unresolved.isEmpty() || unresolved.contains(hash)) {
                return true;
            }
            return false;
        }

        public void resolveStatus() {
            Member regent = getRegent(nextRegent);
            if (getMember().equals(regent)) {
                transitions.becomeLeader();
            } else {
                transitions.becomeFollower();
            }
        }
    }

    public class Service {

        public TransactionResult clientSubmit(SubmitTransaction request, HashKey from) {
            Member member = params.context.getMember(from);
            if (member == null) {
                log.warn("Received client transaction submission from non member: {} on: {}", from, getMember());
                return TransactionResult.getDefaultInstance();
            }
            EnqueuedTransaction enqueuedTransaction = new EnqueuedTransaction(hashOf(request.getTransaction()),
                    request.getTransaction());
            if (enqueuedTransaction.getTransaction().getJoin()) {
                if (viewContext().getMember(from) == null) {
                    log.warn("Received join from non consortium member: {} on: {}", from, getMember());
                    return TransactionResult.getDefaultInstance();
                }
                log.info("Join transaction: {} on: {} from consortium member : {}", enqueuedTransaction.getHash(),
                         getMember(), from);
            } else {
                log.info("Client transaction: {} on: {} from: {}", enqueuedTransaction.getHash(), getMember(), from);
            }
            transitions.receive(enqueuedTransaction.getTransaction(), member);
            return TransactionResult.getDefaultInstance();
        }

        public JoinResult join(Join request, HashKey fromID) {
            Member from = viewContext().getActiveMember(fromID);
            if (from == null) {
                log.debug("Member not part of current view: {} on: {}", fromID, getMember());
                return JoinResult.getDefaultInstance();
            }
            try {
                return fsm.synchonizeOnState(() -> {
                    if (vState.getNextView() == null) {
                        log.debug("Cannot vote for: {} next view undefined on: {}", fromID, getMember());
                        return JoinResult.getDefaultInstance();
                    }
                    ViewMember member = request.getMember();
                    byte[] encoded = member.getConsensusKey().toByteArray();
                    if (!verify(from, member.getSignature().toByteArray(), encoded)) {
                        log.debug("Could not verify consensus key from {} on {}", fromID, getMember());
                    }
                    PublicKey consensusKey = SigningUtils.publicKeyOf(encoded);
                    if (consensusKey == null) {
                        log.debug("Could not deserialize consensus key from {} on {}", fromID, getMember());
                        return JoinResult.getDefaultInstance();
                    }
                    byte[] signed = sign(params.signature.get(), encoded);
                    if (signed == null) {
                        log.debug("Could not sign consensus key from {} on {}", fromID, getMember());
                        return JoinResult.getDefaultInstance();
                    }
                    return JoinResult.newBuilder()
                                     .setSignature(ByteString.copyFrom(signed))
                                     .setNextView(vState.getNextView())
                                     .build();
                });
            } catch (Exception e) {
                log.error("Error voting for: {} on: {}", from, getMember(), e);
                return JoinResult.getDefaultInstance();
            }
        }

        public void stop(StopData request, HashKey from) {
            Member member = viewContext().getMember(from);
            if (member == null) {
                log.warn("Received StopData from non consortium member: {} on: {}", from, getMember());
                return;
            }
            transitions.deliverStopData(request, member);
        }

    }

    public enum Timers {
        AWAIT_GENESIS, AWAIT_GENESIS_VIEW, AWAIT_GROUP, AWAIT_VIEW_MEMBERS, FLUSH_BATCH, PROCLAIM,
        TRANSACTION_TIMEOUT_1, TRANSACTION_TIMEOUT_2;
    }

    private static class CurrentSync {
        public final Map<HashKey, CertifiedBlock> blocks     = new HashMap<>();
        public final List<Proof>                  proofs     = new ArrayList<>();
        public final List<ByteString>             signatures = new ArrayList<>();

    }

    private static class Result {
        public final Member     member;
        public final JoinResult vote;

        public Result(Member member, JoinResult vote) {
            this.member = member;
            this.vote = vote;
        }
    }

    public static final HashKey GENESIS_VIEW_ID = HashKey.ORIGIN.prefix("Genesis".getBytes());

    private final static Logger DEFAULT_LOGGER = LoggerFactory.getLogger(Consortium.class);

    public static HashKey hashOf(TransactionOrBuilder transaction) {
        List<ByteString> buffers = new ArrayList<>();
        buffers.add(transaction.getNonce());
        buffers.add(ByteString.copyFrom(transaction.getJoin() ? new byte[] { 1 } : new byte[] { 0 }));
        buffers.add(transaction.getSource());
        for (int i = 0; i < transaction.getBatchCount(); i++) {
            buffers.add(transaction.getBatch(i).toByteString());
        }

        return new HashKey(Conversion.hashOf(BbBackedInputStream.aggregate(buffers)));
    }

    public static Block manifestBlock(byte[] data) {
        if (data.length == 0) {
            System.out.println(" Invalid data");
        }
        try {
            return Block.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid data");
        }
    }

    public static boolean noGaps(Collection<CertifiedBlock> blocks, HashKey lastBlock) {
        Map<HashKey, CertifiedBlock> hashed = blocks.stream()
                                                    .collect(Collectors.toMap(cb -> new HashKey(
                                                            Conversion.hashOf(cb.getBlock().toByteString())),
                                                                              cb -> cb));

        return noGaps(hashed, lastBlock);
    }

    public static boolean noGaps(Map<HashKey, CertifiedBlock> hashed, HashKey lastBlock) {
        CertifiedBlock emptyBlock = CertifiedBlock.getDefaultInstance();
        return hashed.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> {
            HashKey p = new HashKey(e.getValue().getBlock().getHeader().getPrevious());
            if (lastBlock.equals(p)) {
                return CertifiedBlock.newBuilder().build();
            }
            return hashed.getOrDefault(p, emptyBlock);
        })).entrySet().stream().filter(e -> e.getValue() == emptyBlock).count() == 0;
    }

    private final Function<HashKey, CommonCommunications<ConsortiumClientCommunications, Service>> createClientComms;
    private final Fsm<CollaboratorContext, Transitions>                                            fsm;
    private final byte[]                                                                           genesisData = "Give me food or give me slack or kill me".getBytes();
    private Logger                                                                                 log         = DEFAULT_LOGGER;
    private final Parameters                                                                       params;
    private final TickScheduler                                                                    scheduler   = new TickScheduler();
    private final AtomicBoolean                                                                    started     = new AtomicBoolean();
    private final Map<HashKey, SubmittedTransaction>                                               submitted   = new ConcurrentHashMap<>();
    private final Transitions                                                                      transitions;
    private final VolatileState                                                                    vState      = new VolatileState();

    public Consortium(Parameters parameters) {
        this.params = parameters;
        this.createClientComms = k -> parameters.communications.create(parameters.member, k, new Service(),
                                                                       r -> new ConsortiumServerCommunications(
                                                                               parameters.communications.getClientIdentityProvider(),
                                                                               null, r),
                                                                       ConsortiumClientCommunications.getCreate(null));
        parameters.context.register(vState);
        fsm = Fsm.construct(new CollaboratorContext(), Transitions.class, CollaboratorFsm.INITIAL, true);
        fsm.setName(getMember().getId().b64Encoded());
        transitions = fsm.getTransitions();
    }

    public Logger getLog() {
        return log;
    }

    public Member getMember() {
        return params.member;
    }

    public boolean process(CertifiedBlock certifiedBlock) {
        if (!started.get()) {
            return false;
        }
        Block block = certifiedBlock.getBlock();
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        log.debug("Processing block {} : {} on: {}", hash, block.getBody().getType(), getMember());
        final CurrentBlock previousBlock = vState.getCurrent();
        if (previousBlock != null) {
            if (block.getHeader().getHeight() != previousBlock.getBlock().getHeader().getHeight() + 1) {
                log.error("Protocol violation on {}.  Block: {} height should be {} and next block height is {}",
                          getMember(), hash, previousBlock.getBlock().getHeader().getHeight() + 1,
                          block.getHeader().getHeight());
                return false;
            }
            HashKey prev = new HashKey(block.getHeader().getPrevious().toByteArray());
            if (!previousBlock.getHash().equals(prev)) {
                log.error("Protocol violation ons {}. New block does not refer to current block hash. Should be {} and next block's prev is {}",
                          getMember(), previousBlock.getHash(), prev);
                return false;
            }
            if (!viewContext().validate(certifiedBlock)) {
                log.error("Protocol violation on {}. New block is not validated {}", getMember(), hash);
                return false;
            }
        } else {
            if (block.getBody().getType() != BodyType.GENESIS) {
                log.error("Invalid genesis block on: {} block: {}", getMember(), block.getBody().getType());
                return false;
            }
            Genesis body;
            try {
                body = Genesis.parseFrom(getBody(block));
            } catch (IOException e) {
                log.error("Protocol violation ont: {}. Genesis block body cannot be deserialized {}", getMember(),
                          hash);
                return false;
            }
            if (!validateGenesis(certifiedBlock, body.getInitialView(), params.context, toleranceLevel())) {
                log.error("Protocol violation on: {}. Genesis block is not validated {}", getMember(), hash);
                return false;
            }
        }
        return next(new CurrentBlock(hash, block));
    }

    public void setLog(Logger log) {
        this.log = log;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            transitions.start();
        }
        log.trace("Starting consortium on {}", getMember());
        transitions.start();
        vState.resume(new Service(), params.gossipDuration, params.scheduler);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            transitions.start();
        }
        log.trace("Stopping consortium on {}", getMember());
        vState.clear();
        transitions.context().clear();
        transitions.stop();
    }

    public HashKey submit(Consumer<HashKey> onCompletion, Any... transactions) throws TimeoutException {
        return submit(false, onCompletion, transactions);

    }

    // test access
    Fsm<CollaboratorContext, Transitions> fsm() {
        return fsm;
    }

    // test access
    CollaboratorContext getState() {
        return fsm.getContext();
    }

    // test accessible
    Transitions getTransitions() {
        return transitions;
    }

    private EnqueuedTransaction build(boolean join, Any... transactions) {
        byte[] nonce = new byte[32];
        entropy().nextBytes(nonce);

        Transaction.Builder builder = Transaction.newBuilder()
                                                 .setJoin(join)
                                                 .setSource(params.member.getId().toByteString())
                                                 .setNonce(ByteString.copyFrom(nonce));
        for (Any t : transactions) {
            builder.addBatch(t);
        }

        HashKey hash = hashOf(builder);

        byte[] signature = sign(params.signature.get(), hash.bytes());
        if (signature == null) {
            throw new IllegalStateException("Unable to sign transaction batch on: " + getMember());
        }
        builder.setSignature(ByteString.copyFrom(signature));
        return new EnqueuedTransaction(hash, builder.build());
    }

    private String classNameOf(Any content) {
        String url = content.getTypeUrl();
        int index = url.lastIndexOf(".");
        if (index <= 0) {
            return "Unknown Class";
        }
        return url.substring(index + 1);
    }

    private ByteString compress(ByteString input) {
        DeflaterInputStream dis = new DeflaterInputStream(
                BbBackedInputStream.aggregate(input.asReadOnlyByteBufferList()));
        try {
            return ByteString.readFrom(dis);
        } catch (IOException e) {
            log.error("Cannot compress input", e);
            return null;
        }
    }

    private void deliver(com.google.protobuf.Message message) {
        final Messenger currentMsgr = vState.getMessenger();
        if (currentMsgr == null) {
            log.error("skipping message publish as no messenger");
            return;
        }
        currentMsgr.publish(message);
    }

    private SecureRandom entropy() {
        return params.msgParameters.entropy;
    }

    private void finalize(ExecutedTransaction txn) {
        HashKey hash = new HashKey(txn.getHash());
        SubmittedTransaction previous = submitted.remove(hash);
        if (previous != null) {
            ForkJoinPool.commonPool().execute(() -> {
                if (previous.onCompletion != null) {
                    previous.onCompletion.accept(hash);
                }
            });
        }
    }

    private Block generateGenesis(Map<HashKey, EnqueuedTransaction> joining, byte[] genesisData) {
        log.info("Generating genesis on {} join transactions: {}", getMember(), joining.size());
        Reconfigure.Builder genesisView = Reconfigure.newBuilder()
                                                     .setCheckpointBlocks(256)
                                                     .setId(GENESIS_VIEW_ID.toByteString())
                                                     .setToleranceLevel(toleranceLevel());
        joining.values().forEach(join -> {
            genesisView.addTransactions(ExecutedTransaction.newBuilder()
                                                           .setHash(join.getHash().toByteString())
                                                           .setTransaction(join.getTransaction()));
            JoinTransaction txn;
            try {
                txn = join.getTransaction().getBatch(0).unpack(JoinTransaction.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("Cannot generate genesis, unable to parse Join txnL {} on: {}", join.getHash(), getMember());
                transitions.fail();
                return;
            }
            genesisView.addView(txn.getMember());
        });
        if (genesisView.getViewCount() != joining.size()) {
            log.error("Did not successfully add all validations: {}:{}", joining.size(), genesisView.getViewCount());
            return null;
        }
        Body genesisBody = Body.newBuilder()
                               .setType(BodyType.GENESIS)
                               .setContents(compress(Genesis.newBuilder()
                                                            .setGenesisData(ByteString.copyFrom(genesisData))
                                                            .setInitialView(genesisView)
                                                            .build()
                                                            .toByteString()))
                               .build();
        return Block.newBuilder()
                    .setHeader(Header.newBuilder()
                                     .setHeight(0)
                                     .setBodyHash(ByteString.copyFrom(Conversion.hashOf(genesisBody.toByteString())))
                                     .build())
                    .setBody(genesisBody)
                    .build();
    }

    private Validate generateValidationFromNextView(HashKey hash, Block block) {
        byte[] headerHash = Conversion.hashOf(block.getHeader().toByteString());
        byte[] signature = sign(vState.getNextViewConsensusKeyPair().getPrivate(), entropy(), headerHash);
        Validate validation = viewContext().generateValidation(hash, signature);
        assert viewContext().validate(block, validation);
        return validation;
    }

    private InputStream getBody(Block block) {
        return new InflaterInputStream(
                BbBackedInputStream.aggregate(block.getBody().getContents().asReadOnlyByteBufferList()));
    }

    private void joinMessageGroup(ViewContext newView) {
        log.info("Joining message group: {} on: {}", newView.getId(), getMember());
        Messenger nextMsgr = newView.createMessenger(params);
        vState.setMessenger(nextMsgr);
        nextMsgr.register(round -> scheduler.tick(round));
        vState.setOrder(new MemberOrder((m, k) -> process(m), nextMsgr));
    }

    private ConsortiumClientCommunications linkFor(Member m) {
        try {
            return vState.getComm().apply(m, params.member);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private boolean next(CurrentBlock next) {
        switch (next.getBlock().getBody().getType()) {
        case CHECKPOINT:
            transitions.processCheckpoint(next);
            break;
        case GENESIS:
            transitions.processGenesis(next);
            break;
        case RECONFIGURE:
            transitions.processReconfigure(next);
            break;
        case USER:
            transitions.processUser(next);
            break;
        case UNRECOGNIZED:
        default:
            log.error("Unrecognized block type: {} : {}", next.hashCode(), next.getBlock());
        }
        return vState.getCurrent() == next;
    }

    private KeyPair nextViewConsensusKey() {
        KeyPair current = vState.getNextViewConsensusKeyPair();

        KeyPair keyPair = generateKeyPair(2048, "RSA");
        byte[] encoded = keyPair.getPublic().getEncoded();
        byte[] signed = sign(params.signature.get(), encoded);
        if (signed == null) {
            log.error("Unable to generate and sign consensus key on: {}", getMember());
            transitions.fail();
        }
        vState.setNextViewConsensusKeyPair(keyPair);
        vState.setNextView(ViewMember.newBuilder()
                                     .setId(getMember().getId().toByteString())
                                     .setConsensusKey(ByteString.copyFrom(encoded))
                                     .setSignature(ByteString.copyFrom(signed))
                                     .build());
        return current;
    }

    private void process(Msg msg) {
        if (!started.get()) {
            return;
        }
        Any content = msg.content;

        if (content.is(TotalOrdering.class)) {
            try {
                transitions.deliverTotalOrdering(content.unpack(TotalOrdering.class), msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid validate delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        if (content.is(Block.class)) {
            try {
                Block block = content.unpack(Block.class);
                transitions.deliverBlock(block, msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid block delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        if (content.is(Persist.class)) {
            try {
                @SuppressWarnings("unused")
                Persist persist = content.unpack(Persist.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid persist delivered from: {} on: {}", msg.from, getMember(), e);
            }
            transitions.deliverPersist(HashKey.ORIGIN);
            return;
        }
        if (content.is(Transaction.class)) {
            try {
                transitions.deliverTransaction(content.unpack(Transaction.class), msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid transaction delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        if (content.is(Validate.class)) {
            try {
                transitions.deliverValidate(content.unpack(Validate.class));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid validate delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        if (content.is(Stop.class)) {
            try {
                transitions.deliverStop(content.unpack(Stop.class), msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid stop delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        if (content.is(Sync.class)) {
            try {
                transitions.deliverSync(content.unpack(Sync.class), msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid sync delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        if (content.is(StopData.class)) {
            try {
                transitions.deliverStopData(content.unpack(StopData.class), msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid sync delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        if (content.is(ReplicateTransactions.class)) {
            try {
                transitions.deliverTransactions(content.unpack(ReplicateTransactions.class), msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid replication of transactions delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        log.error("Invalid consortium message type: {} from: {} on: {}", classNameOf(content), msg.from, getMember());

    }

    private HashKey submit(boolean join, Consumer<HashKey> onCompletion, Any... transactions) throws TimeoutException {
        if (viewContext() == null) {
            throw new IllegalStateException(
                    "The current view is undefined, unable to process transactions on: " + getMember());
        }
        EnqueuedTransaction transaction = build(join, transactions);
        submit(transaction, onCompletion);
        return transaction.getHash();
    }

    private void submit(EnqueuedTransaction transaction, Consumer<HashKey> onCompletion) throws TimeoutException {
        assert transaction.getHash().equals(hashOf(transaction.getTransaction())) : "Hash does not match!";

        submitted.put(transaction.getHash(), new SubmittedTransaction(transaction.getTransaction(), onCompletion));
        int toleranceLevel = toleranceLevel();
        SubmitTransaction submittedTxn = SubmitTransaction.newBuilder()
                                                          .setContext(viewContext().getId().toByteString())
                                                          .setTransaction(transaction.getTransaction())
                                                          .build();
        log.info("Submitting txn: {} from: {}", transaction.getHash(), getMember());
        List<TransactionResult> results;
        results = viewContext().streamRandomRing().map(c -> {
            if (!getMember().equals(c)) {
                ConsortiumClientCommunications link = linkFor(c);
                if (link == null) {
                    log.trace("Cannot get link for {}", c.getId());
                    return null;
                }
                try {
                    return link.clientSubmit(submittedTxn);
                } catch (Throwable t) {
                    log.trace("Cannot submit txn {} to {}: {}", transaction.getHash(), c, t.getMessage());
                    return null;
                }
            } else {
                transitions.receive(transaction.getTransaction(), getMember());
                return TransactionResult.getDefaultInstance();
            }
        }).filter(r -> r != null).collect(Collectors.toList());

        if (results.size() <= toleranceLevel) {
            throw new TimeoutException("Cannot submit transaction " + transaction.getHash());
        }
    }

    private int toleranceLevel() {
        return params.context.toleranceLevel();
    }

    /**
     * Ye Jesus Nut
     * 
     * @param list
     */
    private void viewChange(ViewContext newView) {
        vState.pause();

        log.debug("Installing new view rings: {} ttl: {} on: {}", newView.getRingCount(), newView.timeToLive(),
                  getMember());

        vState.setComm(createClientComms.apply(newView.getId()));
        vState.setMessenger(null);
        vState.setOrder(null);
        vState.setViewContext(newView);
        if (newView.isViewMember()) {
            joinMessageGroup(newView);
        }

        vState.resume(new Service(), params.gossipDuration, params.scheduler);
    }

    private ViewContext viewContext() {
        return vState.getViewContext();
    }
}
