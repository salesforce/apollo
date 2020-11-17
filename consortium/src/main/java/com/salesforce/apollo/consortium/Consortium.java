/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.Validator.generateKeyPair;
import static com.salesforce.apollo.consortium.Validator.sign;
import static com.salesforce.apollo.consortium.Validator.validateGenesis;
import static com.salesforce.apollo.consortium.Validator.verify;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Header;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.JoinTransaction;
import com.salesfoce.apollo.consortium.proto.JoinTransaction.Builder;
import com.salesfoce.apollo.consortium.proto.Persist;
import com.salesfoce.apollo.consortium.proto.Proclamation;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.TransactionOrBuilder;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesfoce.apollo.consortium.proto.User;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesfoce.apollo.consortium.proto.ViewMember;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.PendingTransactions.EnqueuedTransaction;
import com.salesforce.apollo.consortium.TickScheduler.Timer;
import com.salesforce.apollo.consortium.TransactionSimulator.EvaluatedTransaction;
import com.salesforce.apollo.consortium.comms.ConsortiumClientCommunications;
import com.salesforce.apollo.consortium.comms.ConsortiumServerCommunications;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
import com.salesforce.apollo.consortium.fsm.Transitions;
import com.salesforce.apollo.membership.Context;
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
    public class CollaboratorContext {
        private final NavigableMap<Long, CurrentBlock>     blockCache    = new ConcurrentSkipListMap<>();
        private volatile long                              lastBlock     = -1;
        private final PendingTransactions                  pending       = new PendingTransactions();
        private final TransactionSimulator                 simulator;
        private final Map<Timers, Timer>                   timers        = new ConcurrentHashMap<>();
        private final Set<HashKey>                         unreplicated  = new HashSet<>();
        private final Map<HashKey, CertifiedBlock.Builder> workingBlocks = new HashMap<>();

        public CollaboratorContext() {
            simulator = new TransactionSimulator(params.maxBatchByteSize, this, params.validator);
        }

        public void add(Transaction txn) {
            EnqueuedTransaction transaction = new EnqueuedTransaction(hashOf(txn), txn);
            if (pending.add(transaction)) {
                unreplicated.add(transaction.getHash());
            } else {
                unreplicated.remove(transaction.getHash());
            }
        }

        public void awaitFormation() {
            schedule(Timers.AWAIT_FORMATION, () -> join(), vState.getCurrentView().timeToLive());
        }

        public void awaitGenesis() {
            schedule(Timers.AWAIT_GENESIS, () -> {
                transitions.missingGenesis();
            }, params.context.timeToLive());
        }

        public void becomeLeader() {
            emitProclamation();
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

        public void cancelAll() {
            scheduler.cancelAll();
            timers.clear();
        }

        public void deliverBlock(Block block, Member from) {
            Member leader = vState.getLeader();
            if (!from.equals(leader)) {
                log.debug("Rejecting block proposal from {} on: {} not leader: {}", from, getMember(),
                          vState.getLeader());
                return;
            }
            HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
            workingBlocks.computeIfAbsent(hash, k -> {
                Validate validation = generateValidation(hash, block);
                if (validation == null) {
                    log.debug("Rejecting block proposal, cannot validate: {} from {} on: {}", hash, from, getMember(),
                              vState.getLeader());
                    return null;
                }
                return CertifiedBlock.newBuilder()
                                     .setBlock(block)
                                     .addCertifications(Certification.newBuilder()
                                                                     .setId(validation.getId())
                                                                     .setSignature(validation.getSignature()));
            });
        }

        public void deliverGenesisBlock(Block block, Member from) {
            HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
            Member leader = vState.getLeader();
            if (!from.equals(leader)) {
                log.debug("Rejecting genesis block proposal from {}", from);
                return;
            }

            workingBlocks.computeIfAbsent(hash, k -> {
                Genesis genesis;
                try {
                    genesis = Genesis.parseFrom(getBody(block));
                } catch (IOException e) {
                    log.error("Cannot deserialize genesis block from {}", from, e);
                    return null;
                }
                genesis.getInitialView().getViewList().forEach(vm -> {
                    HashKey memberID = new HashKey(vm.getId());
                    Member member = vState.getCurrentView().getMember(memberID);
                    if (member == null) {
                        log.warn("invalid genesis, view member does not exist: {}", memberID);
                        return;
                    }
                    byte[] encoded = vm.getConsensusKey().toByteArray();
                    if (!Validator.verify(member, vm.getSignature().toByteArray(), encoded)) {
                        log.warn("invalid genesis view member consensus key: {}", memberID);
                        return;
                    }
                    PublicKey consensusKey = Validator.publicKeyOf(encoded);
                    if (consensusKey == null) {
                        log.warn("invalid genesis view member, cannot generate consensus key: {}", memberID);
                        return;
                    }
                    validators.computeIfAbsent(memberID, key -> {
                        log.debug("defining genesis view member: {} consensus key: {}", memberID,
                                  HashKey.bytesToHex(encoded));
                        return consensusKey;
                    });
                });
                Validate validation = generateValidationFromNextView(hash, block);
                return CertifiedBlock.newBuilder()
                                     .setBlock(block)
                                     .addCertifications(Certification.newBuilder()
                                                                     .setId(validation.getId())
                                                                     .setSignature(validation.getSignature()));
            });
        }

        public void deliverProclamation(Proclamation p, Member from) {
            // TODO Auto-generated method stub

        }

        public void drainPending() {
            transitions.drainPending();
        }

        public void enterView() {
            Context<Member> current = vState.getCurrentView();
            Member leader = vState.getLeader();
            if (current.isActive(getMember())) {
                if (getMember().getId().equals(leader.getId())) {
                    transitions.becomeLeader();
                } else {
                    transitions.becomeFollower();
                }
            } else {
                transitions.join();
            }
        }

        public void establishGenesisView() {
            viewChange(viewFor(GENESIS_VIEW_ID, params.context), Collections.emptyList());
            validators.put(getMember().getId(), vState.getNextViewConsensusKeyPair().getPublic());
        }

        public void evaluate(EnqueuedTransaction transaction) {
            if (pending.add(transaction)) {
                log.debug("Evaluating transaction {} on: {}", transaction.getHash(), getMember());
                unreplicated.add(transaction.getHash());
                simulator.add(transaction);
            } else {
                log.trace("Not evaluating duplicate transaction {} on: {}", transaction.getHash(), getMember());
                unreplicated.remove(transaction.getHash());
            }
            scheduleBlockTimeout();
        }

        public void evaluate(Transaction txn) {
            evaluate(new EnqueuedTransaction(hashOf(txn), txn));
        }

        public void generateGenesis() {
            if (pending.size() == vState.getCurrentView().cardinality()) {
                generateGenesisBlock();
            } else {
                log.trace("Genesis group has not formed, rescheduling: {} want: {}", pending.size(),
                          vState.getCurrentView().cardinality());
                rescheduleGenesis();
            }
        }

        public boolean generateNextBlock() {
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
            deliver(Any.pack(block));
            lastBlock = thisHeight;

            Validate validation = generateValidation(hash, block);
            builder.addCertifications(Certification.newBuilder()
                                                   .setId(validation.getId())
                                                   .setSignature(validation.getSignature()));
            deliver(Any.pack(validation));

            log.info("Generated next block: {} height: {} on: {} txns: {}", hash, thisHeight, getMember(),
                     user.getTransactionsCount());
            return true;
        }

        public void join() {
            log.debug("Attempting to join view: {} on: {}", vState.getCurrentView().getId(), params.member);
            final Join voteForMe = Join.newBuilder()
                                       .setMember(vState.getNextView())
                                       .setContext(vState.getCurrentView().getId().toByteString())
                                       .build();
            Context<Member> current = vState.getCurrentView();
            List<Result> votes = current.ring(entropy().nextInt(current.getRingCount()))
                                        .stream()
                                        .filter(m -> !m.equals(params.member))
                                        .map(c -> {
                                            ConsortiumClientCommunications link = linkFor(c);
                                            if (link == null) {
                                                log.warn("Cannot get link for {}", c.getId());
                                                return null;
                                            }
                                            JoinResult vote;
                                            try {
                                                vote = link.vote(voteForMe);
                                            } catch (Throwable e) {
                                                log.trace("Unable to poll vote from: {}:{}", c, e.getMessage());
                                                return null;
                                            }

                                            log.trace("One vote to join: {} : {} from: {}", params.member,
                                                      vState.getCurrentView().getId(), c);
                                            return new Result(c, vote);
                                        })
                                        .filter(r -> r != null)
                                        .filter(r -> r.vote.isInitialized())
                                        .limit(params.context.toleranceLevel() + 1)
                                        .collect(Collectors.toList());

            if (votes.size() <= params.context.toleranceLevel()) {
                log.debug("Did not gather votes necessary to join consortium needed: {} got: {}",
                          params.context.toleranceLevel() + 1, votes.size());
                transitions.fail();
                return;
            }
            Builder txn = JoinTransaction.newBuilder().setMember(voteForMe.getMember());
            for (Result vote : votes) {
                txn.addCertification(Certification.newBuilder()
                                                  .setId(vote.member.getId().toByteString())
                                                  .setSignature(vote.vote.getSignature()));
            }
            try {
                Consortium.this.submit(true, h -> {
                }, Any.pack(txn.build()));
            } catch (TimeoutException e) {
                transitions.fail();
                return;
            }
            log.info("successfully joined view: {} on: {}", vState.getCurrentView().getId(), params.member);
            if (vState.getLeader().equals(params.member)) {
                transitions.becomeLeader();
            } else {
                transitions.becomeFollower();
            }
        }

        public void nextView() {
            Consortium.this.nextView();
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
            body.getInitialView().getTransactionsList().forEach(txn -> Consortium.this.finalize(txn));
            pending.clear();
            submitted.clear();
            unreplicated.clear();
            transitions.genesisAccepted();
            viewChange(viewFor(new HashKey(body.getInitialView().getId()), params.context),
                       body.getInitialView().getViewList());
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
            viewChange(viewFor(new HashKey(body.getId()), params.context), body.getViewList());
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

        private void finalized(HashKey hash) {
            pending.remove(hash);
            unreplicated.remove(hash);
        }

        public void resendUnreplicated(Proclamation p, Member from) {
            if (unreplicated.size() == 0) {
                log.trace("No pending txns to rebroadcast: {}", getMember());
            }
            Iterator<HashKey> hashes = unreplicated.iterator();
            while (hashes.hasNext()) {
                HashKey hash = hashes.next();
                EnqueuedTransaction enqueued = pending.get(hash);
                if (enqueued == null) {
                    hashes.remove();
                } else {
                    log.trace("rebroadcasting txn: {}", hash);

                    deliver(Any.pack(enqueued.getTransaction()));
                }
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
            }, vState.getCurrentView().timeToLive());
        }

        public void shutdown() {
            stop();
        }

        public void submit(EnqueuedTransaction enqueuedTransaction) {
            if (pending.add(enqueuedTransaction)) {
                log.trace("Enqueueing txn: {}", enqueuedTransaction.getHash());
                unreplicated.add(enqueuedTransaction.getHash());
            } else {
                log.trace("Transaction already seen: {}", enqueuedTransaction.getHash());
            }
        }

        public void submitJoin(EnqueuedTransaction enqueuedTransaction) {
            if (enqueuedTransaction.getTransaction().getJoin()) {
                if (pending.add(enqueuedTransaction)) {
                    log.trace("Enqueueing a join transaction: {}", enqueuedTransaction.getHash());
                    unreplicated.add(enqueuedTransaction.getHash());
                } else {
                    log.trace("Join transaction already pending: {}", enqueuedTransaction.getHash());
                }
            } else {
                log.debug("Not a join transaction: {}", enqueuedTransaction.getHash());
            }
        }

        public void totalOrderDeliver() {
            log.debug("Attempting total ordering of working blocks: {} on: {}", workingBlocks.size(), getMember());
            List<HashKey> published = new ArrayList<>();
            workingBlocks.entrySet()
                         .stream()
                         .peek(e -> log.trace("TO Consider: {}:{} on: {}", e.getKey(),
                                              e.getValue().getCertificationsCount(), getMember()))
                         .filter(e -> e.getValue().getCertificationsCount() > params.context.toleranceLevel())
                         .forEach(e -> {
                             log.info("Totally ordering block: {} height: {} on: {}", e.getKey(),
                                      e.getValue().getBlock().getHeader().getHeight(), getMember());
                             params.consensus.apply(e.getValue().build());
                             published.add(e.getKey());
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
            final Validator validator = vState.getValidator();
            final HashKey memberID = new HashKey(v.getId());
            log.trace("Validation: {} from: {}", hash, memberID);
            final PublicKey key = validators.get(memberID);
            if (key == null) {
                log.debug("No valdator key to validate: {} from: {} on: {}", hash, memberID, getMember());
                return;
            }
//            log.debug("using key: {} for verifying from: {} on: {}", HashKey.bytesToHex(key.getEncoded()), memberID,
//                      getMember());
            Signature signature = Validator.signatureForVerification(key);

            if (validator.validate(certifiedBlock.getBlock(), v, signature)) {
                certifiedBlock.addCertifications(Certification.newBuilder()
                                                              .setId(v.getId())
                                                              .setSignature(v.getSignature()));
                log.debug("Adding block validation: {} from: {} on: {} count: {}", hash, memberID, getMember(),
                          certifiedBlock.getCertificationsCount());
            } else {
                log.debug("Failed block validation: {} from: {} on: {}", hash, memberID, getMember());
            }
        }

        PendingTransactions getPending() {
            return pending;
        }

        private void accept(CurrentBlock next) {
            workingBlocks.remove(next.getHash());
            vState.setCurrent(next);
            blockCache.put(next.getBlock().getHeader().getHeight(), next);
        }

        private void clear() {
            timers.values().forEach(e -> e.cancel());
            timers.clear();
            pending.clear();
            unreplicated.clear();
            lastBlock = -1;
            scheduler.cancelAll();
            workingBlocks.clear();
        }

        private void emitProclamation() {
            deliver(Any.pack(Proclamation.newBuilder().setRegenecy(0).build()));
            schedule(Timers.PROCLAIM, () -> emitProclamation(), vState.getCurrentView().timeToLive());
        }

        private void generateGenesisBlock() {
            Block block = Consortium.this.generateGenesis(pending, genesisData);
            HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
            workingBlocks.computeIfAbsent(hash, k -> {
                lastBlock = 0;
                deliver(Any.pack(block));
                Validate validation = generateValidationFromNextView(hash, block);
                if (validation == null) {
                    log.error("Cannot validate generated genesis block: {} on: {}", hash, getMember());
                }
                CertifiedBlock.Builder builder = CertifiedBlock.newBuilder().setBlock(block);
                builder.addCertifications(Certification.newBuilder()
                                                       .setId(validation.getId())
                                                       .setSignature(validation.getSignature()));
                deliver(Any.pack(validation));
                return builder;
            });
        }

        private void rescheduleGenesis() {
            schedule(Timers.AWAIT_GROUP, () -> {
                if (pending.size() > params.context.toleranceLevel()) {
                    generateGenesisBlock();
                } else {
                    log.trace("Genesis group has not formed, rescheduling: {} want: {}", pending.size(),
                              params.context.toleranceLevel());
                    rescheduleGenesis();
                }
            }, vState.getCurrentView().timeToLive());
            deliver(Any.pack(Proclamation.newBuilder().setRegenecy(0).build()));
        }

        private void schedule(Timers label, Runnable a, int delta) {
            Runnable action = () -> {
                timers.remove(label);
                a.run();
            };
            Messenger messenger = vState.getMessenger();
            int current = messenger == null ? 0 : messenger.getRound();
            Timer previous = timers.put(label, scheduler.schedule(label, action, current + delta));
            if (previous != null) {
                log.trace("Cancelling previous timer for: {}", label);
                previous.cancel();
            }
            log.trace("Setting timer for: {}", label);
        }

        private void scheduleIfAbsent(Timers label, Runnable a, int delta) {
            Runnable action = () -> {
                timers.remove(label);
                a.run();
            };
            Messenger messenger = vState.getMessenger();
            int current = messenger == null ? 0 : messenger.getRound();
            timers.computeIfAbsent(label, k -> {
                log.trace("Setting timer for: {}", label);
                return scheduler.schedule(k, action, current + delta);
            });
        }
    }

    public class Service {

        public TransactionResult clientSubmit(SubmitTransaction request, HashKey from) {
            if (params.context.getMember(from) == null) {
                log.warn("Received client transaction submission from non member: {} on: {}", from, getMember());
                return TransactionResult.getDefaultInstance();
            }
            EnqueuedTransaction enqueuedTransaction = new EnqueuedTransaction(hashOf(request.getTransaction()),
                    request.getTransaction());
            if (enqueuedTransaction.getTransaction().getJoin()) {
                Member joiningMember = vState.getCurrentView().getMember(from);
                if (joiningMember == null) {
                    log.warn("Received join from non consortium member: {} on: {}", from, getMember());
                    return TransactionResult.getDefaultInstance();
                }
                log.info("Join transaction: {} on: {} from consortium member : {}", enqueuedTransaction.getHash(),
                         getMember(), from);
            } else {
                log.info("Client transaction: {} on: {} from: {}", enqueuedTransaction.getHash(), getMember(), from);
            }
            transitions.submit(enqueuedTransaction);
            return TransactionResult.getDefaultInstance();
        }

        public JoinResult vote(Join request, HashKey fromID) {
            Member from = vState.getCurrentView().getActiveMember(fromID);
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
                    PublicKey consensusKey = Validator.publicKeyOf(encoded);
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

    }

    public enum Timers {
        AWAIT_FORMATION, AWAIT_GENESIS, AWAIT_GROUP, AWAIT_VIEW_MEMBERS, FLUSH_BATCH, PROCLAIM;
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

    /**
     * Answer the live successors of the hash on the base context view
     */
    public static Context<Member> viewFor(HashKey hash, Context<? super Member> baseContext) {
        Context<Member> newView = new Context<Member>(hash, baseContext.getRingCount());
        Set<Member> successors = new HashSet<Member>();
        baseContext.successors(hash, m -> {
            if (successors.size() == baseContext.getRingCount()) {
                return false;
            }
            boolean contained = successors.contains(m);
            successors.add(m);
            return !contained;
        });
        assert successors.size() == baseContext.getRingCount();
        successors.forEach(e -> {
            if (baseContext.isActive(e)) {
                newView.activate(e);
            } else {
                newView.offline(e);
            }
        });
        assert newView.getActive().size() + newView.getOffline().size() == baseContext.getRingCount();
        return newView;
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
    private final Map<HashKey, PublicKey>                                                          validators  = new HashMap<>();
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
            if (!vState.getValidator().validate(certifiedBlock)) {
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

    private void deliver(Any message) {
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

    private Block generateGenesis(PendingTransactions joining, byte[] genesisData) {
        log.info("Generating genesis on {} join transactions: {}", getMember(), joining.size());
        Reconfigure.Builder genesisView = Reconfigure.newBuilder()
                                                     .setCheckpointBlocks(256)
                                                     .setId(GENESIS_VIEW_ID.toByteString())
                                                     .setToleranceLevel(toleranceLevel());
        joining.forEach(join -> {
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
            ViewMember vm = txn.getMember();
            HashKey memberId = new HashKey(vm.getId());
            PublicKey consensusKey = Validator.publicKeyOf(vm.getConsensusKey().toByteArray());
            if (consensusKey == null) {
                log.error("Cannot deserialize consensus key for: {} on: {}", memberId, getMember());
            } else {
                validators.put(memberId, consensusKey);
                genesisView.addView(vm);
            }
        });
        if (genesisView.getViewCount() != joining.size()) {
            log.error("Did not successfully add all validations: {}:{}", joining.size(), genesisView.getViewCount());
            return null;
        }
        Body genesisBody = Body.newBuilder()
                               .setConsensusId(0)
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

    private void generateNextViewConsensusKey() {
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
//        log.info("Next view: {}  next view consensus key: {}", getMember(),
//                 HashKey.bytesToHex(keyPair.getPublic().getEncoded()));
    }

    private Validate generateValidation(HashKey hash, Block block) {
        byte[] signature = sign(vState.getConsensusKeyPair().getPrivate(), entropy(),
                                Conversion.hashOf(block.getHeader().toByteString()));
        return generateValidation(hash, signature);
    }

    private Validate generateValidation(HashKey hash, byte[] signature) {
        if (signature == null) {
            log.error("Unable to sign block: {} on: {}", hash, getMember());
            return null;
        }
        Validate validation = Validate.newBuilder()
                                      .setId(params.member.getId().toByteString())
                                      .setHash(hash.toByteString())
                                      .setSignature(ByteString.copyFrom(signature))
                                      .build();
        deliver(Any.pack(validation));
        return validation;
    }

    private Validate generateValidationFromNextView(HashKey hash, Block block) {
//        log.debug("using key: {} for validation generation on: {}",
//                  HashKey.bytesToHex(vState.getNextViewConsensusKeyPair().getPublic().getEncoded()), getMember());
        byte[] headerHash = Conversion.hashOf(block.getHeader().toByteString());
        byte[] signature = sign(vState.getNextViewConsensusKeyPair().getPrivate(), entropy(), headerHash);
//        log.info("signing block: {} header hash: {} on: {} sig: {}", hash, HashKey.bytesToHex(headerHash), getMember(),
//                 HashKey.bytesToHex(signature));
        Validate validation = generateValidation(hash, signature);
        assert vState.getValidator()
                     .validate(block, validation,
                               Validator.signatureForVerification(vState.getNextViewConsensusKeyPair().getPublic()));
        return validation;
    }

    private InputStream getBody(Block block) {
        return new InflaterInputStream(
                BbBackedInputStream.aggregate(block.getBody().getContents().asReadOnlyByteBufferList()));
    }

    private Member leaderOf(Context<Member> newView) {
        return newView.ring(0).successor(newView.getId());
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

    private KeyPair nextView() {
        KeyPair currentKP = vState.getNextViewConsensusKeyPair();
        vState.setConsensusKeyPair(currentKP);
//        log.info("Next view: {} current consensus key: {}", getMember(),
//                 currentKP == null ? null : HashKey.bytesToHex(currentKP.getPublic().getEncoded()));

        generateNextViewConsensusKey();
        return currentKP;
    }

    private void process(Msg msg) {
        if (!started.get()) {
            return;
        }
        if (msg.content.is(Block.class)) {
            try {
                transitions.deliverBlock(msg.content.unpack(Block.class), msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid block delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        if (msg.content.is(Persist.class)) {
            try {
                @SuppressWarnings("unused")
                Persist persist = msg.content.unpack(Persist.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid persis delivered from: {} on: {}", msg.from, getMember(), e);
            }
            transitions.deliverPersist(HashKey.ORIGIN);
            return;
        }
        if (msg.content.is(Transaction.class)) {
            try {
                transitions.deliverTransaction(msg.content.unpack(Transaction.class));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid transaction delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        if (msg.content.is(Validate.class)) {
            try {
                transitions.deliverValidate(msg.content.unpack(Validate.class));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid validate delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        if (msg.content.is(Proclamation.class)) {
            try {
                transitions.deliverProclamation(msg.content.unpack(Proclamation.class), msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid validate delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        log.error("Invalid consortium message type: {} from: {} on: {}", msg.content.getDescriptorForType(), msg.from,
                  getMember());

    }

    private HashKey submit(boolean join, Consumer<HashKey> onCompletion, Any... transactions) throws TimeoutException {
        final Context<Member> current = vState.getCurrentView();
        if (current == null) {
            throw new IllegalStateException(
                    "The current view is undefined, unable to process transactions on: " + getMember());
        }

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
        Transaction transaction = builder.build();
        assert hash.equals(hashOf(transaction)) : "Hash does not match!";

        submitted.put(hash, new SubmittedTransaction(transaction, onCompletion));
        int toleranceLevel = toleranceLevel();
        SubmitTransaction submittedTxn = SubmitTransaction.newBuilder()
                                                          .setContext(current.getId().toByteString())
                                                          .setTransaction(transaction)
                                                          .build();
        log.info("Submitting txn: {} from: {}", hash, getMember());
        List<TransactionResult> results;
        results = current.ring(entropy().nextInt(current.getRingCount())).stream().map(c -> {
            if (!getMember().equals(c)) {
                ConsortiumClientCommunications link = linkFor(c);
                if (link == null) {
                    log.trace("Cannot get link for {}", c.getId());
                    return null;
                }
                try {
                    return link.clientSubmit(submittedTxn);
                } catch (Throwable t) {
                    log.trace("Cannot submit txn {} to {}: {}", hash, c, t.getMessage());
                    return null;
                }
            } else {
                transitions.submit(new EnqueuedTransaction(hash, transaction));
                return TransactionResult.getDefaultInstance();
            }
        }).filter(r -> r != null).limit(toleranceLevel + 1).collect(Collectors.toList());

        if (results.size() <= toleranceLevel) {
            throw new TimeoutException("Cannot submit transaction " + hash);
        }
        return hash;
    }

    private int toleranceLevel() {
        return params.context.toleranceLevel();
    }

    /**
     * Ye Jesus Nut
     * 
     * @param list
     */
    private void viewChange(Context<Member> newView, List<ViewMember> members) {
        vState.pause();

        log.debug("Installing new view rings: {} ttl: {} on: {}", newView.getRingCount(), newView.timeToLive(),
                  getMember());

        // Live successor of the view ID on ring zero is presumed leader
        Member newLeader = leaderOf(newView);

        vState.setComm(createClientComms.apply(newView.getId()));
        vState.setValidator(new Validator(newLeader, newView, toleranceLevel()));
        vState.setMessenger(null);
        vState.setOrder(null);

        nextView();

        validators.clear();
        members.forEach(vm -> {
            HashKey memberID = new HashKey(vm.getId());
            if (getMember().getId().equals(memberID)) {
                validators.put(memberID, vState.getConsensusKeyPair().getPublic());
            } else {
                byte[] encoded = vm.getConsensusKey().toByteArray();
                PublicKey consensusKey = Validator.publicKeyOf(encoded);
                if (consensusKey == null) {
                    log.debug("invalid view member, cannot deserialize consensus key for: {} on: {}", memberID,
                              getMember());
                    return;
                }
                validators.computeIfAbsent(memberID, k -> consensusKey);
            }
        });

        if (newView.getMember(params.member.getId()) != null) { // cohort member
            Messenger nextMsgr = new Messenger(params.member, params.signature, newView, params.communications,
                    params.msgParameters);
            vState.setMessenger(nextMsgr);
            nextMsgr.register(round -> scheduler.tick(round));
            vState.setOrder(new MemberOrder((m, k) -> process(m), nextMsgr));
            log.debug("reconfiguring, becoming joining member: {}", params.member);
            transitions.join();
        } else { // you are all my puppets
            log.debug("reconfiguring, becoming client: {}", params.member);
            transitions.becomeClient();
        }

        vState.resume(new Service(), params.gossipDuration, params.scheduler);
    }
}
