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
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Body;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.Certification;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.ConsortiumMessage;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Header;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.JoinTransaction;
import com.salesfoce.apollo.consortium.proto.JoinTransaction.Builder;
import com.salesfoce.apollo.consortium.proto.MessageType;
import com.salesfoce.apollo.consortium.proto.Proclamation;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.Transaction;
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
        private final Map<HashKey, ViewMember>             members         = new HashMap<>();
        private final PendingTransactions                  pending         = new PendingTransactions();
        private final Set<HashKey>                         publishedBlocks = new HashSet<>();
        private final TickScheduler                        scheduler       = new TickScheduler();
        private final TransactionSimulator                 simulator       = new TransactionSimulator(
                params.maxBatchByteSize, this, params.validator);
        private final Map<Timers, Timer>                   timers          = new ConcurrentHashMap<>();
        private final Map<HashKey, CertifiedBlock.Builder> workingBlocks   = new HashMap<>();

        public void add(Transaction txn) {
            HashKey hash = new HashKey(Conversion.hashOf(txn.toByteString()));
            pending.add(new EnqueuedTransaction(hash, txn));
        }

        public void awaitFormation() {
            schedule(Timers.AWAIT_FORMATION, () -> join(), vState.getCurrentView().timeToLive());
        }

        public void awaitGenesis() {
            validators.put(getMember().getId(), vState.getNextViewConsensusKeyPair().getPublic());
            schedule(Timers.AWAIT_GENESIS, () -> {
                transitions.missingGenesis();
            }, params.context.timeToLive());

            viewChange(viewFor(GENESIS_VIEW_ID, params.context), Collections.emptyList());
        }

        public void awaitViewMembers() {
            schedule(Timers.AWAIT_VIEW_MEMBERS, () -> {
                if (members.size() > params.context.toleranceLevel() + 1) {
                    transitions.success();
                } else {
                    transitions.fail();
                }
            }, vState.getValidator().getView().timeToLive() * 2);
        }

        public void becomeLeader() {
            emitProclamation();
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
            timers.values().forEach(t -> t.cancel());
            timers.clear();
        }

        public void deliverBlock(Block block, Member from) {
            Member leader = vState.getLeader();
            if (!from.equals(leader)) {
                log.debug("Rejecting block proposal from {}", from);
                return;
            }
            HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
            CertifiedBlock.Builder builder = workingBlocks.computeIfAbsent(hash, k -> CertifiedBlock.newBuilder()
                                                                                                    .setBlock(block));
            Validate validation = generateValidation(hash, block);
            builder.addCertifications(Certification.newBuilder()
                                                   .setId(validation.getId())
                                                   .setSignature(validation.getSignature()));
        }

        public void deliverGenesisBlock(Block block, Member from) {
            HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
            Member leader = vState.getLeader();
            if (!from.equals(leader)) {
                log.debug("Rejecting genesis block proposal from {}", from);
                return;
            }
            Genesis genesis;
            try {
                genesis = Genesis.parseFrom(getBody(block));
            } catch (IOException e) {
                log.error("Cannot deserialize genesis block from {}", from, e);
                return;
            }
            genesis.getInitialView().getViewList().forEach(vm -> {
                HashKey memberID = new HashKey(vm.getId());
                Member member = vState.getCurrentView().getMember(memberID);
                if (member == null) {
                    log.info("invalid genesis, view member does not exist: {}", memberID);
                    return;
                }
                byte[] encoded = vm.getConsensusKey().toByteArray();
                if (!Validator.verify(member.forVerification(Conversion.DEFAULT_SIGNATURE_ALGORITHM),
                                      vm.getSignature().toByteArray(), encoded)) {
                    log.info("invalid genesis view member consensus key: {}", memberID);
                    return;
                }
                PublicKey consensusKey = Validator.publicKeyOf(encoded);
                if (consensusKey == null) {
                    log.info("invalid genesis view member, cannot generate consensus key: {}", memberID);
                    return;
                }
                validators.computeIfAbsent(memberID, k -> consensusKey);
            });
            CertifiedBlock.Builder builder = workingBlocks.computeIfAbsent(hash, k -> CertifiedBlock.newBuilder()
                                                                                                    .setBlock(block));
            Validate validation = generateValidationFromNextView(hash, block);
            builder.addCertifications(Certification.newBuilder()
                                                   .setId(validation.getId())
                                                   .setSignature(validation.getSignature()));
        }

        public void deliverProclamation(Proclamation p, Member from) {
            // TODO Auto-generated method stub

        }

        public void drain() {
            EnqueuedTransaction txn = pending.first();
            while (txn != null) {
                if (simulator.add(txn)) {
                    pending.removeFirst();
                    txn = pending.first();
                } else {
                    txn = null;
                }
            }
        }

        public void drainPending(Deque<EnqueuedTransaction> transactions) {
            transitions.drainPending(transactions);
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

        public void generateGenesis() {
            if (pending.size() == vState.getCurrentView().cardinality()) {
                generateGenesisBlock();
            } else {
                log.trace("Genesis group has not formed, rescheduling: {} want: {}", pending.size(),
                          vState.getCurrentView().cardinality());
                rescheduleGenesis();
            }
        }

        public void join() {
            log.info("Attempting to join: {} : {}", params.member, vState.getCurrentView().getId());
            final Join voteForMe = Join.newBuilder()
                                       .setMember(vState.getNextView())
                                       .setContext(vState.getCurrentView().getId().toByteString())
                                       .build();
            Context<Member> current = vState.getCurrentView();
            List<Result> votes = current.ring(entropy().nextInt(current.getRingCount())).stream().map(c -> {
                ConsortiumClientCommunications link = linkFor(c);
                if (link == null) {
                    log.warn("Cannot get link for {}", c.getId());
                    return null;
                }
                JoinResult vote;
                try {
                    vote = link.vote(voteForMe);
                } catch (Throwable e) {
                    log.trace("Unable to get vote from: {}:{}", c, e.getMessage());
                    return null;
                }

                log.trace("One vote to join: {} : {} from: {}", params.member, vState.getCurrentView().getId(), c);
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
                }, txn.build().toByteArray());
            } catch (TimeoutException e) {
                transitions.fail();
                return;
            }
            log.debug("successfully joined: {} : {}", params.member, vState.getCurrentView().getId());
            if (vState.getLeader().equals(params.member)) {
                transitions.becomeLeader();
            } else {
                transitions.becomeFollower();
            }
        }

        public Member member() {
            return Consortium.this.getMember();
        }

        public void nextView() {
            Consortium.this.nextView();
        }

        public void processCheckpoint(CurrentBlock next) {
            @SuppressWarnings("unused")
            Checkpoint body;
            try {
                body = Checkpoint.parseFrom(getBody(next.getBlock()));
            } catch (IOException e) {
                log.trace("Protocol violation.  Cannot decode checkpoint body: {}", e);
                return;
            }
            vState.setCurrent(next);
        }

        public void processGenesis(CurrentBlock next) {
            Genesis body;
            try {
                body = Genesis.parseFrom(getBody(next.getBlock()));
            } catch (IOException e) {
                log.trace("Protocol violation.  Cannot decode genesis body: {}", e);
                return;
            }
            pending.clear();
            vState.setCurrent(next);
            transitions.genesisAccepted();
            body.getInitialView().getTransactionsList().forEach(txn -> Consortium.this.finalize(txn));
            viewChange(viewFor(new HashKey(body.getInitialView().getId()), params.context),
                       body.getInitialView().getViewList());
        }

        public void processReconfigure(CurrentBlock next) {
            Reconfigure body;
            try {
                body = Reconfigure.parseFrom(getBody(next.getBlock()));
            } catch (IOException e) {
                log.trace("Protocol violation.  Cannot decode reconfiguration body: {}", e);
                return;
            }
            vState.setCurrent(next);
            viewChange(viewFor(new HashKey(body.getId()), params.context), body.getViewList());
        }

        public void processUser(CurrentBlock next) {
            @SuppressWarnings("unused")
            User body;
            try {
                body = User.parseFrom(getBody(next.getBlock()));
            } catch (IOException e) {
                log.trace("Protocol violation.  Cannot decode reconfiguration body: {}", next.getHash(), e);
                return;
            }
            vState.setCurrent(next);
        }

        public void resendPending(Proclamation p, Member from) {
            if (pending.size() == 0) {
                log.trace("No pending txns to rebroadcast: {}", getMember());
            }
            pending.forEach(enqueuedTransaction -> {
                log.trace("rebroadcasting txn: {}", enqueuedTransaction.getHash());
                deliver(ConsortiumMessage.newBuilder()
                                         .setMsg(enqueuedTransaction.getTransaction().toByteString())
                                         .setType(MessageType.TRANSACTION)
                                         .build());
            });
        }

        public void scheduleBlockTimeout() {
            scheduleIfAbsent(Timers.FLUSH_BATCH, () -> {
                if (pending.isEmpty()) {
                    generateNextBlock();
                }
            }, vState.getCurrentView().timeToLive());
        }

        public void shutdown() {
            stop();
        }

        public void submit(EnqueuedTransaction enqueuedTransaction) {
            if (pending.add(enqueuedTransaction)) {
                log.trace("Enqueueing txn: {}", enqueuedTransaction.getHash());
                deliver(ConsortiumMessage.newBuilder()
                                         .setMsg(enqueuedTransaction.getTransaction().toByteString())
                                         .setType(MessageType.TRANSACTION)
                                         .build());
            } else {
                log.trace("Transaction already seen: {}", enqueuedTransaction.getHash());
            }
        }

        public void submitJoin(EnqueuedTransaction enqueuedTransaction) {
            if (!enqueuedTransaction.getTransaction().getJoin()) {
                if (pending.add(enqueuedTransaction)) {
                    log.trace("Enqueuing a join transaction: {}", enqueuedTransaction.getHash());
                } else {
                    log.trace("Join transaction already pending: {}", enqueuedTransaction.getHash());
                }
            } else {
                log.debug("Not a join transaction: {}", enqueuedTransaction.getHash());
            }
        }

        public void tick(int round) {
            scheduler.tick(round);
        }

        public void totalOrderDeliver() {
            log.trace("Attempting total ordering of working blocks on: {} : {}", getMember(), workingBlocks.size());
            workingBlocks.entrySet()
                         .stream()
                         .peek(e -> log.trace("TO Consider: {}:{}", e.getKey(), e.getValue().getCertificationsCount()))
                         .filter(e -> !publishedBlocks.contains(e.getKey()))
                         .filter(e -> e.getValue().getCertificationsCount() >= params.context.toleranceLevel())
                         .forEach(e -> {
                             log.info("Totally ordering block {}", e.getKey());
                             publishedBlocks.add(e.getKey());
                             params.consensus.apply(e.getValue().build());
                         });
        }

        public void validate(Validate v) {
            HashKey hash = new HashKey(v.getHash());
            CertifiedBlock.Builder certifiedBlock = workingBlocks.get(hash);
            if (certifiedBlock == null) {
                log.trace("No working block to validate: {}", hash);
                return;
            }
            final Validator validator = vState.getValidator();
            final HashKey memberID = new HashKey(v.getId());
            log.trace("Validation: {} from: {}", hash, memberID);
            final PublicKey key = validators.get(memberID);
            if (key == null) {
                log.debug("No valdator key to validate: {}:{}", hash, memberID);
                return;
            }
            Signature signature = Validator.signatureForVerification(key);

            if (validator.validate(certifiedBlock.getBlock(), v, signature)) {
                certifiedBlock.addCertifications(Certification.newBuilder()
                                                              .setId(v.getId())
                                                              .setSignature(v.getSignature()));
                log.trace("Adding block validation: {} from: {} on: {} count: {}", hash, memberID, getMember(),
                          certifiedBlock.getCertificationsCount());
            } else {
                log.debug("Failed block validation: {} from: {} on: {}", hash, memberID, getMember());
            }
        }

        void evaluated(Deque<EvaluatedTransaction> evaluated) {
            // TODO Auto-generated method stub

        }

        PendingTransactions getPending() {
            return pending;
        }

        private void clear() {
            members.clear();
            timers.values().forEach(e -> e.cancel());
            timers.clear();
            pending.clear();
            scheduler.cancelAll();
            workingBlocks.clear();
        }

        private void emitProclamation() {
            deliver(ConsortiumMessage.newBuilder()
                                     .setType(MessageType.PROCLAMATION)
                                     .setMsg(Proclamation.newBuilder().setRegenecy(0).build().toByteString())
                                     .build());
            schedule(Timers.PROCLAIM, () -> emitProclamation(), vState.getCurrentView().timeToLive());
        }

        @SuppressWarnings("unused")
        private void generate(Block block) {
            HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
            CertifiedBlock.Builder builder = workingBlocks.computeIfAbsent(hash, k -> CertifiedBlock.newBuilder()
                                                                                                    .setBlock(block));
            deliver(ConsortiumMessage.newBuilder().setType(MessageType.BLOCK).setMsg(block.toByteString()).build());
            Validate validation = generateValidation(hash, block);
            builder.addCertifications(Certification.newBuilder()
                                                   .setId(validation.getId())
                                                   .setSignature(validation.getSignature()));
            deliver(ConsortiumMessage.newBuilder()
                                     .setType(MessageType.VALIDATE)
                                     .setMsg(validation.toByteString())
                                     .build());
        }

        private void generateGenesisBlock() {
            Block block = Consortium.this.generateGenesis(pending, genesisData);
            HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
            CertifiedBlock.Builder builder = workingBlocks.computeIfAbsent(hash, k -> CertifiedBlock.newBuilder()
                                                                                                    .setBlock(block));
            deliver(ConsortiumMessage.newBuilder().setType(MessageType.BLOCK).setMsg(block.toByteString()).build());
            Validate validation = generateValidationFromNextView(hash, block);
            builder.addCertifications(Certification.newBuilder()
                                                   .setId(validation.getId())
                                                   .setSignature(validation.getSignature()));
            deliver(ConsortiumMessage.newBuilder()
                                     .setType(MessageType.VALIDATE)
                                     .setMsg(validation.toByteString())
                                     .build());
        }

        private void generateNextBlock() {
            User.Builder body = User.newBuilder();
            EnqueuedTransaction txn = pending.removeFirst();
            int processed = 0;
            int processedBytes = 0;
            while (txn != null && processed <= params.maxBatchByteSize && processedBytes <= params.maxBatchByteSize) {
                processed++;
                processedBytes += txn.getTransaction().getSerializedSize() + 32;
                body.addTransactions(ExecutedTransaction.newBuilder()
                                                        .setHash(txn.getHash().toByteString())
                                                        .setTransaction(txn.getTransaction()));
            }
            scheduleBlockTimeout();
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
            deliver(ConsortiumMessage.newBuilder()
                                     .setType(MessageType.PROCLAMATION)
                                     .setMsg(Proclamation.newBuilder().setRegenecy(0).build().toByteString())
                                     .build());
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

        public void evaluate() {
            simulator.evaluateNext();
        }
    }

    public class Service {

        public TransactionResult clientSubmit(SubmitTransaction request) {
            HashKey hash = new HashKey(Conversion.hashOf(request.getTransaction().toByteString()));
            transitions.submit(new EnqueuedTransaction(hash, request.getTransaction()));
            return TransactionResult.getDefaultInstance();
        }

        public JoinResult vote(Join request, HashKey fromID) {
            Member from = vState.getCurrentView().getActiveMember(fromID);
            if (from == null) {
                log.debug("Member not part of current view: {}", fromID);
                return JoinResult.getDefaultInstance();
            }
            ViewMember member = request.getMember();
            byte[] encoded = member.getConsensusKey().toByteArray();
            PublicKey consensusKey = verify(member.getSignature().toByteArray(), encoded);
            if (consensusKey == null) {
                log.debug("Could not verify consensus key from {}", fromID);
                return JoinResult.getDefaultInstance();
            }
            byte[] signed = sign(params.signature.get(), entropy(), encoded);
            if (signed == null) {
                log.debug("Could not sign consensus key from {}", fromID);
                return JoinResult.getDefaultInstance();
            }
            return JoinResult.newBuilder()
                             .setSignature(ByteString.copyFrom(signed))
                             .setNextView(vState.getNextView())
                             .build();
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

    private final byte[]                             genesisData = "Give me food or give me slack or kill me".getBytes();
    private Logger                                   log         = DEFAULT_LOGGER;
    private final Parameters                         params;
    private final AtomicBoolean                      started     = new AtomicBoolean();
    private final Map<HashKey, SubmittedTransaction> submitted   = new HashMap<>();
    private final Transitions                        transitions;
    private final Map<HashKey, PublicKey>            validators  = new HashMap<>();
    private final VolatileState                      vState      = new VolatileState();

    public Consortium(Parameters parameters) {
        this.params = parameters;
        this.createClientComms = k -> parameters.communications.create(parameters.member, k, new Service(),
                                                                       r -> new ConsortiumServerCommunications(
                                                                               parameters.communications.getClientIdentityProvider(),
                                                                               null, r),
                                                                       ConsortiumClientCommunications.getCreate(null));
        parameters.context.register(vState);
        Fsm<CollaboratorContext, Transitions> fsm = Fsm.construct(new CollaboratorContext(), Transitions.class,
                                                                  CollaboratorFsm.INITIAL, true);
        fsm.setName(getMember().getId().b64Encoded());
        transitions = fsm.getTransitions();
        nextView();
    }

    public Logger getLog() {
        return log;
    }

    public Member getMember() {
        return params.member;
    }

    public void process(CertifiedBlock certifiedBlock) {
        if (!started.get()) {
            return;
        }
        Block block = certifiedBlock.getBlock();
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        log.trace("Processing block {} : {}", hash, block.getBody().getType());
        final CurrentBlock previousBlock = vState.getCurrent();
        if (previousBlock != null) {
            if (block.getHeader().getHeight() != previousBlock.getBlock().getHeader().getHeight() + 1) {
                log.error("Protocol violation.  Block height should be {} and next block height is {}",
                          previousBlock.getBlock().getHeader().getHeight(), block.getHeader().getHeight());
                return;
            }
            HashKey prev = new HashKey(block.getHeader().getPrevious().toByteArray());
            if (previousBlock.getHash().equals(prev)) {
                log.error("Protocol violation. New block does not refer to current block hash. Should be {} and next block's prev is {}",
                          previousBlock.getHash(), prev);
                return;
            }
            if (!vState.getValidator().validate(certifiedBlock)) {
                log.error("Protocol violation. New block is not validated {}", hash);
                return;
            }
        } else {
            if (block.getBody().getType() != BodyType.GENESIS) {
                log.error("Invalid genesis block: {}", block.getBody().getType());
                return;
            }
            Genesis body;
            try {
                body = Genesis.parseFrom(getBody(block));
            } catch (IOException e) {
                log.error("Protocol violation. Genesis block body cannot be deserialized {}", hash);
                return;
            }
            if (!validateGenesis(certifiedBlock, body.getInitialView(), params.context, toleranceLevel())) {
                log.error("Protocol violation. Genesis block is not validated {}", hash);
                return;
            }
        }
        next(new CurrentBlock(hash, block));
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

    public HashKey submit(Consumer<HashKey> onCompletion, byte[]... transactions) throws TimeoutException {
        return submit(false, onCompletion, transactions);

    }

    CollaboratorContext getState() {
        return transitions.context();
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

    private void deliver(ConsortiumMessage message) {
        final Messenger currentMsgr = vState.getMessenger();
        if (currentMsgr == null) {
            log.error("skipping message publish as no messenger");
            return;
        }
        currentMsgr.publish(message.toByteArray());
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
        log.info("Generating genesis on {}", getMember());
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
                txn = JoinTransaction.parseFrom(join.getTransaction().getBatch(0));
            } catch (InvalidProtocolBufferException e) {
                log.error("Cannot generate genesis, unable to parse Join txn {}", join.getHash());
                transitions.fail();
                return;
            }
            ViewMember vm = txn.getMember();
            HashKey memberId = new HashKey(vm.getId());
            PublicKey consensusKey = Validator.publicKeyOf(vm.getConsensusKey().toByteArray());
            if (consensusKey == null) {
                log.error("Cannot deserialize consensus key for {}", memberId);
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

    private Validate generateValidation(HashKey hash, Block block) {
        byte[] signature = sign(vState.getConsensusKeyPair().getPrivate(), entropy(),
                                Conversion.hashOf(block.getHeader().toByteString()));
        return generateValidation(hash, signature);
    }

    private Validate generateValidation(HashKey hash, byte[] signature) {
        if (signature == null) {
            log.error("Unable to sign block {}", hash);
            return null;
        }
        Validate validation = Validate.newBuilder()
                                      .setId(params.member.getId().toByteString())
                                      .setHash(hash.toByteString())
                                      .setSignature(ByteString.copyFrom(signature))
                                      .build();
        vState.getMessenger()
              .publish(ConsortiumMessage.newBuilder()
                                        .setType(MessageType.VALIDATE)
                                        .setMsg(validation.toByteString())
                                        .build()
                                        .toByteArray());
        return validation;
    }

    private Validate generateValidationFromNextView(HashKey hash, Block block) {
        byte[] signed = Conversion.hashOf(block.getHeader().toByteString());
        byte[] signature = sign(vState.getNextViewConsensusKeyPair().getPrivate(), entropy(), signed);
        return generateValidation(hash, signature);
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

    private void next(CurrentBlock next) {
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

    }

    private KeyPair nextView() {
        KeyPair currentKP = vState.getNextViewConsensusKeyPair();
        vState.setConsensusKeyPair(currentKP);

        KeyPair keyPair = generateKeyPair(2048, "RSA");
        byte[] encoded = keyPair.getPublic().getEncoded();
        byte[] signed = sign(params.signature.get(), params.msgParameters.entropy, encoded);
        if (signed == null) {
            log.error("Unable to generate and sign consensus key");
            transitions.fail();
        }
        vState.setNextViewConsensusKeyPair(keyPair);
        vState.setNextView(ViewMember.newBuilder()
                                     .setId(getMember().getId().toByteString())
                                     .setConsensusKey(ByteString.copyFrom(encoded))
                                     .setSignature(ByteString.copyFrom(signed))
                                     .build());
        return currentKP;
    }

    private void process(Msg msg) {
        if (!started.get()) {
            return;
        }
        log.trace("Processing {} from {}", msg.sequenceNumber, msg.from);
        ConsortiumMessage message;
        try {
            message = ConsortiumMessage.parseFrom(msg.content);
        } catch (InvalidProtocolBufferException e) {
            log.error("error parsing message from {}", msg.from, e);
            return;
        }
        switch (message.getType()) {
        case BLOCK:
            try {
                transitions.deliverBlock(Block.parseFrom(message.getMsg().asReadOnlyByteBuffer()), msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid block delivered from {}", msg.from, e);
            }
            break;
        case PERSIST:
            transitions.deliverPersist(new HashKey(message.getMsg()));
            break;
        case TRANSACTION:
            try {
                transitions.deliverTransaction(Transaction.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid transaction delivered from {}", msg.from, e);
            }
            break;
        case VALIDATE:
            try {
                transitions.deliverValidate(Validate.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid validate delivered from {}", msg.from, e);
            }
            break;
        case PROCLAMATION:
            try {
                transitions.deliverProclamation(Proclamation.parseFrom(message.getMsg().asReadOnlyByteBuffer()),
                                                msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid validate delivered from {}", msg.from, e);
            }
            break;
        case UNRECOGNIZED:
        default:
            log.error("Invalid consortium message type: {} from: {}", message.getType(), msg.from);
        }
    }

    private HashKey submit(boolean join, Consumer<HashKey> onCompletion,
                           byte[]... transactions) throws TimeoutException {
        final Context<Member> current = vState.getCurrentView();
        if (current == null) {
            throw new IllegalStateException("The current view is undefined, unable to process transactions");
        }

        byte[] nonce = new byte[32];
        entropy().nextBytes(nonce);
        int tsize = 0;
        for (byte[] t : transactions) {
            tsize += t.length;
        }
        ByteBuffer signed = ByteBuffer.allocate(1 + nonce.length + HashKey.BYTE_SIZE + tsize);
        signed.put(join ? (byte) 1 : (byte) 0);
        signed.put(params.member.getId().bytes());
        signed.put(nonce);

        Transaction.Builder builder = Transaction.newBuilder()
                                                 .setSource(params.member.getId().toByteString())
                                                 .setNonce(ByteString.copyFrom(nonce));
        for (byte[] t : transactions) {
            builder.addBatch(ByteString.copyFrom(t));
            signed.put(t);
        }

        byte[] hash = Conversion.hashOf(signed.array());

        byte[] signature = sign(params.signature.get(), entropy(), hash);
        if (signature == null) {
            throw new IllegalStateException("Unable to sign transaction batch");
        }
        builder.setSignature(ByteString.copyFrom(signature));
        HashKey hashKey = new HashKey(hash);
        Transaction transaction = builder.build();

        submitted.put(hashKey, new SubmittedTransaction(transaction, onCompletion));
        int toleranceLevel = toleranceLevel();
        SubmitTransaction submittedTxn = SubmitTransaction.newBuilder()
                                                          .setContext(current.getId().toByteString())
                                                          .setTransaction(transaction)
                                                          .build();
        log.info("Submitting txn: {} from {}", hashKey, getMember());
        List<TransactionResult> results = current.ring(entropy().nextInt(current.getRingCount())).stream().map(c -> {
            if (!getMember().equals(c)) {
                ConsortiumClientCommunications link = linkFor(c);
                if (link == null) {
                    log.warn("Cannot get link for {}", c.getId());
                    return null;
                }
                try {
                    return link.clientSubmit(submittedTxn);
                } catch (Throwable t) {
                    log.warn("Cannot submit txn {} to {}: {}", hashKey, c, t.getMessage());
                    return null;
                }
            } else {
                transitions.submit(new EnqueuedTransaction(new HashKey(hash), transaction));
                return TransactionResult.getDefaultInstance();
            }
        }).filter(r -> r != null).limit(toleranceLevel).collect(Collectors.toList());

        if (results.size() < toleranceLevel) {
            throw new TimeoutException("Cannot submit transaction " + hashKey);
        }
        return hashKey;
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

        log.debug("Installing new view rings: {} ttl: {}", newView.getRingCount(), newView.timeToLive());

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
                    log.info("invalid genesis view member, cannot deserialize consensus key for: {}", memberID);
                    return;
                }
                validators.computeIfAbsent(memberID, k -> consensusKey);
            }
        });

        if (newView.getMember(params.member.getId()) != null) { // cohort member
            Messenger nextMsgr = new Messenger(params.member, params.signature, newView, params.communications,
                    params.msgParameters);
            vState.setMessenger(nextMsgr);
            nextMsgr.register(round -> transitions.context().tick(round));
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
