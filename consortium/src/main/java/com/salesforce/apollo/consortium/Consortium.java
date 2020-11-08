/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.Signature;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.common.base.Supplier;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.Certification;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.ConsortiumMessage;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.MessageType;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesfoce.apollo.consortium.proto.User;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesfoce.apollo.proto.ID;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.PendingTransactions.EnqueuedTransaction;
import com.salesforce.apollo.consortium.comms.ConsortiumClientCommunications;
import com.salesforce.apollo.consortium.comms.ConsortiumServerCommunications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.membership.messaging.Messenger.MessageChannelHandler.Msg;
import com.salesforce.apollo.membership.messaging.TotalOrder;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Consortium {

    public class CollaboratorContext {
        private final PendingTransactions                  pending       = new PendingTransactions();
        private final TickScheduler                        scheduler     = new TickScheduler();
        private final Map<HashKey, CertifiedBlock.Builder> workingBlocks = new HashMap<>();

        public void add(Transaction txn) {
            HashKey hash = new HashKey(Conversion.hashOf(txn.toByteArray()));
            pending.add(new EnqueuedTransaction(hash, txn));
        }

        public void deliverBlock(Block block, Member from) {
            Member leader = vState.leader;
            if (!from.equals(leader)) {
                log.debug("Rejecting block proposal from {}", from);
                return;
            }
            HashKey hash = new HashKey(Conversion.hashOf(block.toByteArray()));
            workingBlocks.computeIfAbsent(hash, k -> CertifiedBlock.newBuilder().setBlock(block));
            generateValidation(hash, block);
        }

        public void generateConsensusKeyPair() {
            vState.consensusKeyPair = Validator.generateKeyPair(2048, "RSA");
        }

        public Member member() {
            return parameters.member;
        }

        public boolean processCheckpoint(CurrentBlock next) {
            Checkpoint body;
            try {
                body = Checkpoint.parseFrom(next.getBlock().getBody().getContents());
            } catch (InvalidProtocolBufferException e) {
                log.error("Protocol violation.  Cannot decode checkpoint body: {}", e);
                return false;
            }
            return body != null;
        }

        public boolean processGenesis(CurrentBlock next) {
            Genesis body;
            try {
                body = Genesis.parseFrom(next.getBlock().getBody().getContents());
            } catch (InvalidProtocolBufferException e) {
                log.error("Protocol violation.  Cannot decode genesis body: {}", e);
                return false;
            }
            transitions.genesisAccepted();
            return reconfigure(body.getInitialView());
        }

        public boolean processReconfigure(CurrentBlock next) {
            Reconfigure body;
            try {
                body = Reconfigure.parseFrom(next.getBlock().getBody().getContents());
            } catch (InvalidProtocolBufferException e) {
                log.error("Protocol violation.  Cannot decode reconfiguration body: {}", e);
                return false;
            }
            return reconfigure(body);
        }

        public boolean processUser(CurrentBlock next) {
            User body;
            try {
                body = User.parseFrom(next.getBlock().getBody().getContents());
            } catch (InvalidProtocolBufferException e) {
                log.error("Protocol violation.  Cannot decode reconfiguration body: {}", e);
                return false;
            }
            return body != null;
        }

        public void schedudule(Runnable action, int delta) {
            scheduler.schedule(action, delta);
        }

        public void submit(EnqueuedTransaction enqueuedTransaction) {
            if (pending.add(enqueuedTransaction)) {
                log.debug("Submitted txn: {}", enqueuedTransaction.getHash());
                deliver(ConsortiumMessage.newBuilder()
                                         .setMsg(enqueuedTransaction.getTransaction().toByteString())
                                         .setType(MessageType.TRANSACTION)
                                         .build());
            }
        }

        public void tick() {
            scheduler.tick();
        }

        public void validate(Validate v) {
            HashKey hash = new HashKey(v.getHash());
            CertifiedBlock.Builder certifiedBlock = workingBlocks.get(hash);
            if (certifiedBlock == null) {
                log.trace("No working block to validate: {}", hash);
                return;
            }
            final Validator validator = vState.validator;
            ForkJoinPool.commonPool().execute(() -> {
                if (validator.validate(certifiedBlock.getBlock(), v)) {
                    certifiedBlock.addCertifications(Certification.newBuilder()
                                                                  .setId(v.getId())
                                                                  .setSignature(v.getSignature()));
                }
            });
        }

        PendingTransactions getPending() {
            return pending;
        }
    }

    public static class Parameters {
        public static class Builder {
            private Router                                        communications;
            private Context<Member>                               context;
            private Function<List<Transaction>, List<ByteBuffer>> executor;
            private Duration                                      gossipDuration;
            private Member                                        member;
            private Messenger.Parameters                          msgParameters;
            private ScheduledExecutorService                      scheduler;
            private Supplier<Signature>                           signature;

            public Parameters build() {
                return new Parameters(context, communications, executor, member, msgParameters, scheduler, signature,
                        gossipDuration);
            }

            public Router getCommunications() {
                return communications;
            }

            public Context<Member> getContext() {
                return context;
            }

            public Function<List<Transaction>, List<ByteBuffer>> getExecutor() {
                return executor;
            }

            public Duration getGossipDuration() {
                return gossipDuration;
            }

            public Member getMember() {
                return member;
            }

            public Messenger.Parameters getMsgParameters() {
                return msgParameters;
            }

            public ScheduledExecutorService getScheduler() {
                return scheduler;
            }

            public Supplier<Signature> getSignature() {
                return signature;
            }

            public Builder setCommunications(Router communications) {
                this.communications = communications;
                return this;
            }

            @SuppressWarnings("unchecked")
            public Builder setContext(Context<? extends Member> context) {
                this.context = (Context<Member>) context;
                return this;
            }

            public Builder setExecutor(Function<List<Transaction>, List<ByteBuffer>> executor) {
                this.executor = executor;
                return this;
            }

            public Builder setGossipDuration(Duration gossipDuration) {
                this.gossipDuration = gossipDuration;
                return this;
            }

            public Builder setMember(Member member) {
                this.member = member;
                return this;
            }

            public Builder setMsgParameters(Messenger.Parameters msgParameters) {
                this.msgParameters = msgParameters;
                return this;
            }

            public Builder setScheduler(ScheduledExecutorService scheduler) {
                this.scheduler = scheduler;
                return this;
            }

            public Builder setSignature(Supplier<Signature> signature) {
                this.signature = signature;
                return this;
            }
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public final Duration                                       gossipDuration;
        private final Router                                        communications;
        private final Context<Member>                               context;
        @SuppressWarnings("unused")
        private final Function<List<Transaction>, List<ByteBuffer>> executor;
        private final Member                                        member;
        private final Messenger.Parameters                          msgParameters;
        private final ScheduledExecutorService                      scheduler;
        private final Supplier<Signature>                           signature;

        public Parameters(Context<Member> context, Router communications,
                Function<List<Transaction>, List<ByteBuffer>> executor, Member member,
                Messenger.Parameters msgParameters, ScheduledExecutorService scheduler, Supplier<Signature> signature,
                Duration gossipDuration) {
            this.context = context;
            this.communications = communications;
            this.executor = executor;
            this.member = member;
            this.msgParameters = msgParameters;
            this.scheduler = scheduler;
            this.signature = signature;
            this.gossipDuration = gossipDuration;
        }
    }

    public class Service {

        public TransactionResult clientSubmit(SubmitTransaction request) {
            HashKey hash = new HashKey(Conversion.hashOf(request.getTransaction().toByteArray()));
            transitions.submit(new EnqueuedTransaction(hash, request.getTransaction()));
            return TransactionResult.getDefaultInstance();
        }

    }

    private class VolatileState implements MembershipListener<Member> {
        private volatile CommonCommunications<ConsortiumClientCommunications, Service> comm;
        private volatile KeyPair                                                       consensusKeyPair;
        private volatile CurrentBlock                                                  current;
        private volatile Context<Collaborator>                                         currentView;
        private volatile Member                                                        leader;
        private volatile Messenger                                                     messenger;
        private volatile TotalOrder                                                    to;
        private volatile int                                                           toleranceLevel;
        private volatile Validator                                                     validator;

        @Override
        public void fail(Member member) {
            final Context<Collaborator> view = currentView;
            if (view != null) {
                view.offlineIfActive(member.getId());
            }
        }

        @Override
        public void recover(Member member) {
            final Context<Collaborator> view = currentView;
            if (view != null) {
                view.activateIfOffline(member.getId());
            }
        }

        private void pause() {
            CommonCommunications<ConsortiumClientCommunications, Service> currentComm = comm;
            if (currentComm != null) {
                Context<Collaborator> current = currentView;
                assert current != null : "No current view, but comm exists!";
                currentComm.deregister(current.getId());
            }

            TotalOrder currentTotalOrder = to;
            if (currentTotalOrder != null) {
                currentTotalOrder.stop();
            }
            Messenger currentMessenger = messenger;
            if (currentMessenger != null) {
                currentMessenger.stop();
            }
        }

        private void resume() {
            CommonCommunications<ConsortiumClientCommunications, Service> currentComm = comm;
            if (currentComm != null) {
                Context<Collaborator> current = currentView;
                assert current != null : "No current view, but comm exists!";
                currentComm.register(current.getId(), new Service());
            }
            TotalOrder currentTO = to;
            if (currentTO != null) {
                currentTO.start();
            }
            Messenger currentMsg = messenger;
            if (currentMsg != null) {
                currentMsg.start(parameters.gossipDuration, parameters.scheduler);
            }
        }

    }

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

    private final Function<HashKey, CommonCommunications<ConsortiumClientCommunications, Service>> createClientComms;
    private final Fsm<CollaboratorContext, Transitions>                                            fsm;
    private Logger                                                                                 log       = DEFAULT_LOGGER;
    private final Parameters                                                                       parameters;
    private final Map<HashKey, SubmittedTransaction>                                               submitted = new ConcurrentHashMap<>();
    private final Transitions                                                                      transitions;
    private final VolatileState                                                                    vState    = new VolatileState();

    public Consortium(Parameters parameters) {
        this.parameters = parameters;
        this.createClientComms = k -> parameters.communications.create(parameters.member, k, new Service(),
                                                                       r -> new ConsortiumServerCommunications(
                                                                               parameters.communications.getClientIdentityProvider(),
                                                                               null, r),
                                                                       ConsortiumClientCommunications.getCreate(null));
        parameters.context.register(vState);
        fsm = Fsm.construct(new CollaboratorContext(), Transitions.class, CollaboratorFsm.INITIAL, true);
        transitions = fsm.getTransitions();
    }

    public Logger getLog() {
        return log;
    }

    public Member getMember() {
        return parameters.member;
    }

    public CollaboratorContext getState() {
        return fsm.getContext();
    }

    public void process(CertifiedBlock certifiedBlock) {
        Block block = certifiedBlock.getBlock();
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteArray()));
        log.info("Processing block {} : {}", hash, block.getBody().getType());
        final CurrentBlock previousBlock = vState.current;
        final Validator v = vState.validator;
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
            if (!v.validate(certifiedBlock)) {
                log.error("Protocol violation. New block is not validated {}", certifiedBlock);
                return;
            }
        } else {
            if (block.getBody().getType() != BodyType.GENESIS) {
                log.error("Invalid genesis block: {}", block.getBody().getType());
                return;
            } 
            if (!Validator.validateGenesis(certifiedBlock)) {
                log.error("Protocol violation. Genesis block is not validated {}", hash);
                return;
            }
        }
        vState.current = new CurrentBlock(hash, block);
        next();
    }

    public void setLog(Logger log) {
        this.log = log;
    }

    public void start() {
        vState.resume();
    }

    public void stop() {
        vState.pause();
    }

    public HashKey submit(List<byte[]> transactions, Consumer<HashKey> onCompletion) throws TimeoutException {
        final Context<Collaborator> current = vState.currentView;
        if (current == null) {
            throw new IllegalStateException("The current view is undefined, unable to process transactions");
        }

        byte[] nonce = new byte[32];
        parameters.msgParameters.entropy.nextBytes(nonce);
        ByteBuffer signed = ByteBuffer.allocate(nonce.length + HashKey.BYTE_SIZE
                + transactions.stream().mapToInt(e -> e.length).sum());

        signed.put(parameters.member.getId().bytes());
        signed.put(nonce);

        Transaction.Builder builder = Transaction.newBuilder()
                                                 .setSource(parameters.member.getId().toByteString())
                                                 .setNonce(ByteString.copyFrom(nonce));
        transactions.forEach(t -> {
            builder.addBatch(ByteString.copyFrom(t));
            signed.put(t);
        });

        byte[] hash = Conversion.hashOf(signed.array());

        byte[] signature = Validator.sign(parameters.signature.get(), parameters.msgParameters.entropy, hash);
        if (signature == null) {
            throw new IllegalStateException("Unable to sign transaction batch");
        }
        builder.setSignature(ByteString.copyFrom(signature));
        HashKey hashKey = new HashKey(hash);
        Transaction transaction = builder.build();
        submitted.put(hashKey, new SubmittedTransaction(transaction, onCompletion));
        int toleranceLevel = vState.toleranceLevel;
        List<TransactionResult> results = current.sample(toleranceLevel + 1, parameters.msgParameters.entropy,
                                                         parameters.member.getId())
                                                 .stream()
                                                 .map(c -> {
                                                     ConsortiumClientCommunications link = linkFor(c);
                                                     if (link == null) {
                                                         log.warn("Cannot get link for {}", c.getId());
                                                         return null;
                                                     }
                                                     return link.clientSubmit(SubmitTransaction.newBuilder()
                                                                                               .setContext(current.getId()
                                                                                                                  .toByteString())
                                                                                               .setTransaction(transaction)
                                                                                               .build());
                                                 })
                                                 .filter(r -> r != null)
                                                 .limit(toleranceLevel)
                                                 .collect(Collectors.toList());
        if (results.size() < toleranceLevel) {
            throw new TimeoutException("Cannot submit transaction " + hashKey);
        }
        return hashKey;
    }

    private void deliver(ConsortiumMessage message) {
        final Messenger currentMsgr = vState.messenger;
        if (currentMsgr == null) {
            log.error("skipping message publish as no messenger");
            return;
        }
        currentMsgr.publish(message.toByteArray());
    }

    private void generateValidation(HashKey hash, Block block) {
        byte[] signature = Validator.sign(vState.consensusKeyPair.getPrivate(), parameters.msgParameters.entropy,
                                          Conversion.hashOf(block.getHeader().toByteArray()));
        if (signature == null) {
            log.error("Unable to sign block {}", hash);
            return;
        }
        Validate validation;
        validation = Validate.newBuilder()
                             .setId(parameters.member.getId().toByteString())
                             .setHash(hash.toByteString())
                             .setSignature(ByteString.copyFrom(signature))
                             .build();
        vState.messenger.publish(ConsortiumMessage.newBuilder()
                                                  .setType(MessageType.VALIDATE)
                                                  .setMsg(validation.toByteString())
                                                  .build()
                                                  .toByteArray());
    }

    private ConsortiumClientCommunications linkFor(Member m) {
        try {
            return vState.comm.apply(m, parameters.member);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private void next() {
        CurrentBlock next = vState.current;
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

    private void process(Msg msg) {
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
            try {
                transitions.deliverPersist(ID.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid persist delivered from {}", msg.from, e);
            }
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
        case UNRECOGNIZED:
        default:
            log.error("Invalid consortium message type: {} from: {}", message.getType(), msg.from);
            break;
        }
    }

    private boolean reconfigure(Reconfigure body) {
        HashKey viewId = new HashKey(body.getId());
        Context<Collaborator> newView = new Context<Collaborator>(viewId, parameters.context.toleranceLevel() + 1);
        body.getViewList().stream().map(vm -> {
            HashKey memberId = new HashKey(vm.getId());
            Member m = parameters.context.getMember(memberId);
            if (m == null) {
                return null;
            }
            return new Collaborator(m, vm.getConsensusKey().toByteArray());
        }).filter(m -> m != null).forEach(m -> {
            if (parameters.context.isActive(m)) {
                newView.activate(m);
            } else {
                newView.offline(m);
            }
        });

        return viewChange(newView, body.getToleranceLevel());
    }

    /**
     * Ye Jesus Nut
     */
    private boolean viewChange(Context<Collaborator> newView, int t) {
        vState.pause();

        log.trace("View rings: {} ttl: {}", newView.getRingCount(), newView.timeToLive());

        vState.currentView = newView;
        vState.toleranceLevel = t;
        vState.comm = createClientComms.apply(newView.getId());
        vState.validator = new Validator(newView, t);

        // Live successor of the view ID on ring zero is leader
        Collaborator newLeader = newView.ring(0).successor(newView.getId());
        vState.leader = newLeader;

        if (newView.getMember(parameters.member.getId()) != null) { // cohort member
            Messenger nextMsgr = new Messenger(parameters.member, parameters.signature, newView,
                    parameters.communications, parameters.msgParameters);
            vState.messenger = nextMsgr;
            vState.to = new TotalOrder((m, k) -> process(m), newView);
            nextMsgr.register(() -> fsm.getContext().tick());
            nextMsgr.register(messages -> vState.to.process(messages));
            if (parameters.member.equals(newLeader)) { // I yam what I yam
                log.info("reconfiguring, becoming leader: {}", parameters.member);
                transitions.becomeLeader();
            } else {
                log.info("reconfiguring, becoming follower: {}", parameters.member);
                transitions.becomeFollower();
            }
        } else { // you are all my puppets
            vState.messenger = null;
            vState.to = null;

            log.info("reconfiguring, becoming client: {}", parameters.member);
            transitions.becomeClient();
        }

        vState.resume();
        return true;
    }
}
