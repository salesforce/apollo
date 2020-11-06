/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.nio.ByteBuffer;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static class Collaborator extends Member {
        public final PublicKey consensusKey;

        public Collaborator(Member member, byte[] consensusKey) {
            this(member, publicKeyOf(consensusKey));
        }

        public Collaborator(Member member, PublicKey consensusKey) {
            super(member.getId(), member.getCertificate());
            this.consensusKey = consensusKey;
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
            getState().submit(new PendingTransactions.EnqueuedTransaction(hash, request.getTransaction()));
            return TransactionResult.getDefaultInstance();
        }

    }

    class Client extends State {

        @Override
        void deliverBlock(Block parseFrom) {
            throw new IllegalStateException("client does not participate in the consortium");
        }

        @Override
        void deliverPersist(ID hash) {
            throw new IllegalStateException("client does not participate in the consortium");
        }

        @Override
        void deliverTransaction(Transaction txn) {
            throw new IllegalStateException("client does not participate in the consortium");
        }

        @Override
        void deliverValidate(Validate validation) {
            throw new IllegalStateException("client does not participate in the consortium");
        }

        @Override
        void submit(PendingTransactions.EnqueuedTransaction enqueuedTransaction) {
            throw new IllegalStateException("client does not participate in the consortium");
        }
    }

    abstract class CommitteeMember extends State {
        final PendingTransactions pending             = new PendingTransactions();
        volatile Block.Builder    workingBlock;
        final Set<Certification>  workingCertificates = new HashSet<>();

        @Override
        public void deliverTransaction(Transaction txn) {
            HashKey hash = new HashKey(Conversion.hashOf(txn.toByteArray()));
            pending.add(new PendingTransactions.EnqueuedTransaction(hash, txn));
        }

        @Override
        void deliverBlock(Block parseFrom) {
            // TODO Auto-generated method stub
        }

        @Override
        void deliverPersist(ID hash) {
            // TODO Auto-generated method stub
        }

        @Override
        void deliverValidate(Validate validation) {
            // TODO Auto-generated method stub
        }

        @Override
        void submit(PendingTransactions.EnqueuedTransaction enqueuedTransaction) {
            if (pending.add(enqueuedTransaction)) {
                log.info("Submitted txn: {}", enqueuedTransaction.hash);
                deliver(ConsortiumMessage.newBuilder()
                                         .setMsg(enqueuedTransaction.transaction.toByteString())
                                         .setType(MessageType.TRANSACTION)
                                         .build());
            }
        }
    }

    static class CurrentBlock {
        final Block   block;
        final HashKey hash;

        CurrentBlock(HashKey hash, Block block) {
            this.hash = hash;
            this.block = block;
        }
    }

    class Follower extends CommitteeMember {
    }

    class Leader extends CommitteeMember {
    }

    abstract class State {

        boolean becomeClient() {
            vState.state = new Client();
            return true;
        };

        boolean becomeFollower() {
            vState.state = new Follower();
            return true;
        }

        boolean becomeLeader() {
            vState.state = new Leader();
            return true;
        }

        abstract void deliverBlock(Block parseFrom);

        abstract void deliverPersist(ID hash);

        abstract void deliverTransaction(Transaction txn);

        abstract void deliverValidate(Validate validation);

        abstract void submit(PendingTransactions.EnqueuedTransaction enqueuedTransaction);

    }

    private class VolatileState implements MembershipListener<Member> {
        private volatile CommonCommunications<ConsortiumClientCommunications, Service> comm;
        private volatile CurrentBlock                                                  current;
        private volatile Context<Collaborator>                                         currentView;
        @SuppressWarnings("unused")
        private volatile Member                                                        leader;
        private volatile Messenger                                                     messenger;
        private volatile State                                                         state = new Client();
        private volatile TotalOrder                                                    to;

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

    private final static Logger log = LoggerFactory.getLogger(Consortium.class);

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

    public static PublicKey publicKeyOf(byte[] consensusKey) {
        return null;
    }

    private final Function<HashKey, CommonCommunications<ConsortiumClientCommunications, Service>> createClientComms;

    private final Parameters                         parameters;
    private final Map<HashKey, SubmittedTransaction> submitted = new ConcurrentHashMap<>();
    private volatile int                             toleranceLevel;
    private final VolatileState                      vState    = new VolatileState();

    public Consortium(Parameters parameters) {
        this.parameters = parameters;
        this.createClientComms = k -> parameters.communications.create(parameters.member, k, new Service(),
                                                                       r -> new ConsortiumServerCommunications(
                                                                               parameters.communications.getClientIdentityProvider(),
                                                                               null, r),
                                                                       ConsortiumClientCommunications.getCreate(null));
        parameters.context.register(vState);
    }

    public Member getMember() {
        return parameters.member;
    }

    public boolean process(CertifiedBlock certifiedBlock) {
        Block block = certifiedBlock.getBlock();
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteArray()));
        log.info("Processing block {} : {}", hash, block.getBody().getType());
        if (!validate(certifiedBlock)) {
            log.error("Protocol violation. New block is not certified {}", certifiedBlock);
            return false;
        }
        final CurrentBlock previousBlock = vState.current;
        if (previousBlock != null) {
            if (block.getHeader().getHeight() != previousBlock.block.getHeader().getHeight() + 1) {
                log.error("Protocol violation.  Block height should be {} and next block height is {}",
                          previousBlock.block.getHeader().getHeight(), block.getHeader().getHeight());
                return false;
            }
            HashKey prev = new HashKey(block.getHeader().getPrevious().toByteArray());
            if (previousBlock.hash.equals(prev)) {
                log.error("Protocol violation. New block does not refer to current block hash. Should be {} and next block's prev is {}",
                          previousBlock.hash, prev);
                return false;
            }
        } else {
            if (block.getBody().getType() != BodyType.GENESIS) {
                log.error("Invalid genesis block: {}", block.getBody().getType());
                return false;
            }
        }
        vState.current = new CurrentBlock(hash, block);
        return next();
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

        Signature s = parameters.signature.get();
        try {
            s.update(hash);
        } catch (SignatureException e) {
            throw new IllegalStateException("Unable to sign transaction batch", e);
        }
        try {
            builder.setSignature(ByteString.copyFrom(s.sign()));
        } catch (SignatureException e) {
            throw new IllegalStateException("Unable to sign transaction batch", e);
        }
        HashKey hashKey = new HashKey(hash);
        Transaction transaction = builder.build();
        submitted.put(hashKey, new SubmittedTransaction(transaction, onCompletion));
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

    State getState() {
        final State get = vState.state;
        return get;
    }

    private void deliver(ConsortiumMessage message) {
        final Messenger currentMsgr = vState.messenger;
        if (currentMsgr == null) {
            log.error("skipping message publish as no messenger");
            return;
        }
        currentMsgr.publish(message.toByteArray());
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

    private boolean next() {
        CurrentBlock next = vState.current;
        switch (next.block.getBody().getType()) {
        case CHECKPOINT:
            return processCheckpoint(next);
        case GENESIS:
            return processGenesis(next);
        case RECONFIGURE:
            return processReconfigure(next);
        case USER:
            return processUser(next);
        case UNRECOGNIZED:
        default:
            log.info("Unrecognized block type: {} : {}", next.hashCode(), next.block);
            return false;
        }

    }

    private void process(Msg msg) {
        log.info("Processing {} from {}", msg.sequenceNumber, msg.from);
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
                getState().deliverBlock(Block.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid block delivered from {}", msg.from, e);
            }
            break;
        case PERSIST:
            try {
                getState().deliverPersist(ID.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid persist delivered from {}", msg.from, e);
            }
            break;
        case TRANSACTION:
            try {
                getState().deliverTransaction(Transaction.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid transaction delivered from {}", msg.from, e);
            }
            break;
        case VALIDATE:
            try {
                getState().deliverValidate(Validate.parseFrom(message.getMsg().asReadOnlyByteBuffer()));
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

    private boolean processCheckpoint(CurrentBlock next) {
        Checkpoint body;
        try {
            body = Checkpoint.parseFrom(next.block.getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            log.error("Protocol violation.  Cannot decode checkpoint body: {}", e);
            return false;
        }
        return body != null;
    }

    private boolean processGenesis(CurrentBlock next) {
        Genesis body;
        try {
            body = Genesis.parseFrom(next.block.getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            log.error("Protocol violation.  Cannot decode genesis body: {}", e);
            return false;
        }
        return reconfigure(body.getInitialView());
    }

    private boolean processReconfigure(CurrentBlock next) {
        Reconfigure body;
        try {
            body = Reconfigure.parseFrom(next.block.getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            log.error("Protocol violation.  Cannot decode reconfiguration body: {}", e);
            return false;
        }
        return reconfigure(body);
    }

    private boolean processUser(CurrentBlock next) {
        User body;
        try {
            body = User.parseFrom(next.block.getBody().getContents());
        } catch (InvalidProtocolBufferException e) {
            log.error("Protocol violation.  Cannot decode reconfiguration body: {}", e);
            return false;
        }
        return body != null;
    }

    private boolean reconfigure(Reconfigure body) {
        HashKey viewId = new HashKey(body.getId());
        Context<Collaborator> newView = new Context<Collaborator>(viewId, parameters.context.getRingCount());
        body.getViewList().stream().map(v -> {
            HashKey memberId = new HashKey(v.getId());
            Member m = parameters.context.getMember(memberId);
            if (m == null) {
                return null;
            }
            return new Collaborator(m, v.getConsensusKey().toByteArray());
        }).filter(m -> m != null).forEach(m -> {
            if (parameters.context.isActive(m)) {
                newView.activate(m);
            } else {
                newView.offline(m);
            }
        });

        return viewChange(newView, body.getToleranceLevel());
    }

    private boolean validate(CertifiedBlock block) {
        // TODO Auto-generated method stub
        return true;
    }

    /**
     * Ye Jesus Nut
     */
    private boolean viewChange(Context<Collaborator> newView, int t) {
        vState.pause();

        vState.currentView = newView;
        toleranceLevel = t;
        vState.comm = createClientComms.apply(newView.getId());

        // Live successor of the view ID on ring zero is leader
        Collaborator newLeader = newView.ring(0).successor(newView.getId());
        vState.leader = newLeader;

        if (newView.getMember(parameters.member.getId()) != null) { // cohort member
            vState.messenger = new Messenger(parameters.member, parameters.signature, newView,
                    parameters.communications, parameters.msgParameters);
            vState.to = new TotalOrder((m, k) -> process(m), newView);
            vState.messenger.register(0, messages -> {
                vState.to.process(messages);
            });
            if (parameters.member.equals(newLeader)) { // I yam what I yam
                log.info("reconfiguring, becoming leader: {}", parameters.member);
                if (!getState().becomeLeader()) {
                    return false;
                }
            }
            log.info("reconfiguring, becoming follower: {}", parameters.member);
            if (!getState().becomeFollower()) { // I'm here for you, bruh
                return false;
            }
        } else { // you are all my puppets
            vState.messenger = null;
            vState.to = null;

            log.info("reconfiguring, becoming client: {}", parameters.member);
            if (!getState().becomeClient()) {
                return false;
            }
        }

        vState.resume();
        return true;
    }
}
