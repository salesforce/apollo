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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
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
import com.salesforce.apollo.membership.messaging.Messenger.Parameters;
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

    public class Service {

        public TransactionResult clientSubmit(SubmitTransaction request) {
            return TransactionResult.getDefaultInstance();
        }

    }

    public static class SubmittedTransaction {
        public final Consumer<HashKey> onCompletion;
        public final Transaction       submitted;

        public SubmittedTransaction(Transaction submitted, Consumer<HashKey> onCompletion) {
            this.submitted = submitted;
            this.onCompletion = onCompletion;
        }
    }

    private class Client extends State {
    }

    @SuppressWarnings("unused")
    private abstract class CommitteeMember extends State {
        private final Deque<CertifiedBlock> pending             = new ArrayDeque<>();
        private final Deque<Transaction>    transactions        = new ArrayDeque<>();
        private volatile Block.Builder      workingBlock;
        private final Set<Certification>    workingCertificates = new HashSet<>();

        @Override
        public void deliverTransaction(Transaction txn, HashKey from) {
            transactions.add(txn);
        }
    }

    private static class CurrentBlock {
        final Block   block;
        final HashKey hash;

        CurrentBlock(HashKey hash, Block block) {
            this.hash = hash;
            this.block = block;
        }
    }

    private class Follower extends CommitteeMember {
    }

    private class Leader extends CommitteeMember {
    }

    private abstract class State {

        boolean becomeClient() {
            state = new Client();
            return true;
        };

        boolean becomeFollower() {
            state = new Follower();
            return true;
        }

        boolean becomeLeader() {
            state = new Leader();
            return true;
        }

        void deliverBlock(Block parseFrom, HashKey from) {
            // TODO Auto-generated method stub
        }

        void deliverPersist(ID parseFrom, HashKey from) {
            // TODO Auto-generated method stub
        }

        void deliverTransaction(Transaction parseFrom, HashKey from) {
            // TODO Auto-generated method stub
        }

        void deliverValidate(Validate parseFrom, HashKey from) {
            // TODO Auto-generated method stub
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

    private volatile CommonCommunications<ConsortiumClientCommunications, Service>                 comm;
    private final Router                                                                           communications;
    private final Context<Member>                                                                  context;
    private final Function<HashKey, CommonCommunications<ConsortiumClientCommunications, Service>> createClientComms;
    private volatile CurrentBlock                                                                  current;
    private volatile Context<Collaborator>                                                         currentView;
    @SuppressWarnings("unused")
    private final Function<List<Transaction>, List<ByteBuffer>>                                    executor;
    private final Duration                                                                         gossipDuration;
    private final AtomicInteger                                                                    lastSequenceNumber = new AtomicInteger(
            -1);
    private volatile Member                                                                        leader;
    private final Parameters                                                                       msgParameters;
    private final Member                                                                           member;
    private volatile Messenger                                                                     messenger;
    private final ScheduledExecutorService                                                         scheduler;
    private final Supplier<Signature>                                                              signature;
    private volatile State                                                                         state              = new Client();
    private final Map<HashKey, SubmittedTransaction>                                               submitted          = new ConcurrentHashMap<>();
    private volatile int                                                                           toleranceLevel;
    private volatile TotalOrder                                                                    totalOrder;

    @SuppressWarnings("unchecked")
    public Consortium(Function<List<Transaction>, List<ByteBuffer>> executor, Member member,
            Supplier<Signature> signature, Context<? extends Member> ctx, Parameters msgParameters,
            Router communications, Duration gossipDuration, ScheduledExecutorService scheduler) {
        this.executor = executor;
        this.member = member;
        this.context = (Context<Member>) ctx;
        this.msgParameters = msgParameters;
        this.communications = communications;
        this.signature = signature;
        this.gossipDuration = gossipDuration;
        this.scheduler = scheduler;
        this.createClientComms = k -> communications.create(member, k, new Service(),
                                                            r -> new ConsortiumServerCommunications(
                                                                    communications.getClientIdentityProvider(), null,
                                                                    r),
                                                            ConsortiumClientCommunications.getCreate(null));
        context.register(membershipListener());
    }

    public Member getMember() {
        return member;
    }

    public boolean process(CertifiedBlock certifiedBlock) {
        Block block = certifiedBlock.getBlock();
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteArray()));
        log.info("Processing block {} : {}", hash, block.getBody().getType());
        if (!validate(certifiedBlock)) {
            log.error("Protocol violation. New block is not certified {}", certifiedBlock);
            return false;
        }
        final CurrentBlock previousBlock = current;
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
        current = new CurrentBlock(hash, block);
        return next();
    }

    public void start() {
        resume();
    }

    public void stop() {
        pause();
    }

    public HashKey submit(List<byte[]> transactions, Consumer<HashKey> onCompletion) throws TimeoutException {
        final Context<Collaborator> current = currentView;
        if (current == null) {
            throw new IllegalStateException("The current view is undefined, unable to process transactions");
        }

        byte[] nonce = new byte[32];
        msgParameters.entropy.nextBytes(nonce);
        ByteBuffer signed = ByteBuffer.allocate(nonce.length + HashKey.BYTE_SIZE
                + transactions.stream().mapToInt(e -> e.length).sum());

        signed.put(member.getId().bytes());
        signed.put(nonce);

        Transaction.Builder builder = Transaction.newBuilder()
                                                 .setSource(member.getId().toByteString())
                                                 .setNonce(ByteString.copyFrom(nonce));
        transactions.forEach(t -> {
            builder.addBatch(ByteString.copyFrom(t));
            signed.put(t);
        });

        byte[] hash = Conversion.hashOf(signed.array());

        Signature s = signature.get();
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
        List<TransactionResult> results = current.ring(0).stream().map(c -> {
            ConsortiumClientCommunications link = linkFor(c);
            if (link == null) {
                log.warn("Cannot get link for {}", c.getId());
                return null;
            }
            return link.clientSubmit(SubmitTransaction.newBuilder()
                                                      .setContext(current.getId().toByteString())
                                                      .setTransaction(transaction)
                                                      .build());
        }).filter(r -> r != null).limit(toleranceLevel).collect(Collectors.toList());
        if (results.size() < toleranceLevel) {
            throw new TimeoutException("Cannot submit transaction " + hashKey);
        }
        return hashKey;
    }

    @SuppressWarnings("unused")
    private void deliver(ConsortiumMessage.Builder message) {
        final Messenger currentMsgr = messenger;
        if (currentMsgr == null) {
            log.error("skipping message publish as no messenger");
            return;
        }
        message.setSequenceNumber(lastSequenceNumber.incrementAndGet());
        messenger.publish(0, message.build().toByteArray());
    }

    private State getState() {
        final State get = state;
        return get;
    }

    private ConsortiumClientCommunications linkFor(Member m) {
        try {
            return comm.apply(m, member);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private MembershipListener<Member> membershipListener() {
        return new MembershipListener<Member>() {

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
        };
    }

    private boolean next() {
        CurrentBlock next = current;
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

    private void pause() {
        CommonCommunications<ConsortiumClientCommunications, Service> currentComm = comm;
        if (currentComm != null) {
            Context<Collaborator> current = currentView;
            assert current != null : "No current view, but comm exists!";
            currentComm.deregister(current.getId());
        }

        TotalOrder currentTotalOrder = totalOrder;
        if (currentTotalOrder != null) {
            currentTotalOrder.stop();
        }
        Messenger currentMessenger = messenger;
        if (currentMessenger != null) {
            currentMessenger.stop();
        }
    }

    private void process(ConsortiumMessage message, HashKey from) {
        switch (message.getType()) {
        case BLOCK:
            try {
                getState().deliverBlock(Block.parseFrom(message.getMsg().asReadOnlyByteBuffer()), from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid block delivered from {}", from, e);
            }
            break;
        case PERSIST:
            try {
                getState().deliverPersist(ID.parseFrom(message.getMsg().asReadOnlyByteBuffer()), from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid persist delivered from {}", from, e);
            }
            break;
        case TRANSACTION:
            try {
                getState().deliverTransaction(Transaction.parseFrom(message.getMsg().asReadOnlyByteBuffer()), from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid transaction delivered from {}", from, e);
            }
            break;
        case VALIDATE:
            try {
                getState().deliverValidate(Validate.parseFrom(message.getMsg().asReadOnlyByteBuffer()), from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid validate delivered from {}", from, e);
            }
            break;
        case UNRECOGNIZED:
        default:
            log.error("Invalid consortium message type: {} from: {}", message.getType(), from);
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
        Context<Collaborator> newView = new Context<Collaborator>(viewId, context.getRingCount());
        body.getViewList().stream().map(v -> {
            HashKey memberId = new HashKey(v.getId());
            Member m = context.getMember(memberId);
            if (m == null) {
                return null;
            }
            return new Collaborator(m, v.getConsensusKey().toByteArray());
        }).filter(m -> m != null).forEach(m -> {
            if (context.isActive(m)) {
                log.trace("Collaborator {} is active", m);
                newView.activate(m);
            } else {
                log.trace("Collaborator {} is offline", m);
                newView.offline(m);
            }
        });

        return viewChange(newView, body.getToleranceLevel());
    }

    private void resume() {
        CommonCommunications<ConsortiumClientCommunications, Service> currentComm = comm;
        if (currentComm != null) {
            Context<Collaborator> current = currentView;
            assert current != null : "No current view, but comm exists!";
            currentComm.register(current.getId(), new Service());
        }
        TotalOrder currentTO = totalOrder;
        if (currentTO != null) {
            currentTO.start();
        }
        Messenger currentMsg = messenger;
        if (currentMsg != null) {
            currentMsg.start(gossipDuration, scheduler);
        }
    }

    private boolean validate(CertifiedBlock block) {
        // TODO Auto-generated method stub
        return true;
    }

    private boolean viewChange(Context<Collaborator> newView, int t) {
        pause();

        currentView = newView;
        log.trace("New view: {}", newView);
        lastSequenceNumber.set(-1);
        toleranceLevel = t;
        comm = createClientComms.apply(newView.getId());

        // Live successor of the view ID on ring zero is leader
        Collaborator newLeader = newView.ring(0).successor(newView.getId());
        leader = newLeader;

        if (newView.getMember(member.getId()) != null) {
            log.info("Joining group {}", newView);
            messenger = new Messenger(member, signature, newView, communications, msgParameters);
            totalOrder = new TotalOrder((m, mId) -> process(m, mId), newView);
            messenger.register(0, messages -> {
                totalOrder.process(messages);
            });
            if (member.equals(newLeader)) {
                log.info("reconfiguring, becoming leader: {}", member);
                return getState().becomeLeader();
            }
            log.info("reconfiguring, becoming follower: {}", member);
            return getState().becomeFollower();
        }
        messenger = null;
        totalOrder = null;
        log.info("reconfiguring, becoming client: {}", member);
        getState().becomeClient();
        resume();
        return true;
    }
}
