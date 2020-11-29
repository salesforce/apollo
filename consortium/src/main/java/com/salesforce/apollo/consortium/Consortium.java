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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.Persist;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.TransactionOrBuilder;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesfoce.apollo.consortium.proto.ViewMember;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.comms.ConsortiumClientCommunications;
import com.salesforce.apollo.consortium.comms.ConsortiumServerCommunications;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
import com.salesforce.apollo.consortium.fsm.Transitions;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.MemberOrder;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.membership.messaging.Messenger.MessageHandler.Msg;
import com.salesforce.apollo.protocols.BbBackedInputStream;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Consortium {

    public class Service {

        public TransactionResult clientSubmit(SubmitTransaction request, HashKey from) {
            Member member = getParams().context.getMember(from);
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
                log.debug("Join transaction: {} on: {} from consortium member : {}", enqueuedTransaction.getHash(),
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
                    if (getNextView() == null) {
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
                    byte[] signed = sign(getParams().signature.get(), encoded);
                    if (signed == null) {
                        log.debug("Could not sign consensus key from {} on {}", fromID, getMember());
                        return JoinResult.getDefaultInstance();
                    }
                    return JoinResult.newBuilder()
                                     .setSignature(ByteString.copyFrom(signed))
                                     .setNextView(getNextView())
                                     .build();
                });
            } catch (Exception e) {
                log.error("Error voting for: {} on: {}", from, getMember(), e);
                return JoinResult.getDefaultInstance();
            }
        }

        public void replicate(ReplicateTransactions request, HashKey from) {
            Member member = viewContext().getMember(from);
            if (member == null) {
                log.warn("Received ReplicateTransactions from non consortium member: {} on: {}", from, getMember());
                return;
            }
            whileStable(new HashKey(request.getContext()), () -> transitions.deliverTransactions(request, member));
        }

        public void stop(Stop stop, HashKey from) {
            Member member = viewContext().getMember(from);
            if (member == null) {
                log.warn("Received Stop from non consortium member: {} on: {}", from, getMember());
                return;
            }
            whileStable(new HashKey(stop.getContext()), () -> transitions.deliverStop(stop, member));
        }

        public void stopData(StopData stopData, HashKey from) {
            Member member = viewContext().getMember(from);
            if (member == null) {
                log.warn("Received StopData from non consortium member: {} on: {}", from, getMember());
                return;
            }
            whileStable(new HashKey(stopData.getContext()), () -> transitions.deliverStopData(stopData, member));
        }

        public void sync(Sync sync, HashKey from) {
            Member member = viewContext().getMember(from);
            if (member == null) {
                log.warn("Received Sync from non consortium member: {} on: {}", from, getMember());
                return;
            }
            whileStable(new HashKey(sync.getContext()), () -> transitions.deliverSync(sync, member));
        }

    }

    public enum Timers {
        AWAIT_GENESIS, AWAIT_GENESIS_VIEW, AWAIT_GROUP, AWAIT_VIEW_MEMBERS, FLUSH_BATCH, PROCLAIM,
        TRANSACTION_TIMEOUT_1, TRANSACTION_TIMEOUT_2;
    }

    static class Result {
        public final Member     member;
        public final JoinResult vote;

        public Result(Member member, JoinResult vote) {
            this.member = member;
            this.vote = vote;
        }
    }

    public static final HashKey GENESIS_VIEW_ID = HashKey.ORIGIN.prefix("Genesis".getBytes());

    private static final Logger log = LoggerFactory.getLogger(Consortium.class);

    public static ByteString compress(ByteString input) {
        DeflaterInputStream dis = new DeflaterInputStream(
                BbBackedInputStream.aggregate(input.asReadOnlyByteBufferList()));
        try {
            return ByteString.readFrom(dis);
        } catch (IOException e) {
            log.error("Cannot compress input", e);
            return null;
        }
    }

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

    private volatile CommonCommunications<ConsortiumClientCommunications, Service>                 comm;
    private final Function<HashKey, CommonCommunications<ConsortiumClientCommunications, Service>> createClientComms;
    private volatile CurrentBlock                                                                  current;
    private final Fsm<CollaboratorContext, Transitions>                                            fsm;
    private final byte[]                                                                           genesisData = "Give me food or give me slack or kill me".getBytes();
    private volatile Messenger                                                                     messenger;
    private volatile ViewMember                                                                    nextView;
    private volatile KeyPair                                                                       nextViewConsensusKeyPair;
    private volatile MemberOrder                                                                   order;
    private final Parameters                                                                       params;
    private final TickScheduler                                                                    scheduler   = new TickScheduler();
    private final AtomicBoolean                                                                    started     = new AtomicBoolean();
    private final Map<HashKey, SubmittedTransaction>                                               submitted   = new ConcurrentHashMap<>();
    private final Transitions                                                                      transitions;
    private final ReadWriteLock                                                                    viewChange  = new ReentrantReadWriteLock();

    private volatile ViewContext viewContext;

    public Consortium(Parameters parameters) {
        this.params = parameters;
        this.createClientComms = k -> parameters.communications.create(parameters.member, k, new Service(),
                                                                       r -> new ConsortiumServerCommunications(
                                                                               parameters.communications.getClientIdentityProvider(),
                                                                               null, r),
                                                                       ConsortiumClientCommunications.getCreate(null));
        fsm = Fsm.construct(new CollaboratorContext(this), Transitions.class, CollaboratorFsm.INITIAL, true);
        fsm.setName(getMember().getId().b64Encoded());
        transitions = fsm.getTransitions();
        nextViewConsensusKey();
    }

    public Logger getLog() {
        return log;
    }

    public Member getMember() {
        return getParams().member;
    }

    public boolean process(CertifiedBlock certifiedBlock) {
        if (!started.get()) {
            return false;
        }
        Block block = certifiedBlock.getBlock();
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        log.debug("Processing block {} : {} on: {}", hash, block.getBody().getType(), getMember());
        final CurrentBlock previousBlock = getCurrent();
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
            if (!validateGenesis(hash, certifiedBlock, body.getInitialView(), getParams().context,
                                 viewContext().majority(), getMember())) {
                log.error("Protocol violation on: {}. Genesis block is not validated {}", getMember(), hash);
                return false;
            }
        }
        return next(new CurrentBlock(hash, block));
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        log.info("Starting consortium on {}", getMember());
        transitions.start();
        resume();
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping consortium on {}", getMember());
        clear();
        transitions.context().clear();
        transitions.stop();
    }

    public HashKey submit(Consumer<HashKey> onCompletion, Message... transactions) throws TimeoutException {
        return submit(false, onCompletion, transactions);

    }

    SecureRandom entropy() {
        return getParams().msgParameters.entropy;
    }

    void finalized(final EnqueuedTransaction finald) {
        final SubmittedTransaction previous = getSubmitted().remove(finald.getHash());
        if (previous != null) {
            ForkJoinPool.commonPool().execute(() -> {
                if (previous.onCompletion != null) {
                    log.info("finalizing: {} on: {}", finald.getHash(), getMember());
                    previous.onCompletion.accept(finald.getHash());
                }
            });
        }
    }

    // test access
    Fsm<CollaboratorContext, Transitions> fsm() {
        return fsm;
    }

    InputStream getBody(Block block) {
        return new InflaterInputStream(
                BbBackedInputStream.aggregate(block.getBody().getContents().asReadOnlyByteBufferList()));
    }

    CommonCommunications<ConsortiumClientCommunications, Service> getComm() {
        final CommonCommunications<ConsortiumClientCommunications, Service> cc = comm;
        return cc;
    }

    CurrentBlock getCurrent() {
        final CurrentBlock cb = current;
        return cb;
    }

    byte[] getGenesisData() {
        return genesisData;
    }

    Messenger getMessenger() {
        Messenger currentMsgr = messenger;
        return currentMsgr;
    }

    ViewMember getNextView() {
        final ViewMember c = nextView;
        return c;
    }

    KeyPair getNextViewConsensusKeyPair() {
        final KeyPair c = nextViewConsensusKeyPair;
        return c;
    }

    Parameters getParams() {
        return params;
    }

    TickScheduler getScheduler() {
        return scheduler;
    }

    // test access
    CollaboratorContext getState() {
        return fsm.getContext();
    }

    Map<HashKey, SubmittedTransaction> getSubmitted() {
        return submitted;
    }

    // test accessible
    Transitions getTransitions() {
        return transitions;
    }

    ReadWriteLock getViewChange() {
        return viewChange;
    }

    ViewContext getViewContext() {
        return viewContext;
    }

    void joinMessageGroup(ViewContext newView) {
        log.debug("Joining message group: {} on: {}", newView.getId(), getMember());
        Messenger nextMsgr = newView.createMessenger(getParams());
        setMessenger(nextMsgr);
        nextMsgr.register(round -> getScheduler().tick(round));
        setOrder(new MemberOrder((id, messages) -> process(id, messages), nextMsgr));
    }

    ConsortiumClientCommunications linkFor(Member m) {
        try {
            return getComm().apply(m, getParams().member);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    KeyPair nextViewConsensusKey() {
        KeyPair current = getNextViewConsensusKeyPair();

        KeyPair keyPair = generateKeyPair(2048, "RSA");
        setNextViewConsensusKeyPair(keyPair);
        byte[] encoded = keyPair.getPublic().getEncoded();
        byte[] signed = sign(getParams().signature.get(), encoded);
        if (signed == null) {
            log.error("Unable to generate and sign consensus key on: {}", getMember());
            transitions.fail();
        }
        setNextView(ViewMember.newBuilder()
                              .setId(getMember().getId().toByteString())
                              .setConsensusKey(ByteString.copyFrom(encoded))
                              .setSignature(ByteString.copyFrom(signed))
                              .build());
        return current;
    }

    void pause() {
        CommonCommunications<ConsortiumClientCommunications, Service> currentComm = getComm();
        if (currentComm != null) {
            ViewContext current = viewContext;
            assert current != null : "No current view, but comm exists!";
            currentComm.deregister(current.getId());
        }
        MemberOrder currentTotalOrder = getOrder();
        if (currentTotalOrder != null) {
            currentTotalOrder.stop();
        }
        Messenger currentMessenger = getMessenger();
        if (currentMessenger != null) {
            currentMessenger.stop();
        }
    }

    void publish(com.google.protobuf.Message message) {
        final Messenger currentMsgr = getMessenger();
        if (currentMsgr == null) {
            log.error("skipping message publish as no messenger");
            return;
        }
//        log.info("publish message: {} on: {}", message.getClass().getSimpleName(), getMember());
        currentMsgr.publish(message);
    }

    void resume() {
        resume(new Service(), getParams().gossipDuration, getParams().scheduler);
    }

    void setComm(CommonCommunications<ConsortiumClientCommunications, Service> comm) {
        this.comm = comm;
    }

    void setCurrent(CurrentBlock current) {
        this.current = current;
    }

    void setMessenger(Messenger messenger) {
        this.messenger = messenger;
    }

    void setNextView(ViewMember nextView) {
        this.nextView = nextView;
    }

    void setNextViewConsensusKeyPair(KeyPair nextViewConsensusKeyPair) {
        this.nextViewConsensusKeyPair = nextViewConsensusKeyPair;
    }

    void setOrder(MemberOrder order) {
        this.order = order;
    }

    void setViewContext(ViewContext viewContext) {
        this.viewContext = viewContext;
    }

    HashKey submit(boolean join, Consumer<HashKey> onCompletion, Message... transactions) throws TimeoutException {
        if (viewContext() == null) {
            throw new IllegalStateException(
                    "The current view is undefined, unable to process transactions on: " + getMember());
        }
        EnqueuedTransaction transaction = build(join, transactions);
        submit(transaction, onCompletion);
        return transaction.getHash();
    }

    void synchronousSendToAll(Consumer<ConsortiumClientCommunications> msg) {
        viewContext().streamRandomRing().forEach(c -> {
            if (!params.member.equals(c)) {
                ConsortiumClientCommunications link = linkFor(c);
                if (link == null) {
                    log.debug("Cannot get link for: {} on: {}", c.getId(), getMember());
                }
                try {
//                    log.debug("Executing synchronous action to: {} on: {}", c.getId(), getMember());
                    msg.accept(link);
                } catch (Throwable t) {
                    log.trace("Error sending synchronous message to: {} on: {}", c, getMember());
                }
            }
        });
    }

    /**
     * Ye Jesus Nut
     *
     * @param list
     */
    void viewChange(ViewContext newView) {
        pause();

        log.info("Installing new view: {} rings: {} ttl: {} on: {} regent: {} member: {} view member: {}",
                 newView.getId(), newView.getRingCount(), newView.timeToLive(), getMember(),
                 getState().currentRegent() >= 0 ? newView.getRegent(getState().currentRegent()) : "None",
                 newView.isMember(), newView.isViewMember());

        setComm(createClientComms.apply(newView.getId()));
        setMessenger(null);
        setOrder(null);
        setViewContext(newView);
        if (newView.isViewMember()) {
            joinMessageGroup(newView);
        }

        resume();
    }

    ViewContext viewContext() {
        return getViewContext();
    }

    private EnqueuedTransaction build(boolean join, Message... transactions) {
        byte[] nonce = new byte[32];
        entropy().nextBytes(nonce);

        Transaction.Builder builder = Transaction.newBuilder()
                                                 .setJoin(join)
                                                 .setSource(getParams().member.getId().toByteString())
                                                 .setNonce(ByteString.copyFrom(nonce));
        for (Message t : transactions) {
            builder.addBatch(Any.pack(t));
        }

        HashKey hash = hashOf(builder);

        byte[] signature = sign(getParams().signature.get(), hash.bytes());
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

    private void clear() {
        pause();
        comm = null;
        order = null;
        current = null;
        messenger = null;
        nextView = null;
        order = null;
    }

    private MemberOrder getOrder() {
        final MemberOrder cTo = order;
        return cTo;
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
        return getCurrent() == next;
    }

    private void process(HashKey contextId, List<Msg> messages) {
        if (!started.get()) {
            return;
        }
        whileStable(contextId, () -> {
            for (Msg msg : messages) {
                if (!started.get()) {
                    return;
                }

                try {
                    process(msg);
                } catch (Throwable t) {
                    log.error("Error processing msg: {} from: {} on: {}", classNameOf(msg.content), msg.from,
                              getMember(), t);
                }
            }
        });
    }

    private void process(Msg msg) {
        if (!started.get()) {
            return;
        }
        assert !msg.from.equals(getMember()) : "Whoopsie";
        Any content = msg.content;

//        log.info("processing msg: {} from: {} on: {} seq: {} ", classNameOf(content), msg.from, getMember(),
//                 msg.sequenceNumber);
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
        log.error("Invalid consortium message type: {} from: {} on: {}", classNameOf(content), msg.from, getMember());

    }

    private void resume(Service service, Duration gossipDuration, ScheduledExecutorService scheduler) {
        CommonCommunications<ConsortiumClientCommunications, Service> currentComm = getComm();
        if (currentComm != null) {
            ViewContext current = viewContext;
            assert current != null : "No current view, but comm exists!";
            currentComm.register(current.getId(), service);
        }
        MemberOrder currentTO = getOrder();
        if (currentTO != null) {
            currentTO.start();
        }
        Messenger currentMsg = getMessenger();
        if (currentMsg != null) {
            currentMsg.start(gossipDuration, scheduler);
        }
    }

    private void submit(EnqueuedTransaction transaction, Consumer<HashKey> onCompletion) throws TimeoutException {
        assert transaction.getHash().equals(hashOf(transaction.getTransaction())) : "Hash does not match!";

        getSubmitted().put(transaction.getHash(), new SubmittedTransaction(transaction.getTransaction(), onCompletion));
        SubmitTransaction submittedTxn = SubmitTransaction.newBuilder()
                                                          .setContext(viewContext().getId().toByteString())
                                                          .setTransaction(transaction.getTransaction())
                                                          .build();
        log.info("Submitting txn: {} from: {}", transaction.getHash(), getMember());
        List<TransactionResult> results;
        results = viewContext().streamRandomRing().map(c -> {
            if (getMember().equals(c)) {
                log.trace("submit: {} to self: {}", transaction.getHash(), c.getId());
                transitions.receive(transaction.getTransaction(), getMember());
                return TransactionResult.getDefaultInstance();
            } else {
                ConsortiumClientCommunications link = linkFor(c);
                if (link == null) {
                    log.debug("Cannot get link for {}", c.getId());
                    return null;
                }
                try {
                    return link.clientSubmit(submittedTxn);
                } catch (Throwable t) {
                    log.trace("Cannot submit txn {} to {}: {}", transaction.getHash(), c, t.getMessage());
                    return null;
                }
            }
        }).filter(r -> r != null).collect(Collectors.toList());

        if (results.size() < viewContext().majority()) {
            log.debug("Cannot submit txn {} on: {} responses: {} required: {}", transaction.getHash(), getMember(),
                      results.size(), viewContext().majority());
            throw new TimeoutException("Cannot submit transaction " + transaction.getHash());
        }
    }

    private void whileStable(HashKey targetView, Runnable action) {
//        Lock lock = viewChange.readLock();
//        lock.lock();
//        try {
        if (viewContext().getId().equals(targetView)) {
            action.run();
        } else {
            log.info("Eliding action from stale view: {} current: {} on: {}", targetView, viewContext().getId(),
                     getMember());
        }
//        } finally {
//            lock.unlock();
//        }
    }
}
