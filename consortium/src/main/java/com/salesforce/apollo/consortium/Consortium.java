/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.CollaboratorContext.height;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterInputStream;

import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStore.Builder;
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
import com.salesfoce.apollo.consortium.proto.CheckpointProcessing;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.Persist;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
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
import com.salesforce.apollo.membership.Context;
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
                log.debug("Client transaction: {} on: {} from: {}", enqueuedTransaction.getHash(), getMember(), from);
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
            transitions.receive(request, member);
        }

        public void stop(Stop stop, HashKey from) {
            Member member = viewContext().getMember(from);
            if (member == null) {
                log.warn("Received Stop from non consortium member: {} on: {}", from, getMember());
                return;
            }
            transitions.deliverStop(stop, member);
        }

        public void stopData(StopData stopData, HashKey from) {
            Member member = viewContext().getMember(from);
            if (member == null) {
                log.warn("Received StopData from non consortium member: {} on: {}", from, getMember());
                return;
            }
            transitions.deliverStopData(stopData, member);
        }

        public void sync(Sync sync, HashKey from) {
            Member member = viewContext().getMember(from);
            if (member == null) {
                log.warn("Received Sync from non consortium member: {} on: {}", from, getMember());
                return;
            }
            ((Runnable) () -> transitions.deliverSync(sync, member)).run();
        }

    }

    public enum Timers {
        AWAIT_GENESIS, AWAIT_GENESIS_VIEW, AWAIT_GROUP, AWAIT_VIEW_MEMBERS, CHECKPOINT_TIMEOUT, CHECKPOINTING,
        FLUSH_BATCH, PROCLAIM, TRANSACTION_TIMEOUT_1, TRANSACTION_TIMEOUT_2;
    }

    static class Result {
        public final Member     member;
        public final JoinResult vote;

        public Result(Member member, JoinResult vote) {
            this.member = member;
            this.vote = vote;
        }
    }

    private static class DelayedMessage {
        public final Member from;
        public final Any    msg;

        private DelayedMessage(Member from, Message message) {
            this.from = from;
            this.msg = Any.pack(message);
        }
    }

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
        buffers.add(transaction.getTxn().toByteString());

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

    private static Builder defaultBuilder(Parameters params) {

        MVStore.Builder builder = new MVStore.Builder();
        if (params.storeFile != null) {
            builder.fileName(params.storeFile.getAbsolutePath());
        }
        return builder;
    }

    final MVStore                                                                                  store;
    private final Map<Long, CheckpointState>                                                       cachedCheckpoints = new ConcurrentHashMap<>();
    private volatile CommonCommunications<ConsortiumClientCommunications, Service>                 comm;
    private final Function<HashKey, CommonCommunications<ConsortiumClientCommunications, Service>> createClientComms;
    private volatile CurrentBlock                                                                  current;
    private final PriorityBlockingQueue<CurrentBlock>                                              deferedBlocks     = new PriorityBlockingQueue<>(
            1024, (a, b) -> Long.compare(height(a.getBlock()), height(b.getBlock())));
    private final List<DelayedMessage>                                                             delayed           = new CopyOnWriteArrayList<>();
    private final Fsm<CollaboratorContext, Transitions>                                            fsm;
    private volatile Block                                                                         genesis;
    private volatile long                                                                          lastCheckpoint;
    private volatile Reconfigure                                                                   lastViewChange;
    private volatile Messenger                                                                     messenger;
    private volatile ViewMember                                                                    nextView;
    private volatile KeyPair                                                                       nextViewConsensusKeyPair;
    private volatile MemberOrder                                                                   order;
    private final Parameters                                                                       params;
    private final TickScheduler                                                                    scheduler         = new TickScheduler();
    private final AtomicBoolean                                                                    started           = new AtomicBoolean();
    private final Map<HashKey, SubmittedTransaction>                                               submitted         = new ConcurrentHashMap<>();
    private final Transitions                                                                      transitions;
    private final AtomicReference<ViewContext>                                                     viewContext       = new AtomicReference<>();

    public Consortium(Parameters parameters) {
        this(parameters, defaultBuilder(parameters));
    }

    public Consortium(Parameters parameters, MVStore.Builder builder) {
        this.params = parameters;
        store = builder.open();
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

    public Fsm<CollaboratorContext, Transitions> fsm() {
        return fsm;
    }

    public Block getGenesis() {
        Block c = genesis;
        return c;
    }

    public long getLastCheckpoint() {
        long c = lastCheckpoint;
        return c;
    }

    public Reconfigure getLastViewChange() {
        Reconfigure c = lastViewChange;
        return c;
    }

    public Logger getLog() {
        return log;
    }

    public Member getMember() {
        return getParams().member;
    }

    // test access
    public CollaboratorContext getState() {
        return fsm.getContext();
    }

    public void process(CertifiedBlock certifiedBlock) {
        transitions.synchronizedProcess(certifiedBlock);
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
        transitions.shutdown();
    }

    public HashKey submit(BiConsumer<Object, Throwable> onCompletion, Message transaction) throws TimeoutException {
        return submit(false, onCompletion, transaction);
    }

    void checkpoint(long height, CheckpointState checkpoint) {
        cachedCheckpoints.put(height, checkpoint);
    }

    void delay(Message message, Member from) {
        delayed.add(new DelayedMessage(from, message));
    }

    SecureRandom entropy() {
        return getParams().msgParameters.entropy;
    }

    InputStream getBody(Block block) {
        return new InflaterInputStream(
                BbBackedInputStream.aggregate(block.getBody().getContents().asReadOnlyByteBufferList()));
    }

    CheckpointState getChekpoint(long checkpoint) {
        return cachedCheckpoints.get(checkpoint);
    }

    CommonCommunications<ConsortiumClientCommunications, Service> getComm() {
        final CommonCommunications<ConsortiumClientCommunications, Service> cc = comm;
        return cc;
    }

    CurrentBlock getCurrent() {
        final CurrentBlock cb = current;
        return cb;
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

    Map<HashKey, SubmittedTransaction> getSubmitted() {
        return submitted;
    }

    // test accessible
    Transitions getTransitions() {
        return transitions;
    }

    ViewContext getViewContext() {
        return viewContext.get();
    }

    void joinMessageGroup(ViewContext newView) {
        log.debug("Joining message group: {} on: {}", newView.getId(), getMember());
        Messenger nextMsgr = newView.createMessenger(getParams());
        setMessenger(nextMsgr);
        nextMsgr.register(round -> getScheduler().tick());
        setOrder(new MemberOrder((id, messages) -> process(id, messages), nextMsgr));
    }

    ConsortiumClientCommunications linkFor(Member m) {
        try {
            return getComm().apply(m, getParams().member);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).toString());
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
            ViewContext current = viewContext.get();
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

    void publish(Message message) {
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

    void setGenesis(Block genesis) {
        this.genesis = genesis;
    }

    void setLastCheckpoint(long lastCheckpoint) {
        this.lastCheckpoint = lastCheckpoint;
    }

    void setLastViewChange(Reconfigure lastViewChange) {
        this.lastViewChange = lastViewChange;
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
        this.viewContext.set(viewContext);
    }

    HashKey submit(boolean join, BiConsumer<Object, Throwable> onCompletion, Message txn) throws TimeoutException {
        if (viewContext() == null) {
            throw new IllegalStateException(
                    "The current view is undefined, unable to process transactions on: " + getMember());
        }
        EnqueuedTransaction transaction = build(join, txn);
        submit(transaction, onCompletion);
        return transaction.getHash();
    }

    void synchronizedProcess(CertifiedBlock certifiedBlock) {
        if (!started.get()) {
            return;
        }
        Block block = certifiedBlock.getBlock();
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        log.debug("Processing block {} : {} height: {} on: {}", hash, block.getBody().getType(),
                  block.getHeader().getHeight(), getMember());
        final CurrentBlock previousBlock = getCurrent();
        if (previousBlock != null) {
            HashKey prev = new HashKey(block.getHeader().getPrevious().toByteArray());
            long height = height(block);
            long prevHeight = height(previousBlock.getBlock());
            if (height <= prevHeight) {
                log.debug("Discarding previously committed block: {} height: {} current height: {} on: {}", hash,
                          height, prevHeight, getMember());
                return;
            }
            if (height != prevHeight + 1) {
                deferedBlocks.add(new CurrentBlock(hash, block));
                log.debug("Deferring block on {}.  Block: {} height should be {} and next block height is {}",
                          getMember(), hash, height(previousBlock.getBlock()) + 1, block.getHeader().getHeight());
                return;
            }
            if (!previousBlock.getHash().equals(prev)) {
                log.error("Protocol violation on {}. New block does not refer to current block hash. Should be {} and next block's prev is {}, current height: {} next height: {}",
                          getMember(), previousBlock.getHash(), prev, prevHeight, height);
                return;
            }
            if (!viewContext().validate(certifiedBlock)) {
                log.error("Protocol violation on {}. New block is not validated {}", getMember(), hash);
                return;
            }
        } else {
            if (block.getBody().getType() != BodyType.GENESIS) {
                log.error("Invalid genesis block on: {} block: {}", getMember(), block.getBody().getType());
                return;
            }
            Genesis body;
            try {
                body = Genesis.parseFrom(getBody(block));
            } catch (IOException e) {
                log.error("Protocol violation ont: {}. Genesis block body cannot be deserialized {}", getMember(),
                          hash);
                return;
            }
            Context<Member> context = getParams().context;
            if (!validateGenesis(hash, certifiedBlock, body.getInitialView(), context,
                                 context.getRingCount() - context.toleranceLevel(), getMember())) {
                log.error("Protocol violation on: {}. Genesis block is not validated {}", getMember(), hash);
                return;
            }
        }
        if (next(new CurrentBlock(hash, block))) {
            processDeferred();
        }
    }

    long targetCheckpoint() {
        Reconfigure currentView = getLastViewChange();
        if (currentView == null) {
            return Long.MAX_VALUE;
        }
        return getLastCheckpoint() + currentView.getCheckpointBlocks();
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

    private EnqueuedTransaction build(boolean join, Message transaction) {
        byte[] nonce = new byte[32];
        entropy().nextBytes(nonce);

        Transaction.Builder builder = Transaction.newBuilder()
                                                 .setJoin(join)
                                                 .setSource(getParams().member.getId().toByteString())
                                                 .setNonce(ByteString.copyFrom(nonce));
        builder.setTxn(Any.pack(transaction));

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
        for (Msg msg : messages) {
            if (!started.get()) {
                return;
            }

            try {
                process(msg);
            } catch (Throwable t) {
                log.error("Error processing msg: {} from: {} on: {}", classNameOf(msg.content), msg.from, getMember(),
                          t);
            }
        }
    }

    private void process(Msg msg) {
        if (!started.get()) {
            return;
        }
        assert !msg.from.equals(getMember()) : "Whoopsie";
        Any content = msg.content;

        processDelayed();
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
                transitions.receive(content.unpack(Transaction.class), msg.from);
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
        if (content.is(CheckpointProcessing.class)) {
            try {
                transitions.deliverCheckpointing(content.unpack(CheckpointProcessing.class), msg.from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid checkpointing delivered from: {} on: {}", msg.from, getMember(), e);
            }
            return;
        }
        if (!processSynchronized(msg.from, content)) {
            log.error("Invalid consortium message type: {} from: {} on: {}", classNameOf(content), msg.from,
                      getMember());
        }
    }

    private void processDeferred() {
        CurrentBlock delayed = deferedBlocks.poll();
        while (delayed != null) {
            long height = height(delayed.getBlock());
            long currentHeight = height(getCurrent().getBlock());
            if (height <= currentHeight) {
                log.debug("dropping deferred block: {} height: {} <= current height: {} on: {}", delayed.getHash(),
                          height, currentHeight, getMember());
                delayed = deferedBlocks.poll();
            } else if (height == currentHeight + 1) {
                log.debug("processing deferred block: {} height: {} on: {}", delayed.getHash(), height, getMember());
                next(delayed);
                delayed = deferedBlocks.poll();
            } else {
                log.debug("current height: {} so re-deferring block: {} height: {} on: {}", currentHeight,
                          delayed.getHash(), height, getMember());
                deferedBlocks.add(delayed);
                delayed = null;
            }
        }
    }

    private void processDelayed() {
        if (!delayed.isEmpty()) {
            log.debug("Processing delayed msgs: {} on: {}", delayed.size(), getMember());
            List<DelayedMessage> toConsider = new ArrayList<>(delayed);
            delayed.clear();
            for (DelayedMessage dm : toConsider) {
                log.trace("Applying delayed: {} on: {}", classNameOf(dm.msg), getMember());
                if (!processSynchronized(dm.from, dm.msg)) {
                    log.error("Protocol error on: {} processing delayed, not sync message: {}", getMember(),
                              classNameOf(dm.msg));
                }
            }
            if (!delayed.isEmpty()) {
                log.debug("Delayed msgs remain: {} on: {}", delayed.size(), getMember());
            }
        }
    }

    private boolean processSynchronized(Member from, Any content) {
        if (content.is(Stop.class)) {
            try {
                transitions.deliverStop(content.unpack(Stop.class), from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid stop delivered from: {} on: {}", from, getMember(), e);
            }
            return true;
        }
        if (content.is(Sync.class)) {
            try {
                transitions.deliverSync(content.unpack(Sync.class), from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid sync delivered from: {} on: {}", from, getMember(), e);
            }
            return true;
        }
        if (content.is(StopData.class)) {
            try {
                transitions.deliverStopData(content.unpack(StopData.class), from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid sync delivered from: {} on: {}", from, getMember(), e);
            }
            return true;
        }
        if (content.is(ReplicateTransactions.class)) {
            try {
                transitions.receive(content.unpack(ReplicateTransactions.class), from);
            } catch (InvalidProtocolBufferException e) {
                log.error("invalid replication of transactions delivered from: {} on: {}", from, getMember(), e);
            }
            return true;
        }
        return false;
    }

    private void resume(Service service, Duration gossipDuration, ScheduledExecutorService scheduler) {
        CommonCommunications<ConsortiumClientCommunications, Service> currentComm = getComm();
        if (currentComm != null) {
            ViewContext current = viewContext.get();
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

    private void submit(EnqueuedTransaction transaction,
                        BiConsumer<Object, Throwable> onCompletion) throws TimeoutException {
        assert transaction.getHash().equals(hashOf(transaction.getTransaction())) : "Hash does not match!";

        getSubmitted().put(transaction.getHash(), new SubmittedTransaction(transaction.getTransaction(), onCompletion));
        SubmitTransaction submittedTxn = SubmitTransaction.newBuilder()
                                                          .setContext(viewContext().getId().toByteString())
                                                          .setTransaction(transaction.getTransaction())
                                                          .build();
        log.debug("Submitting txn: {} from: {}", transaction.getHash(), getMember());
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
}
