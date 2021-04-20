/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.CollaboratorContext.height;
import static com.salesforce.apollo.consortium.support.SigningUtils.sign;
import static com.salesforce.apollo.consortium.support.SigningUtils.validateGenesis;
import static com.salesforce.apollo.consortium.support.SigningUtils.verify;

import java.io.IOException;
import java.io.InputStream;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterInputStream;

import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStore.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.BlockReplication;
import com.salesfoce.apollo.consortium.proto.Blocks;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.BootstrapSync;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.CheckpointProcessing;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesfoce.apollo.consortium.proto.CheckpointSync;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
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
import com.salesforce.apollo.consortium.comms.ConsortiumClientCommunications;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
import com.salesforce.apollo.consortium.fsm.Transitions;
import com.salesforce.apollo.consortium.support.CheckpointState;
import com.salesforce.apollo.consortium.support.EnqueuedTransaction;
import com.salesforce.apollo.consortium.support.HashedCertifiedBlock;
import com.salesforce.apollo.consortium.support.SigningUtils;
import com.salesforce.apollo.consortium.support.SubmittedTransaction;
import com.salesforce.apollo.consortium.support.TickScheduler;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger.MessageHandler.Msg;
import com.salesforce.apollo.protocols.BbBackedInputStream;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class Consortium {

    public class Service {

        public BootstrapSync checkpointSync(CheckpointSync request, HashKey from) {
            Member member = getParams().context.getMember(from);
            if (member == null) {
                log.warn("Received checkpoint sync from non member: {} on: {}", from, getMember());
                return BootstrapSync.getDefaultInstance();
            }

            return BootstrapSync.getDefaultInstance();
        }

        public TransactionResult clientSubmit(SubmitTransaction request, HashKey from) {
            Member member = getParams().context.getMember(from);
            if (member == null) {
                log.warn("Received client transaction submission from non member: {} on: {}", from, getMember());
                return TransactionResult.getDefaultInstance();
            }
            EnqueuedTransaction enqueuedTransaction = new EnqueuedTransaction(hashOf(request.getTransaction()),
                    request.getTransaction());
            if (enqueuedTransaction.transaction.getJoin()) {
                if (view.getContext().getMember(from) == null) {
                    log.warn("Received join from non consortium member: {} on: {}", from, getMember());
                    return TransactionResult.getDefaultInstance();
                }
                log.trace("Join transaction: {} on: {} from consortium member : {}", enqueuedTransaction.hash,
                          getMember(), from);
            } else {
                log.trace("Client transaction: {} on: {} from: {}", enqueuedTransaction.hash, getMember(), from);
            }
            transitions.receive(enqueuedTransaction.transaction, member);
            return TransactionResult.getDefaultInstance();
        }

        public CheckpointSegments fetch(CheckpointReplication request, HashKey from) {
            Member member = getParams().context.getMember(from);
            if (member == null) {
                log.warn("Received checkpoint fetch from non member: {} on: {}", from, getMember());
                return CheckpointSegments.getDefaultInstance();
            }
            return Consortium.this.fetch(request);
        }

        public Blocks fetchBlocks(BlockReplication request, HashKey from) {
            // TODO Auto-generated method stub
            return null;
        }

        public JoinResult join(Join request, HashKey fromID) {
            Member from = view.getContext().getActiveMember(fromID);
            if (from == null) {
                log.debug("Member not part of current view: {} on: {}", fromID, getMember());
                return JoinResult.getDefaultInstance();
            }
            try {
                return fsm.synchonizeOnState(() -> {
                    if (view.getNextView() == null) {
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
                                     .setNextView(view.getNextView())
                                     .build();
                });
            } catch (Exception e) {
                log.error("Error voting for: {} on: {}", from, getMember(), e);
                return JoinResult.getDefaultInstance();
            }
        }

        public void stopData(StopData stopData, HashKey from) {
            Member member = view.getContext().getMember(from);
            if (member == null) {
                log.warn("Received StopData from non consortium member: {} on: {}", from, getMember());
                return;
            }
            transitions.deliverStopData(stopData, member);
        }

    }

    public enum Timers {
        ASSEMBLE_CHECKPOINT, AWAIT_GENESIS, AWAIT_GENESIS_VIEW, AWAIT_GROUP, AWAIT_INITIAL_VIEW, AWAIT_VIEW_MEMBERS,
        CHECKPOINT_TIMEOUT, CHECKPOINTING, FLUSH_BATCH, PROCLAIM, TRANSACTION_TIMEOUT_1, TRANSACTION_TIMEOUT_2;
    }

    public static class TransactionSubmitFailure extends Exception {
        private static final long serialVersionUID = 1L;

        public final HashKey key;
        public final int     suceeded;

        public TransactionSubmitFailure(String message, HashKey key, int succeeded) {
            super(message);
            this.suceeded = succeeded;
            this.key = key;
        }
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

    private static class PendingAction {
        private final Runnable action;
        private final long     targetBlock;

        public PendingAction(long targetBlock, Runnable action) {
            this.targetBlock = targetBlock;
            this.action = action;
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

    final Store                                               store;
    private final Map<Long, CheckpointState>                  cachedCheckpoints     = new ConcurrentHashMap<>();
    private final AtomicReference<HashedCertifiedBlock>       current               = new AtomicReference<>();
    private final PriorityBlockingQueue<HashedCertifiedBlock> deferedBlocks         = new PriorityBlockingQueue<>(1024,
            (a, b) -> Long.compare(height(a.block), height(b.block)));
    private final List<DelayedMessage>                        delayed               = new CopyOnWriteArrayList<>();
    private final AtomicInteger                               deltaCheckpointBlocks = new AtomicInteger(
            Integer.MAX_VALUE);
    private final Fsm<CollaboratorContext, Transitions>       fsm;
    private final AtomicReference<HashedCertifiedBlock>       genesis               = new AtomicReference<>();
    private final AtomicReference<HashedCertifiedBlock>       lastCheckpoint        = new AtomicReference<>();
    private final AtomicReference<HashedCertifiedBlock>       lastViewChange        = new AtomicReference<>();
    private final Parameters                                  params;
    private final AtomicReference<PendingAction>              pending               = new AtomicReference<>();
    private final TickScheduler                               scheduler             = new TickScheduler();
    private final Lock                                        sequencer             = new ReentrantLock();
    private final AtomicBoolean                               started               = new AtomicBoolean();
    private final Map<HashKey, SubmittedTransaction>          submitted             = new ConcurrentHashMap<>();
    private final Transitions                                 transitions;
    private final View                                        view;
    final Service                                             service               = new Service();

    public Consortium(Parameters parameters) {
        this(parameters, defaultBuilder(parameters).open());
    }

    public Consortium(Parameters parameters, MVStore store) {
        this.params = parameters;
        this.store = new Store(store);
        view = new View(new Service(), parameters, (id, messages) -> process(id, messages));
        fsm = Fsm.construct(new CollaboratorContext(this), Transitions.class, CollaboratorFsm.INITIAL, true);
        fsm.setName(getMember().getId().b64Encoded());
        transitions = fsm.getTransitions();
        view.nextViewConsensusKey();
    }

    public Fsm<CollaboratorContext, Transitions> fsm() {
        return fsm;
    }

    public HashedCertifiedBlock getGenesis() {
        return genesis.get();
    }

    public long getLastCheckpoint() {
        HashedCertifiedBlock c = lastCheckpoint.get();
        return c == null ? 0 : lastCheckpoint.get().height();
    }

    public HashedCertifiedBlock getLastCheckpointBlock() {
        return lastCheckpoint.get();
    }

    public long getLastViewChange() {
        final HashedCertifiedBlock c = lastViewChange.get();
        return c == null ? 0 : c.height();
    }

    public HashedCertifiedBlock getLastViewChangeBlock() {
        final HashedCertifiedBlock c = lastViewChange.get();
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
        if (!started.get()) {
            return;
        }
        try {
            sequencer.lock();
            synchronizedProcess(certifiedBlock);
        } finally {
            sequencer.unlock();
        }
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        log.info("Starting consortium on {}", getMember());
        transitions.start();
        view.resume(service);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        clear();
        log.info("Stopping consortium on {}", getMember());
        cachedCheckpoints.values().forEach(cp -> cp.close());
        fsm.getContext().clear();
        transitions.shutdown();
    }

    public HashKey submit(BiConsumer<Boolean, Throwable> onSubmit, BiConsumer<Object, Throwable> onCompletion,
                          Message transaction) throws TimeoutException {
        return submit(onSubmit, false, onCompletion, transaction);
    }

    void checkpoint(long height, CheckpointState checkpoint) {
        cachedCheckpoints.put(height, checkpoint);
    }

    void delay(Message message, Member from) {
        delayed.add(new DelayedMessage(from, message));
    }

    InputStream getBody(Block block) {
        return new InflaterInputStream(
                BbBackedInputStream.aggregate(block.getBody().getContents().asReadOnlyByteBufferList()));
    }

    CheckpointState getChekpoint(long checkpoint) {
        return cachedCheckpoints.get(checkpoint);
    }

    HashedCertifiedBlock getCurrent() {
        return current.get();
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

    Transitions getTransitions() {
        return transitions;
    }

    void joinMessageGroup(ViewContext newView) {
        view.joinMessageGroup(newView, scheduler, process());
    }

    ConsortiumClientCommunications linkFor(Member m) {
        try {
            return view.getComm().apply(m, getParams().member);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).toString());
        }
        return null;
    }

    void performAfter(Runnable action, long blockHeight) {
        HashedCertifiedBlock cb = getCurrent();
        if (cb == null) {
            return;
        }
        long current = cb.height();
        if (blockHeight < current) {
            log.info("Pending action scheduled in the past: {} current: {} on: ", blockHeight, current, getMember());
            return;
        }
        PendingAction pendingAction = new PendingAction(blockHeight, action);
        if (!pending.compareAndSet(null, pendingAction)) {
            throw new IllegalStateException("Previous pending action uncleared on: " + getMember());
        }
        log.info("Pending action scheduled at: {} on: {}", blockHeight, getMember());
        if (current == blockHeight) {
            params.dispatcher.execute(() -> {
                sequencer.lock();
                try {
                    runPending(pendingAction);
                } finally {
                    sequencer.unlock();
                }
            });
        }
    }

    void setCurrent(HashedCertifiedBlock current) {
        this.current.set(current);
    }

    void setGenesis(HashedCertifiedBlock next) {
        if (!this.genesis.compareAndSet(null, next)) {
            throw new IllegalStateException("Genesis already set on: " + getMember());
        }
    }

    void setLastCheckpoint(HashedCertifiedBlock next) {
        this.lastCheckpoint.set(next);
    }

    void setLastViewChange(HashedCertifiedBlock block, Reconfigure view) {
        lastViewChange.set(block);
        deltaCheckpointBlocks.set(view.getCheckpointBlocks());
        log.info("Checkpoint in: {} blocks on: {}", view.getCheckpointBlocks(), getMember());
    }

    View getView() {
        return view;
    }

    HashKey submit(BiConsumer<Boolean, Throwable> onSubmit, boolean join, BiConsumer<Object, Throwable> onCompletion,
                   Message txn) throws TimeoutException {
        if (view.getContext() == null) {
            throw new IllegalStateException(
                    "The current view is undefined, unable to process transactions on: " + getMember());
        }
        EnqueuedTransaction transaction = build(join, txn);
        submit(transaction, onSubmit, onCompletion);
        return transaction.hash;
    }

    long targetCheckpoint() {
        final long delta = deltaCheckpointBlocks.get();
        final HashedCertifiedBlock cp = lastCheckpoint.get();
        return cp == null ? delta : cp.height() + delta;
    }

    private EnqueuedTransaction build(boolean join, Message transaction) {
        byte[] nonce = new byte[32];
        Utils.secureEntropy().nextBytes(nonce);

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
        view.clear();
        pending.set(null);
        current.set(null);
    }

    private CheckpointSegments fetch(CheckpointReplication request) {
        CheckpointState state = cachedCheckpoints.get(request.getCheckpoint());
        if (state == null) {
            log.debug("No cached checkpoint for {} on: {}", request.getCheckpoint(), getMember());
            return CheckpointSegments.getDefaultInstance();
        }
        CheckpointSegments.Builder replication = CheckpointSegments.newBuilder();

        store.fetchBlocks(BloomFilter.from(request.getCheckpointSegments()), replication, params.maxCheckpointBlocks,
                          state.checkpoint.getCheckpoint());

        return replication.addAllSegments(state.fetchSegments(BloomFilter.from(request.getCheckpointSegments()),
                                                              params.maxCheckpointSegments, Utils.bitStreamEntropy()))
                          .build();
    }

    private boolean next(HashedCertifiedBlock next) {
        switch (next.block.getBlock().getBody().getType()) {
        case CHECKPOINT:
            getState().processCheckpoint(next);
            break;
        case GENESIS:
            getState().processGenesis(next);
            break;
        case RECONFIGURE:
            getState().processReconfigure(next);
            break;
        case USER:
            getState().processUser(next);
            break;
        case UNRECOGNIZED:
        default:
            log.error("Unrecognized block type: {} : {}", next.hashCode(), next.block);
        }
        return getCurrent() == next;
    }

    private BiConsumer<HashKey, List<Msg>> process() {
        return (id, messages) -> process(id, messages);
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
        HashedCertifiedBlock delayed = deferedBlocks.poll();
        while (delayed != null) {
            long height = delayed.height();
            long currentHeight = getCurrent().height();
            if (height <= currentHeight) {
                log.debug("dropping deferred block: {} height: {} <= current height: {} on: {}", delayed.hash, height,
                          currentHeight, getMember());
                delayed = deferedBlocks.poll();
            } else if (height == currentHeight + 1) {
                log.debug("processing deferred block: {} height: {} on: {}", delayed.hash, height, getMember());
                next(delayed);
                delayed = deferedBlocks.poll();
            } else {
                log.debug("current height: {} so re-deferring block: {} height: {} on: {}", currentHeight, delayed.hash,
                          height, getMember());
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

    private void processSubmit(EnqueuedTransaction transaction, BiConsumer<Boolean, Throwable> onSubmit,
                               AtomicInteger pending, AtomicBoolean completed, int succeeded) {
        int remaining = pending.decrementAndGet();
        if (completed.get()) {
            return;
        }
        int majority = view.getContext().majority();
        if (succeeded >= majority) {
            if (onSubmit != null && completed.compareAndSet(false, true)) {
                onSubmit.accept(true, null);
            }
        } else {
            if (remaining + succeeded < majority) {
                if (onSubmit != null && completed.compareAndSet(false, true)) {
                    onSubmit.accept(null, new TransactionSubmitFailure("Failed to achieve majority", transaction.hash,
                            succeeded));
                }
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

    private void runPending(PendingAction action) {
        log.info("Running action scheduled at: {} on: {}", action.targetBlock, getMember());
        pending.set(null);
        action.action.run();
    }

    private void submit(EnqueuedTransaction transaction, BiConsumer<Boolean, Throwable> onSubmit,
                        BiConsumer<Object, Throwable> onCompletion) throws TimeoutException {
        assert transaction.hash.equals(hashOf(transaction.transaction)) : "Hash does not match!";

        getSubmitted().put(transaction.hash, new SubmittedTransaction(transaction.transaction, onCompletion));
        SubmitTransaction submittedTxn = SubmitTransaction.newBuilder()
                                                          .setContext(view.getContext().getId().toByteString())
                                                          .setTransaction(transaction.transaction)
                                                          .build();
        log.trace("Submitting txn: {} from: {}", transaction.hash, getMember());
        List<Member> group = view.getContext().streamRandomRing().collect(Collectors.toList());
        AtomicInteger pending = new AtomicInteger(group.size());
        AtomicInteger success = new AtomicInteger();
        AtomicBoolean completed = new AtomicBoolean();
        group.forEach(c -> {
            if (getMember().equals(c)) {
                log.trace("submit: {} to self: {}", transaction.hash, c.getId());
                transitions.receive(transaction.transaction, getMember());
                pending.decrementAndGet();
                processSubmit(transaction, onSubmit, pending, completed, success.incrementAndGet());
            } else {
                ConsortiumClientCommunications link = linkFor(c);
                if (link == null) {
                    log.debug("Cannot get link for {}", c.getId());
                    pending.decrementAndGet();
                    return;
                }
                ListenableFuture<TransactionResult> futureSailor = link.clientSubmit(submittedTxn);
                futureSailor.addListener(() -> {
                    if (completed.get()) {
                        return;
                    }
                    int succeeded;
                    try {
                        futureSailor.get();
                        succeeded = success.incrementAndGet();
                    } catch (InterruptedException e) {
                        log.trace("error submitting txn: {} to {} on: {}", transaction.hash, c, getMember(), e);
                        succeeded = success.get();
                    } catch (ExecutionException e) {
                        succeeded = success.get();
                        log.trace("error submitting txn: {} to {} on: {}", transaction.hash, c, getMember(),
                                  e.getCause());
                    }
                    processSubmit(transaction, onSubmit, pending, completed, succeeded);
                }, ForkJoinPool.commonPool()); // TODO, put in passed executor
            }
        });
    }

    private void synchronizedProcess(CertifiedBlock certifiedBlock) {
        Block block = certifiedBlock.getBlock();
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        log.debug("Processing block {} : {} height: {} on: {}", hash, block.getBody().getType(),
                  block.getHeader().getHeight(), getMember());
        final HashedCertifiedBlock previousBlock = getCurrent();
        long height = height(block);
        if (previousBlock != null) {
            HashKey prev = new HashKey(block.getHeader().getPrevious().toByteArray());
            long prevHeight = previousBlock.height();
            if (height <= prevHeight) {
                log.debug("Discarding previously committed block: {} height: {} current height: {} on: {}", hash,
                          height, prevHeight, getMember());
                return;
            }
            if (height != prevHeight + 1) {
                deferedBlocks.add(new HashedCertifiedBlock(hash, certifiedBlock));
                log.debug("Deferring block on {}.  Block: {} height should be {} and block height is {}", getMember(),
                          hash, previousBlock.height() + 1, block.getHeader().getHeight());
                return;
            }
            if (!previousBlock.hash.equals(prev)) {
                log.error("Protocol violation on {}. New block does not refer to current block hash. Should be {} and next block's prev is {}, current height: {} next height: {}",
                          getMember(), previousBlock.hash, prev, prevHeight, height);
                return;
            }
            if (!view.getContext().validate(certifiedBlock)) {
                log.error("Protocol violation on {}. New block is not validated {}", getMember(), hash);
                return;
            }
        } else {
            if (block.getBody().getType() != BodyType.GENESIS) {
                deferedBlocks.add(new HashedCertifiedBlock(hash, certifiedBlock));
                log.debug("Deferring block on {}.  Block: {} height should be {} and block height is {}", getMember(),
                          hash, 0, block.getHeader().getHeight());
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
        if (next(new HashedCertifiedBlock(hash, certifiedBlock))) {
            PendingAction pendingAction = pending.get();
            if (pendingAction != null && pendingAction.targetBlock == height) {
                runPending(pendingAction);
            }
            processDeferred();
        }
    }
}
