/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Body;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.Certification;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock.Builder;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.CheckpointProcessing;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Header;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.JoinTransaction;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.consortium.proto.User;
import com.salesfoce.apollo.consortium.proto.Validate;
import com.salesfoce.apollo.consortium.proto.ViewMember;
import com.salesforce.apollo.consortium.Consortium.Result;
import com.salesforce.apollo.consortium.Consortium.Timers;
import com.salesforce.apollo.consortium.comms.ConsortiumClient;
import com.salesforce.apollo.consortium.fsm.Transitions;
import com.salesforce.apollo.consortium.support.CheckpointState;
import com.salesforce.apollo.consortium.support.EnqueuedTransaction;
import com.salesforce.apollo.consortium.support.HashedBlock;
import com.salesforce.apollo.consortium.support.HashedCertifiedBlock;
import com.salesforce.apollo.consortium.support.ProcessedBuffer;
import com.salesforce.apollo.consortium.support.SubmittedTransaction;
import com.salesforce.apollo.consortium.support.TickScheduler.Timer;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

/**
 * Context for the state machine. These are the leaf actions driven by the FSM.
 *
 */
public class CollaboratorContext {

    private static final Logger log = LoggerFactory.getLogger(CollaboratorContext.class);

    public static Body body(BodyType type, Message contents) {
        return Body.newBuilder().setType(type).setContents(Consortium.compress(contents.toByteString())).build();
    }

    public static Checkpoint checkpoint(long currentHeight, File state, int blockSize) {
        HashKey stateHash = HashKey.ORIGIN;
        long length = 0;
        if (state != null) {
            try (FileInputStream fis = new FileInputStream(state)) {
                stateHash = new HashKey(Conversion.hashOf(fis));
            } catch (IOException e) {
                log.error("Invalid checkpoint!", e);
                return null;
            }
            length = state.length();
        }
        Checkpoint.Builder builder = Checkpoint.newBuilder()
                                               .setCheckpoint(currentHeight)
                                               .setByteSize(length)
                                               .setSegmentSize(blockSize)
                                               .setStateHash(stateHash.toByteString());
        if (state != null) {
            byte[] buff = new byte[blockSize];
            try (FileInputStream fis = new FileInputStream(state)) {
                for (int read = fis.read(buff); read > 0; read = fis.read(buff)) {
                    builder.addSegments(ByteString.copyFrom(Conversion.hashOf(buff, read)));
                }
            } catch (IOException e) {
                log.error("Invalid checkpoint!", e);
                return null;
            }
        }
        return builder.build();
    }

    public static Checkpoint checkpointBody(Block block) {
        Checkpoint body;
        try {
            body = Checkpoint.parseFrom(Consortium.getBody(block));
        } catch (IOException e) {
            log.debug("Protocol violation.  Cannot decode checkpoint body: {}", e);
            return null;
        }
        return body;
    }

    public static <T> Stream<List<T>> chunked(Stream<T> stream, int chunkSize) {
        AtomicInteger index = new AtomicInteger(0);

        return stream.collect(Collectors.groupingBy(x -> index.getAndIncrement() / chunkSize))
                     .entrySet()
                     .stream()
                     .sorted(Map.Entry.comparingByKey())
                     .map(Map.Entry::getValue);
    }

    public static Block generateBlock(HashedCertifiedBlock checkpoint, final long height, byte[] previous, Body body,
                                      HashedCertifiedBlock viewChange) {
        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();
        byte[] nonce = new byte[32];
        Utils.secureEntropy().nextBytes(nonce);
        Block block = Block.newBuilder()
                           .setHeader(Header.newBuilder()
                                            .setTimestamp(timestamp)
                                            .setNonce(ByteString.copyFrom(nonce))
                                            .setLastCheckpoint(checkpoint == null ? 0 : checkpoint.height())
                                            .setLastReconfig(viewChange == null ? 0 : viewChange.height())
                                            .setPrevious(ByteString.copyFrom(previous))
                                            .setHeight(height)
                                            .setBodyHash(ByteString.copyFrom(Conversion.hashOf(body.toByteString())))
                                            .build())
                           .setBody(body)
                           .build();
        return block;
    }

    public static Genesis genesisBody(Block block) {
        Genesis body;
        try {
            body = Genesis.parseFrom(Consortium.getBody(block));
        } catch (IOException e) {
            log.debug("Protocol violation.  Cannot decode genesis body: {}", e);
            return null;
        }
        return body;
    }

    public static long height(Block block) {
        return block.getHeader().getHeight();
    }

    public static long height(CertifiedBlock cb) {
        return height(cb.getBlock());
    }

    public static List<Long> noGaps(Collection<CertifiedBlock> blocks, Map<Long, ?> cache) {
        Map<HashKey, CertifiedBlock> hashed = blocks.stream()
                                                    .collect(Collectors.toMap(cb -> new HashKey(
                                                            Conversion.hashOf(cb.getBlock().toByteString())),
                                                                              cb -> cb));

        return noGaps(hashed, cache);
    }

    public static List<Long> noGaps(Map<HashKey, CertifiedBlock> hashed, Map<Long, ?> cache) {
        List<Long> ordered = hashed.values()
                                   .stream()
                                   .map(cb -> height(cb.getBlock()))
                                   .sorted()
                                   .collect(Collectors.toList());
        return LongStream.range(0, ordered.size() - 1)
                         .flatMap(idx -> LongStream.range(ordered.get((int) idx) + 1, ordered.get((int) (idx + 1))))
                         .filter(h -> !cache.containsKey(h))
                         .boxed()
                         .collect(Collectors.toList());
    }

    public static Reconfigure reconfigureBody(Block block) {
        Reconfigure body;
        try {
            body = Reconfigure.parseFrom(Consortium.getBody(block));
        } catch (IOException e) {
            log.debug("Protocol violation.  Cannot decode reconfiguration body", e);
            return null;
        }
        return body;
    }

    final Consortium                           consortium;
    private final AtomicLong                   currentConsensus = new AtomicLong(-1);
    private final AtomicReference<HashedBlock> lastBlock        = new AtomicReference<>();

    private final ProcessedBuffer processed;

    private final Regency regency = new Regency();

    private final Map<Timers, Timer> timers = new ConcurrentHashMap<>();

    private final Map<HashKey, EnqueuedTransaction> toOrder = new ConcurrentHashMap<>();

    private final View view;

    private final Map<HashKey, CertifiedBlock.Builder> workingBlocks = new ConcurrentHashMap<>();

    CollaboratorContext(Consortium consortium) {
        this.consortium = consortium;
        Parameters params = consortium.getParams();
        processed = new ProcessedBuffer(params.processedBufferSize);
        view = consortium.getView();
    }

    public void aquireInitialView() {
        schedule(Timers.AWAIT_INITIAL_VIEW, () -> {
            consortium.getTransitions().missingInitialView();
        }, consortium.getParams().initialViewTimeout);
    }

    public void awaitGenesis() {
        schedule(Timers.AWAIT_GENESIS, () -> {
            consortium.getTransitions().missingGenesis();
        }, consortium.getParams().viewTimeout);
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
        regency.nextRegent(regency.currentRegent() + 1);
        log.info("Starting change of regent from: {} to: {} on: {}", regency.currentRegent(), regency.nextRegent(),
                 consortium.getMember());
        toOrder.values().forEach(t -> t.cancel());
        Stop.Builder data = Stop.newBuilder()
                                .setContext(view.getContext().getId().toByteString())
                                .setNextRegent(regency.nextRegent());
        transactions.forEach(eqt -> data.addTransactions(eqt.transaction));
        Stop stop = data.build();
        view.publish(stop);
        consortium.getTransitions().deliverStop(stop, consortium.getMember());
    }

    public void delay(Message message, Member from) {
        consortium.delay(message, from);
    }

    public void deliverBlock(Block block, Member from) {
        Member regent = view.getContext().getRegent(regency.currentRegent());
        if (!regent.equals(from)) {
            log.debug("Ignoring block from non regent: {} actual: {} on: {}", from, regent, getMember());
            return;
        }
        if (block.getBody().getType() == BodyType.GENESIS) {
            deliverGenesisBlock(block, from);
            return;
        }
        if (block.getBody().getType() == BodyType.CHECKPOINT) {
            deliverCheckpointBlock(block, from);
            return;
        }
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        Builder cb = workingBlocks.get(hash);
        if (cb == null) {
            log.debug("Delivering block: {} from: {} on: {}", hash, from, getMember());
            workingBlocks.put(hash, CertifiedBlock.newBuilder().setBlock(block));
            processToOrder(block);
            consortium.getParams().dispatcher.execute(() -> {
                Validate validation = view.getContext().generateValidation(hash, block);
                if (validation == null) {
                    log.debug("Rejecting block proposal: {}, cannot validate from: {} on: {}", hash, from,
                              consortium.getMember());
                    workingBlocks.remove(hash);
                    return;
                }
                view.publish(validation);
                deliverValidate(validation);
            });
        }
    }

    public void deliverCheckpointing(CheckpointProcessing checkpointProcessing, Member from) {
        Member regent = getRegent(regency.currentRegent());
        if (!regent.equals(from)) {
            log.trace("checkpoint processing: {} ignored from: {} current regent: {} on: {}",
                      checkpointProcessing.getCheckpoint(), from, regent, getMember());
            return;
        }
        cancel(Timers.CHECKPOINT_TIMEOUT);
        schedule(Timers.CHECKPOINT_TIMEOUT, () -> consortium.getTransitions().checkpointTimeout(),
                 consortium.getParams().submitTimeout);
        log.debug("deivering checkpoint processing: {} from: {} on: {}", checkpointProcessing.getCheckpoint(), from,
                  getMember());
        toOrder.values().forEach(eqt -> {
            eqt.cancel();
            eqt.setTimedOut(false);
            schedule(eqt);
        });
    }

    public void deliverStop(Stop data, Member from) {
        regency.deliverStop(data, from, consortium, view, toOrder, processed);
    }

    public void deliverStopData(StopData stopData, Member from) {
        regency.deliverStopData(stopData, from, view, consortium);
    }

    public void deliverSync(Sync syncData, Member from) {
        regency.deliverSync(syncData, from, view, this);
    }

    public void deliverValidate(Validate v) {
        HashKey hash = new HashKey(v.getHash());
        CertifiedBlock.Builder certifiedBlock = workingBlocks.get(hash);
        if (certifiedBlock == null) {
            log.trace("No working block to validate: {} on: {}", hash, consortium.getMember());
            return;
        }
        log.debug("Delivering validate: {} on: {}", hash, consortium.getMember());
        HashKey memberID = new HashKey(v.getId());
        if (view.getContext().validate(certifiedBlock.getBlock(), v)) {
            certifiedBlock.addCertifications(Certification.newBuilder()
                                                          .setId(v.getId())
                                                          .setSignature(v.getSignature()));
            log.debug("Adding block validation: {} from: {} on: {} count: {}", hash, memberID, consortium.getMember(),
                      certifiedBlock.getCertificationsCount());
        } else {
            log.debug("Failed block validation: {} from: {} on: {}", hash, memberID, consortium.getMember());
        }
    }

    public void establishGenesisView() {
        ViewContext newView = new ViewContext(consortium.getParams().genesisViewId, consortium.getParams().context,
                consortium.getMember(), this.view.nextViewConsensusKey(), Collections.emptyList());
        newView.activeAll();
        view.viewChange(newView, consortium.getScheduler(), 0, consortium.service);
        if (view.getContext().isMember()) {
            regency.currentRegent(-1);
            regency.nextRegent(-2);
            view.pause();
            consortium.joinMessageGroup(newView);
            consortium.getTransitions().generateView();
            view.resume(consortium.service);
        }
    }

    public void establishNextRegent() {
        if (regency.currentRegent() == regency.nextRegent()) {
            log.trace("Regent already established on {}", consortium.getMember());
            return;
        }
        regency.currentRegent(regency.nextRegent());
        reschedule();
        Member leader = getRegent(regency.currentRegent());
        StopData stopData = buildStopData(regency.currentRegent());
        if (stopData == null) {
            return;
        }
        if (consortium.getMember().equals(leader)) {
            consortium.getTransitions().synchronizingLeader();
            consortium.getTransitions().deliverStopData(stopData, consortium.getMember());
        } else {
            ConsortiumClient link = consortium.linkFor(leader);
            if (link == null) {
                log.warn("Cannot get link to leader: {} on: {}", leader, consortium.getMember());
            } else {
                try {
                    log.trace("Sending StopData: {} regent: {} on: {}", regency.currentRegent(), leader,
                              consortium.getMember());
                    link.stopData(stopData);
                } catch (Throwable e) {
                    log.warn("Error sending stop data: {} to: {} on: {}", regency.currentRegent(), leader,
                             consortium.getMember());
                } finally {
                    link.release();
                }
            }
        }
    }

    public void generateBlock() {
        generateNextBlock(needCheckpoint());
    }

    public void generateView() {
        if (consortium.getCurrent() == null) {
            log.trace("Generating genesis view on: {}", consortium.getMember());
            generateGenesisView();
        } else {
            log.trace("Generating view on: {}", consortium.getMember());
            generateNextView();
        }
    }

    public int getCurrentRegent() {
        return regency.currentRegent();
    }

    public Member getMember() {
        return consortium.getMember();
    }

    public void initializeConsensus() {
        if (currentConsensus() >= 0) {
            return;
        }
        HashedCertifiedBlock current = consortium.getCurrent();
        currentConsensus(current != null ? current.height() : 0);
    }

    public boolean isRegent(int regency) {
        Member regent = getRegent(regency);
        return consortium.getMember().equals(regent);
    }

    public void join() {
        log.debug("Petitioning to join view: {} on: {}", view.getContext().getId(), consortium.getParams().member);

        Join voteForMe = Join.newBuilder()
                             .setMember(consortium.getCurrent() == null
                                     ? view.getContext().getView(consortium.getParams().signature.get())
                                     : view.getNextView())
                             .setContext(view.getContext().getId().toByteString())
                             .build();

        List<Member> group = view.getContext().streamRandomRing().collect(Collectors.toList());
        AtomicInteger pending = new AtomicInteger(group.size());
        AtomicInteger success = new AtomicInteger();
        AtomicBoolean completed = new AtomicBoolean();
        List<Result> votes = new CopyOnWriteArrayList<>();
        group.forEach(c -> {
            if (getMember().equals(c)) {
                return;
            }
            ConsortiumClient link = consortium.linkFor(c);
            if (link == null) {
                log.debug("Cannot get link for {}", c.getId());
                pending.decrementAndGet();
                return;
            }
            ListenableFuture<JoinResult> futureSailor = link.join(voteForMe);
            futureSailor.addListener(() -> {
                if (completed.get()) {
                    return;
                }
                int succeeded;
                try {
                    votes.add(new Consortium.Result(c, futureSailor.get()));
                    succeeded = success.incrementAndGet();
                } catch (InterruptedException e) {
                    log.trace("error submitting join txn to {} on: {}", c, getMember(), e);
                    succeeded = success.get();
                } catch (ExecutionException e) {
                    succeeded = success.get();
                    log.trace("error submitting join txn to {} on: {}", c, getMember(), e.getCause());
                }
                processSubmit(voteForMe, votes, pending, completed, succeeded);
            }, ForkJoinPool.commonPool()); // TODO, put in passed executor
        });
    }

    public void joinView() {
        joinView(0);
    }

    public int nextRegent() {
        return regency.nextRegent();
    }

    public void receive(ReplicateTransactions transactions, Member from) {
        transactions.getTransactionsList().forEach(txn -> receive(txn, true));
    }

    public void receive(Transaction txn) {
        receive(txn, false);
    }

    public boolean receive(Transaction txn, boolean replicated) {
        EnqueuedTransaction transaction = new EnqueuedTransaction(Consortium.hashOf(txn), txn);
        if (processed.contains(transaction.hash)) {
            return false;
        }
        AtomicBoolean added = new AtomicBoolean(); // this is stupid
        toOrder.computeIfAbsent(transaction.hash, k -> {
            if (txn.getJoin()) {
                JoinTransaction join;
                try {
                    join = txn.getTxn().unpack(JoinTransaction.class);
                } catch (InvalidProtocolBufferException e) {
                    log.debug("Cannot deserialize join on: {}", consortium.getMember(), e);
                    return null;
                }
                HashKey memberID = new HashKey(join.getMember().getId());
                log.trace("Join txn:{} received on: {} self join: {}", transaction.hash, consortium.getMember(),
                          consortium.getMember().getId().equals(memberID));
            } else {
                log.trace("Client txn:{} received on: {} ", transaction.hash, consortium.getMember());
            }
            transaction.setTimedOut(replicated);
            schedule(transaction);
            added.set(true);
            return transaction;
        });
        return added.get();
    }

    public void receiveJoin(Transaction txn) {
        if (!txn.getJoin()) {
            return;
        }
        receive(txn);
        reduceJoinTransactions();
    }

    public void receiveJoins(ReplicateTransactions transactions, Member from) {
        receive(transactions, from);
        reduceJoinTransactions();
    }

    public void reschedule(List<EnqueuedTransaction> transactions) {
        transactions.forEach(txn -> {
            txn.setTimedOut(false);
            schedule(txn);
        });
    }

    public void resolveRegentStatus() {
        Member regent = getRegent(regency.nextRegent());
        log.debug("Regent: {} on: {}", regent, consortium.getMember());
        if (consortium.getMember().equals(regent)) {
            consortium.getTransitions().becomeLeader();
        } else {
            consortium.getTransitions().becomeFollower();
        }
    }

    public void scheduleCheckpointBlock() {
        final long currentHeight = lastBlock();
        consortium.performAfter(() -> generateCheckpointBlock(currentHeight), currentHeight);
    }

    public void shutdown() {
        consortium.stop();
    }

    public void synchronize(int elected, Map<Member, StopData> regencyData) {
        log.trace("Start synchronizing: {} votes: {} on: {}", elected, regencyData.size(), consortium.getMember());
        Sync synch = buildSync(elected, regencyData);
        if (synch != null) {
            log.debug("Synchronizing new regent: {} on: {} voting: {}", elected, consortium.getMember(),
                      regencyData.keySet().stream().map(e -> e.getId()).collect(Collectors.toList()));
            consortium.getTransitions().synchronizingLeader();
            view.publish(synch);
            consortium.getTransitions().deliverSync(synch, consortium.getMember());
        } else {
            log.error("Cannot generate Sync regent: {} on: {} voting: {}", elected, consortium.getMember(),
                      regencyData.keySet().stream().map(e -> e.getId()).collect(Collectors.toList()));
        }
    }

    public void totalOrderDeliver() {
        long current = currentConsensus();
        log.trace("Attempting total ordering of working blocks: {} current consensus: {} on: {}", workingBlocks.size(),
                  current, consortium.getMember());
        List<HashKey> published = new ArrayList<>();
        workingBlocks.entrySet()
                     .stream()
                     .filter(e -> e.getValue().getCertificationsCount() >= view.getContext().majority())
                     .sorted((a, b) -> Long.compare(height(a.getValue().getBlock()), height(b.getValue().getBlock())))
                     .filter(e -> height(e.getValue().getBlock()) >= current + 1)
                     .forEach(e -> {
                         long height = height(e.getValue().getBlock());
                         if (height == current + 1) {
                             currentConsensus(height);
                             log.info("Totally ordering block: {} height: {} on: {}", e.getKey(), height,
                                      consortium.getMember());
                             consortium.getParams().consensus.apply(e.getValue().build(), null);
                             published.add(e.getKey());
                         }
                     });
        published.forEach(h -> {
            Builder removed = workingBlocks.remove(h);
            if (removed != null) {
                Block block = removed.getBlock();
                if (block.getBody().getType() == BodyType.CHECKPOINT) {
                    consortium.getTransitions().checkpointGenerated();
                }
            }
        });
    }

    void clear() {
        regency.currentRegent(-1);
        regency.nextRegent(-2);
        currentConsensus(-1);
        timers.values().forEach(e -> e.cancel());
        timers.clear();
        cancelToTimers();
        toOrder.clear();
        lastBlock(null);
        consortium.getScheduler().cancelAll();
        workingBlocks.clear();
    }

    Map<HashKey, EnqueuedTransaction> getToOrder() {
        return toOrder;
    }

    void processCheckpoint(HashedCertifiedBlock next) {
        Checkpoint body = checkpointBody(next.block.getBlock());
        if (body == null) {
            return;
        }
        HashKey hash = next.hash;
        CheckpointState checkpointState = checkpoint(body);
        if (checkpointState == null) {
            log.error("Cannot checkpoint: {} on: {}", hash, getMember());
            consortium.getTransitions().fail();
            return;
        }
        consortium.checkpoint(next.height(), checkpointState);
        accept(next);
        consortium.setLastCheckpoint(next);
        log.info("Processed checkpoint block: {} height: {} on: {}", hash, next.height(), consortium.getMember());
        store().gcFrom(height(next.block.getBlock()));
    }

    void processGenesis(HashedCertifiedBlock next) {
        Genesis body = genesisBody(next.block.getBlock());
        if (body == null) {
            return;
        }
        accept(next);
        cancelToTimers();
        toOrder.clear();
        consortium.getSubmitted().clear();
        consortium.setGenesis(next);
        consortium.getTransitions().genesisAccepted();
        reconfigure(next, body.getInitialView(), true);
        TransactionExecutor exec = consortium.getParams().executor;
        exec.beginBlock(0, next.hash);
        exec.processGenesis(body.getGenesisData());
        log.info("Processed genesis block: {} on: {}", next.hash, consortium.getMember());
    }

    void processReconfigure(HashedCertifiedBlock next) {
        Reconfigure body = reconfigureBody(next.block.getBlock());
        if (body == null) {
            return;
        }
        accept(next);
        reconfigure(next, body, false);
        log.info("Processed reconfigure block: {} height: {} on: {}", next.hash, next.height(), consortium.getMember());
    }

    void processUser(HashedCertifiedBlock next) {
        User body = userBody(next.block.getBlock());
        if (body == null) {
            return;
        }
        long height = next.height();
        TransactionExecutor exec = consortium.getParams().executor;
        exec.beginBlock(height, next.hash);
        body.getTransactionsList().forEach(txn -> {
            HashKey hash = new HashKey(txn.getHash());
            finalized(hash);
            SubmittedTransaction submitted = consortium.getSubmitted().remove(hash);
            exec.execute(next.hash, txn, submitted == null ? null : submitted.onCompletion);
        });
        accept(next);
        log.info("Processed user block: {} height: {} on: {}", next.hash, next.height(), consortium.getMember());
    }

    void reconfigure(HashedCertifiedBlock block, Reconfigure view, boolean genesis) {
        this.view.pause();
        consortium.setLastViewChange(block, view);
        ViewContext newView = new ViewContext(view, consortium.getParams().context, consortium.getMember(),
                genesis ? this.view.getContext().getConsensusKey() : this.view.nextViewConsensusKey());
        int current = genesis ? 2 : 0;
        regency.currentRegent(current);
        regency.nextRegent(current);
        this.view.viewChange(newView, consortium.getScheduler(), current, consortium.service);
        resolveStatus();
    }

    void synchronize(Sync syncData, Member regent) {
        HashedCertifiedBlock current = consortium.getCurrent();
        final long currentHeight = current != null ? current.height() : -1;
        workingBlocks.clear();
        syncData.getBlocksList()
                .stream()
                .sorted((a, b) -> Long.compare(height(a), height(b)))
                .filter(cb -> height(cb) > currentHeight)
                .forEach(cb -> {
                    HashKey hash = new HashKey(Conversion.hashOf(cb.getBlock().toByteString()));
                    workingBlocks.put(hash, cb.toBuilder());
                    store().put(hash, cb);
                    lastBlock(new HashedBlock(hash, cb.getBlock()));
                    processToOrder(cb.getBlock());
                });
        log.debug("Synchronized from: {} to: {} working blocks: {} on: {}", currentHeight, lastBlock(),
                  workingBlocks.size(), consortium.getMember());
        if (getMember().equals(regent)) {
            totalOrderDeliver();
        }
    }

    private void accept(HashedCertifiedBlock next) {
        workingBlocks.remove(next.hash);
        consortium.setCurrent(next);
        store().put(next.hash, next.block);
    }

    private StopData buildStopData(int currentRegent) {
        Map<HashKey, CertifiedBlock> decided;
        decided = workingBlocks.entrySet()
                               .stream()
                               .filter(e -> e.getValue().getCertificationsCount() >= view.getContext().majority())
                               .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().build()));
        List<Long> gaps = noGaps(decided, store().hashes());
        if (!gaps.isEmpty()) {
            log.debug("Have no valid log on: {} gaps: {}", consortium.getMember(), gaps);
        }
        StopData.Builder builder = StopData.newBuilder().setCurrentRegent(currentRegent);
        builder.setContext(view.getContext().getId().toByteString());
        decided.entrySet().forEach(e -> {
            builder.addBlocks(e.getValue());
        });
        return builder.build();
    }

    private Sync buildSync(int elected, Map<Member, StopData> regencyData) {
        List<CertifiedBlock> blocks = new ArrayList<>();
        Map<HashKey, CertifiedBlock> hashed = new HashMap<>();
        regencyData.values().stream().flatMap(sd -> sd.getBlocksList().stream()).forEach(cb -> {
            hashed.put(new HashKey(Conversion.hashOf(cb.getBlock().toByteString())), cb);
        });
        List<Long> gaps = noGaps(hashed, store().hashes());
        if (!gaps.isEmpty()) {
            log.debug("Gaps in sync log, trimming to valid log on: {} gaps: {}", consortium.getMember(), gaps);
            AtomicLong next = new AtomicLong(-1);
            blocks = hashed.values().stream().sorted((a, b) -> Long.compare(height(a), height(b))).filter(cb -> {
                if (next.get() < 0) {
                    next.set(height(cb) + 1);
                    return true;
                } else {
                    long height = height(cb);
                    return next.compareAndSet(height - 1, height);
                }
            }).collect(Collectors.toList());
        } else {
            blocks = hashed.values()
                           .stream()
                           .sorted((a, b) -> Long.compare(height(a), height(b)))
                           .collect(Collectors.toList());
        }
        return Sync.newBuilder()
                   .setCurrentRegent(elected)
                   .setContext(view.getContext().getId().toByteString())
                   .addAllBlocks(blocks)
                   .build();
    }

    private void cancelToTimers() {
        toOrder.values().forEach(eqt -> eqt.cancel());
    }

    private CheckpointState checkpoint(Checkpoint body) {
        HashKey stateHash;
        CheckpointState checkpoint = consortium.getChekpoint(body.getCheckpoint());
        HashKey bsh = new HashKey(body.getStateHash());
        if (checkpoint != null) {
            if (!body.getStateHash().equals(checkpoint.checkpoint.getStateHash())) {
                log.error("Invalid checkpoint state hash: {} does not equal recorded: {} on: {}",
                          new HashKey(checkpoint.checkpoint.getStateHash()), bsh, getMember());
                return null;
            }
        } else {
            File state = consortium.getParams().checkpointer.apply(body.getCheckpoint());
            if (state == null) {
                log.error("Invalid checkpoint on: {}", getMember());
                return null;
            }
            try (FileInputStream fis = new FileInputStream(state)) {
                stateHash = new HashKey(Conversion.hashOf(fis));
            } catch (IOException e) {
                log.error("Invalid checkpoint!", e);
                return null;
            }
            if (!stateHash.equals(bsh)) {
                log.error("Cannot replicate checkpoint: {} state hash: {} does not equal recorded: {} on: {}",
                          body.getCheckpoint(), stateHash, bsh, getMember());
                state.delete();
                return null;
            }
            MVMap<Integer, byte[]> stored = store().putCheckpoint(body.getCheckpoint(), state, body);
            checkpoint = new CheckpointState(body, stored);
            state.delete();
        }
        return checkpoint;
    }

    private long currentConsensus() {
        return currentConsensus.get();
    }

    private void currentConsensus(long l) {
        currentConsensus.set(l);
    }

    private void deliverCheckpointBlock(Block block, Member from) {
        Member regent = view.getContext().getRegent(regency.currentRegent());
        if (!regent.equals(from)) {
            log.debug("Ignoring checkpoint block from non regent: {} actual: {} on: {}", from, regent, getMember());
            return;
        }
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        Checkpoint body = checkpointBody(block);
        if (body == null) {
            log.debug("Ignoring checkpoint invalid checkpoint body of block from: {} on: {}", from, regent,
                      getMember());
            return;
        }
        Builder cb = workingBlocks.get(hash);
        if (cb == null) {
            log.debug("Delivering checkpoint block: {} from: {} on: {}", hash, from, getMember());
            workingBlocks.put(hash, CertifiedBlock.newBuilder().setBlock(block));
            consortium.performAfter(() -> validateCheckpoint(hash, block, from), body.getCheckpoint());
        }
    }

    private void deliverGenesisBlock(final Block block, Member from) {
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        long height = height(block);
        if (height != 0) {
            log.debug("Rejecting genesis block proposal: {} height: {} from {}, not block height 0", hash, height,
                      from);
        }

        if (block.getBody().getType() != BodyType.GENESIS) {
            log.error("Failed on {} [{}] prev: [{}] delivering genesis block: {} invalid body: {}",
                      consortium.getMember(), consortium.fsm.prettyPrint(consortium.fsm.getCurrentState()),
                      consortium.fsm.prettyPrint(consortium.fsm.getPreviousState()), hash, block.getBody().getType());
            return;
        }
        Builder existing = workingBlocks.get(hash);
        if (existing == null) {
            final Genesis genesis = genesisBody(block);
            if (genesis == null) {
                return;
            }
            cancelToTimers();
            toOrder.clear();
            Validate validation = view.getContext().generateValidation(hash, block);
            if (validation == null) {
                log.error("Cannot validate generated genesis: {} on: {}", hash, consortium.getMember());
                return;
            }
            lastBlock(new HashedBlock(block));
            view.setContext(view.getContext().cloneWith(genesis.getInitialView().getViewList()));
            view.publish(validation);
            workingBlocks.put(hash,
                              CertifiedBlock.newBuilder()
                                            .setBlock(block)
                                            .addCertifications(Certification.newBuilder()
                                                                            .setId(validation.getId())
                                                                            .setSignature(validation.getSignature())));
        }
    }

    private void finalized(HashKey hash) {
        EnqueuedTransaction removed = toOrder.remove(hash);
        if (removed != null) {
            removed.cancel();
            processed.add(removed.hash);
        }
    }

    private void firstTimeout(EnqueuedTransaction transaction) {
        assert !transaction.isTimedOut() : "this should be unpossible";
        transaction.setTimedOut(true);

        Parameters params = consortium.getParams();
        long span = transaction.transaction.getJoin() ? params.joinTimeout.toMillis() : params.submitTimeout.toMillis();

        chunked(toOrder.values().stream().filter(eqt -> eqt.getDelay() <= span).peek(eqt -> {
            eqt.cancel();
            eqt.setTimedOut(true);
            eqt.setTimer(consortium.getScheduler()
                                   .schedule(Timers.TRANSACTION_TIMEOUT_2, () -> secondTimeout(eqt),
                                             eqt.transaction.getJoin() ? params.joinTimeout : params.submitTimeout));
        }).map(eqt -> eqt.transaction), 100).forEach(txns -> {
            log.debug("Replicating from: {} count: {} first timeout on: {}", transaction.hash, txns.size(),
                      consortium.getMember());
            view.publish(ReplicateTransactions.newBuilder().addAllTransactions(txns).build());
        });
    }

    private void generateCheckpointBlock(long currentHeight) {
        final long thisHeight = lastBlock() + 1;

        log.info("Generating checkpoint block on: {} height: {} ", consortium.getMember(), currentHeight);
        view.publish(CheckpointProcessing.newBuilder().setCheckpoint(currentHeight).build());

        schedule(Timers.CHECKPOINTING, () -> {
            view.publish(CheckpointProcessing.newBuilder().setCheckpoint(currentHeight).build());
        }, consortium.getParams().submitTimeout.dividedBy(2));

        File state = consortium.getParams().checkpointer.apply(currentHeight);
        if (state == null) {
            log.error("Cannot create checkpoint");
            consortium.getTransitions().fail();
            return;
        }
        Checkpoint checkpoint = checkpoint(currentHeight, state, consortium.getParams().checkpointBlockSize);
        if (checkpoint == null) {
            consortium.getTransitions().fail();
        }

        HashedBlock lb = lastBlock.get();
        byte[] previous = lb == null ? null : lb.hash.bytes();
        if (previous == null) {
            log.error("Cannot generate checkpoint block on: {} height: {} no previous block: {}",
                      consortium.getMember(), currentHeight, lastBlock());
            consortium.getTransitions().fail();
            return;
        }
        Block block = generateBlock(consortium.getLastCheckpointBlock(), thisHeight, previous,
                                    body(BodyType.CHECKPOINT, checkpoint), consortium.getLastViewChangeBlock());

        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));

        CertifiedBlock.Builder builder = workingBlocks.computeIfAbsent(hash, k -> CertifiedBlock.newBuilder()
                                                                                                .setBlock(block));

        Validate validation = view.getContext().generateValidation(hash, block);
        if (validation == null) {
            log.debug("Cannot generate validation for block: {} hash: {} segements: {} on: {}", hash,
                      new HashKey(checkpoint.getStateHash()), checkpoint.getSegmentsCount(), consortium.getMember());
            consortium.getTransitions().fail();
            cancel(Timers.CHECKPOINTING);
            return;
        }
        builder.addCertifications(Certification.newBuilder()
                                               .setId(validation.getId())
                                               .setSignature(validation.getSignature()));
        lastBlock(new HashedBlock(hash, block));
        view.publish(block);
        view.publish(validation);
        cancel(Timers.CHECKPOINTING);

        log.info("Generated next checkpoint block: {} height: {} on: {} ", hash, thisHeight, consortium.getMember());
    }

    private void generateGenesisBlock() {
        reduceJoinTransactions();
        assert toOrder.size() >= view.getContext().majority() : "Whoops";
        log.debug("Generating genesis on {} join transactions: {}", consortium.getMember(), toOrder.size());
        byte[] nextView = new byte[32];
        Utils.secureEntropy().nextBytes(nextView);
        Reconfigure.Builder genesisView = Reconfigure.newBuilder()
                                                     .setCheckpointBlocks(consortium.getParams().deltaCheckpointBlocks)
                                                     .setId(ByteString.copyFrom(nextView))
                                                     .setTolerance(view.getContext().majority());
        toOrder.values().forEach(join -> {
            JoinTransaction txn;
            try {
                txn = join.transaction.getTxn().unpack(JoinTransaction.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("Cannot generate genesis, unable to parse Join txnL {} on: {}", join.hash,
                          consortium.getMember());
                consortium.getTransitions().fail();
                return;
            }
            processed.add(join.hash);
            genesisView.addTransactions(ExecutedTransaction.newBuilder()
                                                           .setHash(join.hash.toByteString())
                                                           .setTransaction(join.transaction)
                                                           .build());
            genesisView.addView(txn.getMember());
        });

        if (log.isTraceEnabled()) {
            StringBuilder b = new StringBuilder();
            b.append("\n");
            for (ViewMember vm : genesisView.getViewList()) {
                b.append("view member: ");
                b.append(new HashKey(vm.getId()));
                b.append(" key: ");
                b.append(new HashKey(Conversion.hashOf(vm.getConsensusKey())));
                b.append("\n");
            }
            log.trace("genesis view {}", b.toString());
        }
        toOrder.values().forEach(e -> e.cancel());
        toOrder.clear();
        Block block = generateBlock(consortium.getLastCheckpointBlock(), (long) 0,
                                    consortium.getParams().genesisViewId.bytes(),
                                    body(BodyType.GENESIS,
                                         Genesis.newBuilder()
                                                .setGenesisData(Any.pack(consortium.getParams().genesisData))
                                                .setInitialView(genesisView)
                                                .build()),
                                    consortium.getLastViewChangeBlock());
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));
        log.info("Genesis block: {} generated on {}", hash, consortium.getMember());
        Builder cb = workingBlocks.get(hash);
        if (cb == null) {
            lastBlock(new HashedBlock(hash, block));
            Validate validation = view.getContext().generateValidation(hash, block);
            if (validation == null) {
                log.error("Cannot validate generated genesis block: {} on: {}", hash, consortium.getMember());
            }
            CertifiedBlock.Builder builder = CertifiedBlock.newBuilder().setBlock(block);
            builder.addCertifications(Certification.newBuilder()
                                                   .setId(validation.getId())
                                                   .setSignature(validation.getSignature()));
            workingBlocks.put(hash, builder);
            view.setContext(view.getContext().cloneWith(genesisView.getViewList()));
            view.publish(block);
            view.publish(validation);
        }
    }

    private void generateGenesisView() {
        if (!selfJoinRecorded()) {
            log.trace("Join transaction not found on: {}, rescheduling", consortium.getMember());
            rescheduleGenesis();
            return;
        }
        int txns = toOrder.size();
        if (txns >= view.getContext().activeCardinality()) {
            generateGenesisBlock();
        } else {
            log.trace("Genesis group has not formed, rescheduling, have: {} want: {} on: {}", txns,
                      view.getContext().activeCardinality(), consortium.getMember());
            rescheduleGenesis();
        }
    }

    /**
     * generate ye next block.
     * 
     * @return true if another block "should" be generated (i.e. enough backed up
     *         txns, etc), false if we should schedule the next flush
     */
    private boolean generateNextBlock() {
        final long currentHeight = lastBlock();
        final long thisHeight = currentHeight + 1;
        HashedBlock lb = lastBlock.get();
        byte[] curr = lb == null ? null : lb.hash.bytes();
        if (curr == null) {
            log.debug("Cannot generate next block: {} on: {}, as previous block for height: {} not found", thisHeight,
                      consortium.getMember(), currentHeight);
            return false;
        }
        List<EnqueuedTransaction> batch = nextBatch();
        if (batch.isEmpty()) {
            return false;
        }
        User.Builder user = User.newBuilder();
        List<HashKey> processed = new ArrayList<>();

        batch.forEach(eqt -> {
            user.addTransactions(ExecutedTransaction.newBuilder()
                                                    .setHash(eqt.hash.toByteString())
                                                    .setTransaction(eqt.transaction));
            processed.add(eqt.hash);
        });

        if (processed.size() == 0) {
            log.debug("No transactions to generate block on: {}", consortium.getMember());
            return false;
        }
        log.debug("Generating next block on: {} height: {} transactions: {}", consortium.getMember(), thisHeight,
                  processed.size());

        Block block = generateBlock(consortium.getLastCheckpointBlock(), thisHeight, curr,
                                    body(BodyType.USER, user.build()), consortium.getLastViewChangeBlock());
        HashKey hash = new HashKey(Conversion.hashOf(block.toByteString()));

        CertifiedBlock.Builder builder = workingBlocks.computeIfAbsent(hash, k -> CertifiedBlock.newBuilder()
                                                                                                .setBlock(block));

        Validate validation = view.getContext().generateValidation(hash, block);
        if (validation == null) {
            log.debug("Cannot generate validation for block: {} on: {}", hash, consortium.getMember());
            return false;
        }
        builder.addCertifications(Certification.newBuilder()
                                               .setId(validation.getId())
                                               .setSignature(validation.getSignature()));
        lastBlock(new HashedBlock(hash, block));
        view.publish(block);
        view.publish(validation);

        log.info("Generated next block: {} height: {} on: {} txns: {}", hash, thisHeight, consortium.getMember(),
                 user.getTransactionsCount());
        return true;
    }

    private void generateNextBlock(boolean needCheckpoint) {
        if (needCheckpoint) {
            consortium.getTransitions().generateCheckpoint();
        } else {
            generateNextBlock();
            if (needCheckpoint()) {
                consortium.getTransitions().generateCheckpoint();
            } else {
                scheduleFlush();
            }
        }
    }

    private void generateNextView() {
        // TODO Auto-generated method stub

    }

    private Member getRegent(int regent) {
        return view.getContext().getRegent(regent);
    }

    private void joinView(int attempt) {
        boolean selfJoin = selfJoinRecorded();
        int majority = view.getContext().majority();
        if (attempt < 20) {
            if (selfJoin && toOrder.size() == view.getContext().activeCardinality()) {
                log.trace("View formed, attempt: {} on: {} have: {} require: {} self join: {}", attempt,
                          consortium.getMember(), toOrder.size(), majority, selfJoin);
                consortium.getTransitions().formView();
                return;
            }
        }

        if (attempt >= 20) {
            if (selfJoin && toOrder.size() >= majority) {
                log.trace("View formed, attempt: {} on: {} have: {} require: {} self join: {}", attempt,
                          consortium.getMember(), toOrder.size(), majority, selfJoin);
                consortium.getTransitions().formView();
                return;
            }
        }

        log.trace("View has not been formed, attempt: {} rescheduling on: {} have: {} require: {} self join: {}",
                  attempt, consortium.getMember(), toOrder.size(), majority, selfJoin);
        join();
        schedule(Timers.AWAIT_VIEW_MEMBERS, () -> joinView(attempt + 1), consortium.getParams().viewTimeout);
    }

    private long lastBlock() {
        HashedBlock lb = lastBlock.get();
        return lb == null ? -1 : lb.height();
    }

    private void lastBlock(HashedBlock block) {
        lastBlock.set(block);
    }

    private boolean needCheckpoint() {
        return lastBlock() >= consortium.targetCheckpoint();
    }

    private List<EnqueuedTransaction> nextBatch() {
        if (toOrder.isEmpty()) {
//            log.debug("No transactions available to batch on: {}:{}", consortium.getMember(), toOrder.size());
            return Collections.emptyList();
        }

        AtomicInteger processedBytes = new AtomicInteger(0);
        List<EnqueuedTransaction> batch = toOrder.values()
                                                 .stream()
                                                 .limit(consortium.getParams().maxBatchSize)
                                                 .filter(eqt -> eqt != null)
                                                 .filter(eqt -> processedBytes.addAndGet(eqt.getSerializedSize()) <= consortium.getParams().maxBatchByteSize)
                                                 .peek(eqt -> eqt.cancel())
                                                 .collect(Collectors.toList());
        batch.forEach(eqt -> {
            eqt.cancel();
            toOrder.remove(eqt.hash);
            processed.add(eqt.hash);
        });
        return batch;
    }

    private void processSubmit(Join voteForMe, List<Result> votes, AtomicInteger pending, AtomicBoolean completed,
                               int succeeded) {
        int remaining = pending.decrementAndGet();
        int majority = view.getContext().majority();
        if (succeeded >= majority) {
            if (completed.compareAndSet(false, true)) {
                JoinTransaction.Builder txn = JoinTransaction.newBuilder().setMember(voteForMe.getMember());
                for (Result vote : votes) {
                    txn.addCertification(Certification.newBuilder()
                                                      .setId(vote.member.getId().toByteString())
                                                      .setSignature(vote.vote.getSignature()));
                }
                JoinTransaction joinTxn = txn.build();

                HashKey txnHash;
                try {
                    txnHash = consortium.submit(null, true, null, joinTxn);
                } catch (TimeoutException e) {
                    return;
                }
                log.debug("Successfully petitioned: {} to join view: {} on: {}", txnHash, view.getContext().getId(),
                          consortium.getParams().member);
            }
        } else {
            if (remaining + succeeded < majority) {
                if (completed.compareAndSet(false, true)) {
                    if (votes.size() < view.getContext().majority()) {
                        log.debug("Did not gather votes necessary to join consortium needed: {} got: {} on: {}",
                                  view.getContext().majority(), votes.size(), consortium.getMember());
                    }
                }
            }
        }
    }

    private void processToOrder(Block block) {
        switch (block.getBody().getType()) {
        case CHECKPOINT: {
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

    @SuppressWarnings("unused")
    // Incrementally assemble target checkpoint from random gossip
    private void recoverFromCheckpoint() {

    }

    private void reduceJoinTransactions() {
        Map<HashKey, EnqueuedTransaction> reduced = new HashMap<>(); // Member ID -> join txn
        toOrder.forEach((h, eqt) -> {
            try {
                JoinTransaction join = eqt.transaction.getTxn().unpack(JoinTransaction.class);
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
        reduced.values().forEach(eqt -> toOrder.put(eqt.hash, eqt));
    }

    private void reschedule() {
        toOrder.values().forEach(eqt -> {
            eqt.setTimedOut(false);
            schedule(eqt);
        });
    }

    private void rescheduleGenesis() {
        schedule(Timers.AWAIT_GROUP, () -> {
            boolean selfJoin = selfJoinRecorded();
            if (selfJoin && toOrder.size() >= view.getContext().majority()) {
                generateGenesisBlock();
            } else {
                log.trace("Genesis group has not formed, rescheduling, have: {} want: {} self join: {} on: {}",
                          toOrder.size(), view.getContext().majority(), selfJoin, consortium.getMember());
                rescheduleGenesis();
            }
        }, consortium.getParams().viewTimeout);
    }

    private void resolveStatus() {
        Member regent = getRegent(regency.currentRegent());
        if (view.getContext().isViewMember()) {
            if (consortium.getMember().equals(regent)) {
                log.debug("becoming leader on: {}", consortium.getMember());
                consortium.getTransitions().becomeLeader();
            } else {
                log.debug("becoming follower on: {} regent: {}", consortium.getMember(), regent);
                consortium.getTransitions().becomeFollower();
            }
        } else if (view.getContext().isMember()) {
            log.debug("becoming joining member on: {} regent: {}", consortium.getMember(), regent);
            consortium.getTransitions().joinAsMember();
        } else {
            log.debug("becoming client on: {} regent: {}", consortium.getMember(), regent);
            consortium.getTransitions().becomeClient();
        }
    }

    private Timer schedule(EnqueuedTransaction eqt) {
        eqt.cancel();
        Parameters params = consortium.getParams();
        Duration delta = eqt.transaction.getJoin() ? params.joinTimeout : params.submitTimeout;
        Timer timer = consortium.getScheduler().schedule(Timers.TRANSACTION_TIMEOUT_1, () -> {
            if (eqt.isTimedOut()) {
                secondTimeout(eqt);
            } else {
                firstTimeout(eqt);
            }
        }, delta);
        eqt.setTimer(timer);
        return timer;
    }

    private void schedule(Timers label, Runnable a, Duration delta) {
        Transitions timerState = consortium.fsm.getCurrentState();
        Runnable action = () -> {
            timers.remove(label);
            Transitions currentState = consortium.fsm.getCurrentState();
            if (timerState.equals(currentState)) {
                a.run();
            } else {
                log.debug("discarding timer for: {} scheduled on: {} but timed out: {} on: {}", label, timerState,
                          currentState, consortium.getMember());
            }
        };
        timers.computeIfAbsent(label, k -> {
            log.trace("Setting timer for: {} duration: {} ms on: {}", label, delta.toMillis(), consortium.getMember());
            return consortium.getScheduler().schedule(k, action, delta);
        });
    }

    private void scheduleFlush() {
        schedule(Timers.FLUSH_BATCH, () -> generateBlock(), consortium.getParams().maxBatchDelay);
    }

    private void secondTimeout(EnqueuedTransaction transaction) {
        log.debug("Second timeout for: {} on: {}", transaction.hash, consortium.getMember());
        Parameters params = consortium.getParams();
        long span = transaction.transaction.getJoin() ? params.joinTimeout.toMillis() : params.submitTimeout.toMillis();
        List<EnqueuedTransaction> timedOut = toOrder.values()
                                                    .stream()
                                                    .filter(eqt -> eqt.getDelay() <= span)
                                                    .limit(99)
                                                    .peek(eqt -> eqt.cancel())
                                                    .collect(Collectors.toList());
        consortium.getTransitions().startRegencyChange(timedOut);
    }

    private boolean selfJoinRecorded() {
        HashKey id = consortium.getMember().getId();
        return toOrder.values().stream().map(eqt -> eqt.transaction).map(t -> {
            try {
                return t.getTxn().unpack(JoinTransaction.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("Cannot generate genesis, unable to parse Join txn on: {}", consortium.getMember());
                return null;
            }
        }).filter(jt -> jt != null).anyMatch(jt -> id.equals(new HashKey(jt.getMember().getId())));
    }

    private Store store() {
        return consortium.store;
    }

    private User userBody(Block block) {
        User body;
        try {
            body = User.parseFrom(Consortium.getBody(block));
        } catch (IOException e) {
            log.debug("Protocol violation.  Cannot decode reconfiguration body", e);
            return null;
        }
        return body;
    }

    private void validateCheckpoint(HashKey hash, Block block, Member from) {
        Checkpoint checkpoint = checkpointBody(block);

        HashedCertifiedBlock current = consortium.getCurrent();
        if (current.height() >= checkpoint.getCheckpoint()) {
            if (checkpoint(checkpoint) == null) {
                log.error("Unable to generate checkpoint: {} on {}", checkpoint.getCheckpoint(), getMember());
                consortium.getTransitions().fail();
                return;
            }
            Validate validation = view.getContext().generateValidation(hash, block);
            if (validation == null) {
                log.debug("Rejecting checkpoint block proposal: {}, cannot validate from: {} on: {}", hash, from,
                          consortium.getMember());
                workingBlocks.remove(hash);
                return;
            }
            log.debug("Validating checkpoint block: {} from: {} on: {}", hash, from, getMember());
            view.publish(validation);
            deliverValidate(validation);
            consortium.getTransitions().checkpointGenerated();
        } else {
            log.debug("Rescheduing checkpoint block: {} from: {} on: {}", hash, from, getMember());
            schedule(Timers.CHECKPOINTING, () -> validateCheckpoint(hash, block, from),
                     consortium.getParams().submitTimeout.dividedBy(4));
        }
    }
}
