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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
import com.salesforce.apollo.consortium.comms.LinearClient;
import com.salesforce.apollo.consortium.fsm.Transitions;
import com.salesforce.apollo.consortium.support.Bootstrapper;
import com.salesforce.apollo.consortium.support.Bootstrapper.SynchronizedState;
import com.salesforce.apollo.consortium.support.CheckpointState;
import com.salesforce.apollo.consortium.support.EnqueuedTransaction;
import com.salesforce.apollo.consortium.support.HashedBlock;
import com.salesforce.apollo.consortium.support.HashedCertifiedBlock;
import com.salesforce.apollo.consortium.support.ProcessedBuffer;
import com.salesforce.apollo.consortium.support.SubmittedTransaction;
import com.salesforce.apollo.consortium.support.TickScheduler.Timer;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.utils.Utils;

/**
 * Context for the state machine. These are the leaf actions driven by the FSM.
 *
 */
public class CollaboratorContext implements Collaborator {

    private static final Logger log = LoggerFactory.getLogger(CollaboratorContext.class);

    public static Body body(BodyType type, Message contents) {
        return Body.newBuilder().setType(type).setContents(Consortium.compress(contents.toByteString())).build();
    }

    public static Checkpoint checkpoint(DigestAlgorithm algo, File state, int blockSize) {
        Digest stateHash = algo.getOrigin();
        long length = 0;
        if (state != null) {
            try (FileInputStream fis = new FileInputStream(state)) {
                stateHash = algo.digest(fis);
            } catch (IOException e) {
                log.error("Invalid checkpoint!", e);
                return null;
            }
            length = state.length();
        }
        Checkpoint.Builder builder = Checkpoint.newBuilder()
                                               .setByteSize(length)
                                               .setSegmentSize(blockSize)
                                               .setStateHash(stateHash.toByteString());
        if (state != null) {
            byte[] buff = new byte[blockSize];
            try (FileInputStream fis = new FileInputStream(state)) {
                for (int read = fis.read(buff); read > 0; read = fis.read(buff)) {
                    ByteString segment = ByteString.copyFrom(buff, 0, read);
                    builder.addSegments(algo.digest(segment).toByteString());
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

    public static Block generateBlock(DigestAlgorithm algo, HashedCertifiedBlock checkpoint, final long height,
                                      Digest curr, Body body, HashedCertifiedBlock viewChange) {
        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();
        byte[] nonce = new byte[32];
        Utils.secureEntropy().nextBytes(nonce);
        Digest checkpointHash = checkpoint == null ? Digest.NONE : checkpoint.hash;
        Digest viewChangeHash = viewChange == null ? Digest.NONE : viewChange.hash;
        long checkpointHeight = checkpoint == null ? 0 : checkpoint.height();
        long viewChangeHeight = viewChange == null ? 0 : viewChange.height();
        Block block = Block.newBuilder()
                           .setHeader(Header.newBuilder()
                                            .setTimestamp(timestamp)
                                            .setNonce(ByteString.copyFrom(nonce))
                                            .setLastCheckpoint(checkpointHeight)
                                            .setLastCheckpointHash(checkpointHash.toByteString())
                                            .setLastReconfig(viewChangeHeight)
                                            .setLastReconfigHash(viewChangeHash.toByteString())
                                            .setPrevious(curr.toByteString())
                                            .setHeight(height)
                                            .setBodyHash(algo.digest(body.toByteString()).toByteString())
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

    public static List<Long> noGaps(DigestAlgorithm algo, Collection<CertifiedBlock> blocks, Map<Long, ?> cache) {
        Map<Digest, CertifiedBlock> hashed = blocks.stream()
                                                   .collect(Collectors.toMap(cb -> algo.digest(cb.getBlock()
                                                                                                 .toByteString()),
                                                                             cb -> cb));

        return noGaps(hashed, cache);
    }

    public static List<Long> noGaps(Map<Digest, CertifiedBlock> hashed, Map<Long, ?> cache) {
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

    final Consortium                                  consortium;
    private final AtomicLong                          currentConsensus = new AtomicLong(-1);
    private CompletableFuture<SynchronizedState>      futureBootstrap;
    private ScheduledFuture<?>                        futureSynchronization;
    private final AtomicReference<HashedBlock>        lastBlock        = new AtomicReference<>();
    private final ProcessedBuffer                     processed;
    private final Regency                             regency          = new Regency();
    private final AtomicBoolean                       synchronizing    = new AtomicBoolean(false);
    private final Map<Timers, Timer>                  timers           = new ConcurrentHashMap<>();
    private final Map<Digest, EnqueuedTransaction>    toOrder          = new ConcurrentHashMap<>();
    private final Map<Digest, CertifiedBlock.Builder> workingBlocks    = new ConcurrentHashMap<>();

    CollaboratorContext(Consortium consortium) {
        this.consortium = consortium;
        Parameters params = consortium.params;
        processed = new ProcessedBuffer(params.processedBufferSize);
    }

    @Override
    public void awaitSynchronization() {
        HashedCertifiedBlock anchor = consortium.pollDefered();
        if (anchor != null) {
            recover(anchor);
            return;
        }
        Parameters params = consortium.params;
        futureSynchronization = params.scheduler.schedule(() -> {
            futureSynchronization = null;
            consortium.transitions.synchronizationFailed();
        }, params.viewTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void cancel(Timers t) {
        Timer timer = timers.remove(t);
        if (timer != null) {
            log.trace("Cancelling timer: {} on: {}", t, consortium.getMember());
            timer.cancel();
        } else {
            log.trace("No timer to cancel: {} on: {}", t, consortium.getMember());
        }
    }

    @Override
    public void cancelSynchronization() {
        if (futureSynchronization != null) {
            futureSynchronization.cancel(true);
            futureSynchronization = null;
        }
        if (futureBootstrap != null) {
            futureBootstrap.cancel(true);
            futureBootstrap = null;
        }
    }

    @Override
    public void changeRegency(List<EnqueuedTransaction> transactions) {
        regency.nextRegent(regency.currentRegent() + 1);
        log.info("Starting change of regent from: {} to: {} on: {}", regency.currentRegent(), regency.nextRegent(),
                 consortium.getMember());
        toOrder.values().forEach(t -> t.cancel());
        Stop.Builder data = Stop.newBuilder()
                                .setContext(consortium.view.getContext().getId().toByteString())
                                .setNextRegent(regency.nextRegent());
        transactions.forEach(eqt -> data.addTransactions(eqt.transaction));
        Stop stop = data.build();
        consortium.view.publish(stop);
        consortium.transitions.deliverStop(stop, consortium.getMember());
    }

    @Override
    public void delay(Message message, Member from) {
        consortium.delay(message, from);
    }

    @Override
    public void deliverBlock(Block block, Member from) {
        Member regent = consortium.view.getContext().getRegent(regency.currentRegent());
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
        Digest hash = digestAlgo().digest(block.toByteString());
        Builder cb = workingBlocks.get(hash);
        if (cb == null) {
            log.debug("Delivering block: {} from: {} on: {}", hash, from, getMember());
            workingBlocks.put(hash, CertifiedBlock.newBuilder().setBlock(block));
            processToOrder(block);
            consortium.params.dispatcher.execute(() -> {
                Validate validation = consortium.view.getContext().generateValidation(hash, block);
                if (validation == null) {
                    log.debug("Rejecting block proposal: {}, cannot validate from: {} on: {}", hash, from,
                              consortium.getMember());
                    workingBlocks.remove(hash);
                    return;
                }
                consortium.view.publish(validation);
                deliverValidate(validation);
            });
        }
    }

    @Override
    public void deliverCheckpointing(CheckpointProcessing checkpointProcessing, Member from) {
        Member regent = getRegent(regency.currentRegent());
        if (!regent.equals(from)) {
            log.trace("checkpoint processing: {} ignored from: {} current regent: {} on: {}",
                      checkpointProcessing.getCheckpoint(), from, regent, getMember());
            return;
        }
        cancel(Timers.CHECKPOINT_TIMEOUT);
        schedule(Timers.CHECKPOINT_TIMEOUT, () -> consortium.transitions.checkpointTimeout(),
                 consortium.params.submitTimeout);
        log.debug("deivering checkpoint processing: {} from: {} on: {}", checkpointProcessing.getCheckpoint(), from,
                  getMember());
        toOrder.values().forEach(eqt -> {
            eqt.cancel();
            eqt.setTimedOut(false);
            schedule(eqt);
        });
    }

    @Override
    public void deliverStop(Stop data, Member from) {
        regency.deliverStop(data, from, consortium, consortium.view, toOrder, processed);
    }

    @Override
    public void deliverStopData(StopData stopData, Member from) {
        regency.deliverStopData(stopData, from, consortium.view, consortium);
    }

    @Override
    public void deliverSync(Sync syncData, Member from) {
        regency.deliverSync(syncData, from, consortium.view, this);
    }

    @Override
    public void deliverValidate(Validate v) {
        Digest hash = new Digest(v.getHash());
        CertifiedBlock.Builder certifiedBlock = workingBlocks.get(hash);
        if (certifiedBlock == null) {
            log.trace("No working block to validate: {} on: {}", hash, consortium.getMember());
            return;
        }
        log.debug("Delivering validate: {} on: {}", hash, consortium.getMember());
        Digest memberID = new Digest(v.getId());
        if (consortium.view.getContext().validate(certifiedBlock.getBlock(), v)) {
            certifiedBlock.addCertifications(Certification.newBuilder()
                                                          .setId(v.getId())
                                                          .setSignature(v.getSignature()));
            log.debug("Adding block validation: {} from: {} on: {} count: {}", hash, memberID, consortium.getMember(),
                      certifiedBlock.getCertificationsCount());
        } else {
            log.debug("Failed block validation: {} from: {} on: {}", hash, memberID, consortium.getMember());
        }
    }

    @Override
    public void establishGenesisView() {
        ViewContext current = consortium.view.getContext();
        Parameters params = consortium.params;
        if (current == null || !current.getId().equals(params.genesisViewId)) {
            current = new ViewContext(params.digestAlgorithm, params.genesisViewId, params.context, params.member,
                    this.consortium.view.nextViewConsensusKey(), Collections.emptyList());
            current.activeAll();
            consortium.view.viewChange(current, consortium.scheduler, 0, true);
        }
        if (current.isMember()) {
            regency.currentRegent(-1);
            regency.nextRegent(-2);
            consortium.view.pause();
            consortium.joinMessageGroup(current);
            consortium.transitions.generateView();
            consortium.view.resume();
        }
    }

    @Override
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
            consortium.transitions.synchronizingLeader();
            consortium.transitions.deliverStopData(stopData, consortium.getMember());
        } else {
            LinearClient link = consortium.linkFor(leader);
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

    @Override
    public void generateBlock() {
        generateNextBlock(needCheckpoint());
    }

    @Override
    public void generateView() {
        if (consortium.getCurrent() == null) {
            log.trace("Generating genesis view on: {}", consortium.getMember());
            generateGenesisView();
        } else {
            log.trace("Generating view on: {}", consortium.getMember());
            generateNextView();
        }
    }

    @Override
    public int getCurrentRegent() {
        return regency.currentRegent();
    }

    @Override
    public Member getMember() {
        return consortium.getMember();
    }

    @Override
    public void initializeConsensus() {
        if (currentConsensus() >= 0) {
            return;
        }
        HashedCertifiedBlock current = consortium.getCurrent();
        currentConsensus(current != null ? current.height() : 0);
    }

    @Override
    public boolean isRegent(int regency) {
        Member regent = getRegent(regency);
        return consortium.getMember().equals(regent);
    }

    @Override
    public void join() {
        log.debug("Petitioning to join view: {} on: {}", consortium.view.getContext().getId(),
                  consortium.params.member);

        Join voteForMe = Join.newBuilder()
                             .setMember(consortium.getCurrent() == null ? consortium.view.getContext().getView()
                                     : consortium.view.getNextView())
                             .setContext(consortium.view.getContext().getId().toByteString())
                             .build();

        List<Member> group = consortium.view.getContext().streamRandomRing().collect(Collectors.toList());
        AtomicInteger pending = new AtomicInteger(group.size());
        AtomicInteger success = new AtomicInteger();
        AtomicBoolean completed = new AtomicBoolean();
        List<Result> votes = new CopyOnWriteArrayList<>();
        group.forEach(c -> {
            if (getMember().equals(c)) {
                return;
            }
            LinearClient link = consortium.linkFor(c);
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

    @Override
    public void joinView() {
        joinView(0);
    }

    @Override
    public int nextRegent() {
        return regency.nextRegent();
    }

    @Override
    public void receive(ReplicateTransactions transactions, Member from) {
        transactions.getTransactionsList().forEach(txn -> receive(txn, true));
    }

    @Override
    public void receive(Transaction txn) {
        receive(txn, false);
    }

    @Override
    public boolean receive(Transaction txn, boolean replicated) {
        EnqueuedTransaction transaction = new EnqueuedTransaction(Consortium.hashOf(digestAlgo(), txn), txn);
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
                Digest memberID = new Digest(join.getMember().getId());
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

    @Override
    public void receiveJoin(Transaction txn) {
        if (!txn.getJoin()) {
            return;
        }
        receive(txn);
        reduceJoinTransactions();
    }

    @Override
    public void receiveJoins(ReplicateTransactions transactions, Member from) {
        receive(transactions, from);
        reduceJoinTransactions();
    }

    @Override
    public void recover(HashedCertifiedBlock anchor) {
        if (futureSynchronization != null) {
            futureSynchronization.cancel(true);
            futureSynchronization = null;
        }
        futureBootstrap = bootstrapper(anchor).synchronize().whenComplete((s, t) -> {
            if (t == null) {
                try {
                    synchronize(s);
                } catch (Throwable e) {
                    log.error("Cannot synchronize on: {}", getMember(), e);
                    consortium.transitions.fail();
                }
            } else {
                log.error("Synchronization failed on: {}", getMember(), t);
                consortium.transitions.fail();
            }
        }).exceptionally(t -> {
            log.error("Synchronization failed on: {}", getMember(), t);
            consortium.transitions.fail();
            return null;
        }).orTimeout(consortium.params.synchronizeTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void reschedule(List<EnqueuedTransaction> transactions) {
        transactions.forEach(txn -> {
            txn.setTimedOut(false);
            schedule(txn);
        });
    }

    @Override
    public void resolveRegentStatus() {
        Member regent = getRegent(regency.nextRegent());
        log.debug("Regent: {} on: {}", regent, consortium.getMember());
        if (consortium.getMember().equals(regent)) {
            consortium.transitions.becomeLeader();
        } else {
            consortium.transitions.becomeFollower();
        }
    }

    @Override
    public void scheduleCheckpointBlock() {
        final long currentHeight = lastBlock();
        consortium.performAfter(() -> generateCheckpointBlock(currentHeight), currentHeight);
    }

    @Override
    public void shutdown() {
        consortium.stop();
    }

    @Override
    public void synchronize(int elected, Map<Member, StopData> regencyData) {
        log.trace("Start synchronizing: {} votes: {} on: {}", elected, regencyData.size(), consortium.getMember());
        Sync synch = buildSync(elected, regencyData);
        if (synch != null) {
            log.debug("Synchronizing new regent: {} on: {} voting: {}", elected, consortium.getMember(),
                      regencyData.keySet().stream().map(e -> e.getId()).collect(Collectors.toList()));
            consortium.transitions.synchronizingLeader();
            consortium.view.publish(synch);
            consortium.transitions.deliverSync(synch, consortium.getMember());
        } else {
            log.error("Cannot generate Sync regent: {} on: {} voting: {}", elected, consortium.getMember(),
                      regencyData.keySet().stream().map(e -> e.getId()).collect(Collectors.toList()));
        }
    }

    @Override
    public void totalOrderDeliver() {
        long current = currentConsensus();
        log.trace("Attempting total ordering of working blocks: {} current consensus: {} on: {}", workingBlocks.size(),
                  current, consortium.getMember());
        List<Digest> published = new ArrayList<>();
        workingBlocks.entrySet()
                     .stream()
                     .filter(e -> e.getValue().getCertificationsCount() >= consortium.view.getContext().majority())
                     .sorted((a, b) -> Long.compare(height(a.getValue().getBlock()), height(b.getValue().getBlock())))
                     .filter(e -> height(e.getValue().getBlock()) >= current + 1)
                     .forEach(e -> {
                         long height = height(e.getValue().getBlock());
                         if (height == current + 1) {
                             currentConsensus(height);
                             log.info("Totally ordering block: {} height: {} on: {}", e.getKey(), height,
                                      consortium.getMember());
                             consortium.params.consensus.apply(e.getValue().build(), null);
                             published.add(e.getKey());
                         }
                     });
        published.forEach(h -> {
            Builder removed = workingBlocks.remove(h);
            if (removed != null) {
                Block block = removed.getBlock();
                if (block.getBody().getType() == BodyType.CHECKPOINT) {
                    consortium.transitions.checkpointGenerated();
                }
            }
        });
    }

    void clear() {
        if (futureBootstrap != null) {
            futureBootstrap.cancel(true);
            futureBootstrap = null;
        }
        regency.currentRegent(-1);
        regency.nextRegent(-2);
        currentConsensus(-1);
        timers.values().forEach(e -> e.cancel());
        timers.clear();
        cancelTimers();
        toOrder.clear();
        lastBlock(null);
        consortium.scheduler.cancelAll();
        workingBlocks.clear();
    }

    Map<Digest, EnqueuedTransaction> getToOrder() {
        return toOrder;
    }

    void processCheckpoint(HashedCertifiedBlock next) {
        Checkpoint body = checkpointBody(next.block.getBlock());
        if (body == null) {
            return;
        }
        Digest hash = next.hash;
        CheckpointState checkpointState = checkpoint(body, next.height());
        if (checkpointState == null) {
            log.error("Cannot checkpoint: {} on: {}", hash, getMember());
            consortium.transitions.fail();
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
        cancelTimers();
        cancelSynchronization();
        toOrder.clear();
        consortium.submitted.clear();
        consortium.setGenesis(next);
        consortium.transitions.genesisAccepted();
        reconfigure(next, body.getInitialView(), !synchronizing.get());
        TransactionExecutor exec = consortium.params.executor;
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
        TransactionExecutor exec = consortium.params.executor;
        exec.beginBlock(height, next.hash);
        body.getTransactionsList().forEach(txn -> {
            Digest hash = new Digest(txn.getHash());
            finalized(hash);
            SubmittedTransaction submitted = consortium.submitted.remove(hash);
            exec.execute(next.hash, txn, submitted == null ? null : submitted.onCompletion);
        });
        accept(next);
        log.info("Processed user block: {} height: {} on: {}", next.hash, next.height(), consortium.getMember());
    }

    void reconfigure(HashedCertifiedBlock block, Reconfigure view, boolean genesis) {
        reconfigureView(block, view, genesis, !synchronizing.get());
        resolveStatus();
    }

    void reconfigureView(HashedCertifiedBlock block, Reconfigure view, boolean genesis, boolean resume) {
        this.consortium.view.pause();
        consortium.setLastViewChange(block, view);
        Parameters params = consortium.params;
        ViewContext newView = new ViewContext(params.digestAlgorithm, view, params.context, consortium.getMember(),
                genesis ? this.consortium.view.getContext().getConsensusKey()
                        : this.consortium.view.nextViewConsensusKey());
        int current = genesis ? 2 : 0;
        regency.currentRegent(current);
        regency.nextRegent(current);
        this.consortium.view.viewChange(newView, consortium.scheduler, current, resume);
    }

    void synchronize(Sync syncData, Member regent) {
        HashedCertifiedBlock current = consortium.getCurrent();
        final long currentHeight = current != null ? current.height() : -1;
        workingBlocks.clear();
        syncData.getBlocksList()
                .stream()
                .sorted((a, b) -> Long.compare(height(a), height(b)))
                .filter(cb -> height(cb) > currentHeight)
                .map(cb -> new HashedCertifiedBlock(digestAlgo(), cb))
                .forEach(cb -> {
                    workingBlocks.put(cb.hash, cb.block.toBuilder());
                    lastBlock(new HashedBlock(cb.hash, cb.block.getBlock()));
                    processToOrder(cb.block.getBlock());
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
        store().put(next);
    }

    private Bootstrapper bootstrapper(HashedCertifiedBlock anchor) {
        Parameters params = consortium.params;
        return new Bootstrapper(anchor, params, store(), consortium.bootstrapComm);
    }

    private StopData buildStopData(int currentRegent) {
        Map<Digest, CertifiedBlock> decided;
        decided = workingBlocks.entrySet()
                               .stream()
                               .filter(e -> e.getValue().getCertificationsCount() >= consortium.view.getContext()
                                                                                                    .majority())
                               .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().build()));
        List<Long> gaps = noGaps(decided, store().hashes());
        if (!gaps.isEmpty()) {
            log.debug("Have no valid log on: {} gaps: {}", consortium.getMember(), gaps);
        }
        StopData.Builder builder = StopData.newBuilder().setCurrentRegent(currentRegent);
        builder.setContext(consortium.view.getContext().getId().toByteString());
        decided.entrySet().forEach(e -> {
            builder.addBlocks(e.getValue());
        });
        return builder.build();
    }

    private Sync buildSync(int elected, Map<Member, StopData> regencyData) {
        List<CertifiedBlock> blocks = new ArrayList<>();
        Map<Digest, CertifiedBlock> hashed = new HashMap<>();
        regencyData.values().stream().flatMap(sd -> sd.getBlocksList().stream()).forEach(cb -> {
            hashed.put(digestAlgo().digest(cb.getBlock().toByteString()), cb);
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
                   .setContext(consortium.view.getContext().getId().toByteString())
                   .addAllBlocks(blocks)
                   .build();
    }

    private void cancelTimers() {
        toOrder.values().forEach(eqt -> eqt.cancel());
    }

    private CheckpointState checkpoint(Checkpoint body, long height) {
        Digest stateHash;
        CheckpointState checkpoint = consortium.getChekpoint(height);
        Digest bsh = new Digest(body.getStateHash());
        if (checkpoint != null) {
            if (!body.getStateHash().equals(checkpoint.checkpoint.getStateHash())) {
                log.error("Invalid checkpoint state hash: {} does not equal recorded: {} on: {}",
                          new Digest(checkpoint.checkpoint.getStateHash()), bsh, getMember());
                return null;
            }
        } else {
            File state = consortium.params.checkpointer.apply(height - 1);
            if (state == null) {
                log.error("Invalid checkpoint on: {}", getMember());
                return null;
            }
            try (FileInputStream fis = new FileInputStream(state)) {
                stateHash = digestAlgo().digest(fis);
            } catch (IOException e) {
                log.error("Invalid checkpoint!", e);
                return null;
            }
            if (!stateHash.equals(bsh)) {
                log.error("Cannot replicate checkpoint: {} state hash: {} does not equal recorded: {} on: {}", height,
                          stateHash, bsh, getMember());
                state.delete();
                return null;
            }
            MVMap<Integer, byte[]> stored = store().putCheckpoint(height, state, body);
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
        Member regent = consortium.view.getContext().getRegent(regency.currentRegent());
        if (!regent.equals(from)) {
            log.debug("Ignoring checkpoint block from non regent: {} actual: {} on: {}", from, regent, getMember());
            return;
        }
        Digest hash = digestAlgo().digest(block.toByteString());
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
            consortium.performAfter(() -> validateCheckpoint(hash, block, from), height(block) - 1);
        }
    }

    private void deliverGenesisBlock(final Block block, Member from) {
        Digest hash = digestAlgo().digest(block.toByteString());
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
            cancelTimers();
            toOrder.clear();
            Validate validation = consortium.view.getContext().generateValidation(hash, block);
            if (validation == null) {
                log.error("Cannot validate generated genesis: {} on: {}", hash, consortium.getMember());
                return;
            }
            lastBlock(new HashedBlock(digestAlgo(), block));
            consortium.view.setContext(consortium.view.getContext().cloneWith(genesis.getInitialView().getViewList()));
            consortium.view.publish(validation);
            workingBlocks.put(hash,
                              CertifiedBlock.newBuilder()
                                            .setBlock(block)
                                            .addCertifications(Certification.newBuilder()
                                                                            .setId(validation.getId())
                                                                            .setSignature(validation.getSignature())));
        }
    }

    private DigestAlgorithm digestAlgo() {
        return consortium.params.digestAlgorithm;
    }

    private void finalized(Digest hash) {
        EnqueuedTransaction removed = toOrder.remove(hash);
        if (removed != null) {
            removed.cancel();
            processed.add(removed.hash);
        }
    }

    private void firstTimeout(EnqueuedTransaction transaction) {
        assert !transaction.isTimedOut() : "this should be unpossible";
        transaction.setTimedOut(true);

        Parameters params = consortium.params;
        long span = transaction.transaction.getJoin() ? params.joinTimeout.toMillis() : params.submitTimeout.toMillis();

        chunked(toOrder.values().stream().filter(eqt -> eqt.getDelay() <= span).peek(eqt -> {
            eqt.cancel();
            eqt.setTimedOut(true);
            eqt.setTimer(consortium.scheduler.schedule(Timers.TRANSACTION_TIMEOUT_2, () -> secondTimeout(eqt),
                                                       eqt.transaction.getJoin() ? params.joinTimeout
                                                               : params.submitTimeout));
        }).map(eqt -> eqt.transaction), 100).forEach(txns -> {
            log.debug("Replicating from: {} count: {} first timeout on: {}", transaction.hash, txns.size(),
                      consortium.getMember());
            consortium.view.publish(ReplicateTransactions.newBuilder().addAllTransactions(txns).build());
        });
    }

    private void generateCheckpointBlock(long currentHeight) {
        final long thisHeight = lastBlock() + 1;

        log.info("Generating checkpoint block on: {} height: {} ", consortium.getMember(), currentHeight);
        consortium.view.publish(CheckpointProcessing.newBuilder().setCheckpoint(currentHeight).build());

        schedule(Timers.CHECKPOINTING, () -> {
            consortium.view.publish(CheckpointProcessing.newBuilder().setCheckpoint(currentHeight).build());
        }, consortium.params.submitTimeout.dividedBy(2));

        File state = consortium.params.checkpointer.apply(currentHeight);
        if (state == null) {
            log.error("Cannot create checkpoint");
            consortium.transitions.fail();
            return;
        }
        Checkpoint checkpoint = checkpoint(digestAlgo(), state, consortium.params.checkpointBlockSize);
        if (checkpoint == null) {
            consortium.transitions.fail();
        }

        HashedBlock lb = lastBlock.get();
        Digest previous = lb == null ? null : lb.hash;
        if (previous == null) {
            log.error("Cannot generate checkpoint block on: {} height: {} no previous block: {}",
                      consortium.getMember(), currentHeight, lastBlock());
            consortium.transitions.fail();
            return;
        }
        Block block = generateBlock(digestAlgo(), consortium.getLastCheckpointBlock(), thisHeight, previous,
                                    body(BodyType.CHECKPOINT, checkpoint), consortium.getLastViewChangeBlock());

        Digest hash = digestAlgo().digest(block.toByteString());

        CertifiedBlock.Builder builder = workingBlocks.computeIfAbsent(hash, k -> CertifiedBlock.newBuilder()
                                                                                                .setBlock(block));

        Validate validation = consortium.view.getContext().generateValidation(hash, block);
        if (validation == null) {
            log.debug("Cannot generate validation for block: {} hash: {} segements: {} on: {}", hash,
                      new Digest(checkpoint.getStateHash()), checkpoint.getSegmentsCount(), consortium.getMember());
            consortium.transitions.fail();
            cancel(Timers.CHECKPOINTING);
            return;
        }
        builder.addCertifications(Certification.newBuilder()
                                               .setId(validation.getId())
                                               .setSignature(validation.getSignature()));
        lastBlock(new HashedBlock(hash, block));
        consortium.view.publish(block);
        consortium.view.publish(validation);
        cancel(Timers.CHECKPOINTING);

        log.info("Generated next checkpoint block: {} height: {} on: {} ", hash, thisHeight, consortium.getMember());
    }

    private void generateGenesisBlock() {
        reduceJoinTransactions();
        assert toOrder.size() >= consortium.view.getContext().majority() : "Whoops";
        log.debug("Generating genesis on {} join transactions: {}", consortium.getMember(), toOrder.size());
        byte[] nextView = new byte[32];
        Utils.secureEntropy().nextBytes(nextView);
        Parameters params = consortium.params;
        Reconfigure.Builder genesisView = Reconfigure.newBuilder()
                                                     .setCheckpointBlocks(params.deltaCheckpointBlocks)
                                                     .setId(new Digest(digestAlgo(), nextView).toByteString())
                                                     .setTolerance(consortium.view.getContext().majority());
        toOrder.values().forEach(join -> {
            JoinTransaction txn;
            try {
                txn = join.transaction.getTxn().unpack(JoinTransaction.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("Cannot generate genesis, unable to parse Join txnL {} on: {}", join.hash,
                          consortium.getMember());
                consortium.transitions.fail();
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
                b.append(new Digest(vm.getId()));
                b.append(" key: ");
                b.append(digestAlgo().digest(vm.getConsensusKey()));
                b.append("\n");
            }
            log.trace("genesis view {}", b.toString());
        }
        toOrder.values().forEach(e -> e.cancel());
        toOrder.clear();
        Block block = generateBlock(params.digestAlgorithm, consortium.getLastCheckpointBlock(), (long) 0,
                                    params.genesisViewId,
                                    body(BodyType.GENESIS,
                                         Genesis.newBuilder()
                                                .setGenesisData(Any.pack(params.genesisData))
                                                .setInitialView(genesisView)
                                                .build()),
                                    consortium.getLastViewChangeBlock());
        Digest hash = digestAlgo().digest(block.toByteString());
        log.info("Genesis block: {} generated on {}", hash, consortium.getMember());
        Builder cb = workingBlocks.get(hash);
        if (cb == null) {
            lastBlock(new HashedBlock(hash, block));
            Validate validation = consortium.view.getContext().generateValidation(hash, block);
            if (validation == null) {
                log.error("Cannot validate generated genesis block: {} on: {}", hash, consortium.getMember());
            }
            CertifiedBlock.Builder builder = CertifiedBlock.newBuilder().setBlock(block);
            builder.addCertifications(Certification.newBuilder()
                                                   .setId(validation.getId())
                                                   .setSignature(validation.getSignature()));
            workingBlocks.put(hash, builder);
            consortium.view.setContext(consortium.view.getContext().cloneWith(genesisView.getViewList()));
            consortium.view.publish(block);
            consortium.view.publish(validation);
        }
    }

    private void generateGenesisView() {
        if (!selfJoinRecorded()) {
            log.trace("Join transaction not found on: {}, rescheduling", consortium.getMember());
            rescheduleGenesis();
            return;
        }
        int txns = toOrder.size();
        if (txns >= consortium.view.getContext().activeCardinality()) {
            generateGenesisBlock();
        } else {
            log.trace("Genesis group has not formed, rescheduling, have: {} want: {} on: {}", txns,
                      consortium.view.getContext().activeCardinality(), consortium.getMember());
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
        Digest curr = lb == null ? null : lb.hash;
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
        List<Digest> processed = new ArrayList<>();

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

        Block block = generateBlock(digestAlgo(), consortium.getLastCheckpointBlock(), thisHeight, curr,
                                    body(BodyType.USER, user.build()), consortium.getLastViewChangeBlock());
        Digest hash = digestAlgo().digest(block.toByteString());

        CertifiedBlock.Builder builder = workingBlocks.computeIfAbsent(hash, k -> CertifiedBlock.newBuilder()
                                                                                                .setBlock(block));

        Validate validation = consortium.view.getContext().generateValidation(hash, block);
        if (validation == null) {
            log.debug("Cannot generate validation for block: {} on: {}", hash, consortium.getMember());
            return false;
        }
        builder.addCertifications(Certification.newBuilder()
                                               .setId(validation.getId())
                                               .setSignature(validation.getSignature()));
        lastBlock(new HashedBlock(hash, block));
        consortium.view.publish(block);
        consortium.view.publish(validation);

        log.info("Generated next block: {} height: {} on: {} txns: {}", hash, thisHeight, consortium.getMember(),
                 user.getTransactionsCount());
        return true;
    }

    private void generateNextBlock(boolean needCheckpoint) {
        if (needCheckpoint) {
            consortium.transitions.generateCheckpoint();
        } else {
            generateNextBlock();
            if (needCheckpoint()) {
                consortium.transitions.generateCheckpoint();
            } else {
                scheduleFlush();
            }
        }
    }

    private void generateNextView() {
        // TODO Auto-generated method stub

    }

    private Member getRegent(int regent) {
        return consortium.view.getContext().getRegent(regent);
    }

    private void joinView(int attempt) {
        boolean selfJoin = selfJoinRecorded();
        int majority = consortium.view.getContext().majority();
        if (attempt < 20) {
            if (selfJoin && toOrder.size() == consortium.view.getContext().activeCardinality()) {
                log.trace("View formed, attempt: {} on: {} have: {} require: {} self join: {}", attempt,
                          consortium.getMember(), toOrder.size(), majority, selfJoin);
                consortium.transitions.formView();
                return;
            }
        }

        if (attempt >= 20) {
            if (selfJoin && toOrder.size() >= majority) {
                log.trace("View formed, attempt: {} on: {} have: {} require: {} self join: {}", attempt,
                          consortium.getMember(), toOrder.size(), majority, selfJoin);
                consortium.transitions.formView();
                return;
            }
        }

        log.trace("View has not been formed, attempt: {} rescheduling on: {} have: {} require: {} self join: {}",
                  attempt, consortium.getMember(), toOrder.size(), majority, selfJoin);
        join();
        schedule(Timers.AWAIT_VIEW_MEMBERS, () -> joinView(attempt + 1), consortium.params.viewTimeout);
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
                                                 .limit(consortium.params.maxBatchSize)
                                                 .filter(eqt -> eqt != null)
                                                 .filter(eqt -> processedBytes.addAndGet(eqt.getSerializedSize()) <= consortium.params.maxBatchByteSize)
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
        int majority = consortium.view.getContext().majority();
        if (succeeded >= majority) {
            if (completed.compareAndSet(false, true)) {
                JoinTransaction.Builder txn = JoinTransaction.newBuilder().setMember(voteForMe.getMember());
                for (Result vote : votes) {
                    txn.addCertification(Certification.newBuilder()
                                                      .setId(vote.member.getId().toByteString())
                                                      .setSignature(vote.vote.getSignature()));
                }
                JoinTransaction joinTxn = txn.build();

                Digest txnHash = consortium.submit(null, true, null, joinTxn);
                log.debug("Successfully petitioned: {} to join view: {} on: {}", txnHash,
                          consortium.view.getContext().getId(), consortium.params.member);
            }
        } else {
            if (remaining + succeeded < majority) {
                if (completed.compareAndSet(false, true)) {
                    if (votes.size() < consortium.view.getContext().majority()) {
                        log.debug("Did not gather votes necessary to join consortium needed: {} got: {} on: {}",
                                  consortium.view.getContext().majority(), votes.size(), consortium.getMember());
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
            Digest hash = new Digest(et.getHash());
            EnqueuedTransaction p = toOrder.remove(hash);
            if (p != null) {
                p.cancel();
                processed.add(hash);
            }
        });
    }

    private void reduceJoinTransactions() {
        Map<Digest, EnqueuedTransaction> reduced = new HashMap<>(); // Member ID -> join txn
        toOrder.forEach((h, eqt) -> {
            try {
                JoinTransaction join = eqt.transaction.getTxn().unpack(JoinTransaction.class);
                EnqueuedTransaction prev = reduced.put(new Digest(join.getMember().getId()), eqt);
                if (prev != null) {
                    prev.cancel();
                }
            } catch (InvalidProtocolBufferException e) {
                log.error("Failure deserializing ToOrder txn Join on {}", consortium.getMember(), e);
            }
        });
        log.trace("Reduced joins on: {} current: {} prev: {}", getMember(), reduced.size(), toOrder.size());
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
            if (selfJoin && toOrder.size() >= consortium.view.getContext().majority()) {
                generateGenesisBlock();
            } else {
                log.trace("Genesis group has not formed, rescheduling, have: {} want: {} self join: {} on: {}",
                          toOrder.size(), consortium.view.getContext().majority(), selfJoin, consortium.getMember());
                rescheduleGenesis();
            }
        }, consortium.params.viewTimeout);
    }

    private void resolveStatus() {
        Member regent = getRegent(regency.currentRegent());
        if (consortium.view.getContext().isViewMember()) {
            if (consortium.getMember().equals(regent)) {
                log.debug("becoming leader on: {}", consortium.getMember());
                consortium.transitions.becomeLeader();
            } else {
                log.debug("becoming follower on: {} regent: {}", consortium.getMember(), regent);
                consortium.transitions.becomeFollower();
            }
        } else if (consortium.view.getContext().isMember()) {
            log.debug("becoming joining member on: {} regent: {}", consortium.getMember(), regent);
            consortium.transitions.joinAsMember();
        } else {
            log.debug("becoming client on: {} regent: {}", consortium.getMember(), regent);
            consortium.transitions.becomeClient();
        }
    }

    private void restoreFrom(HashedCertifiedBlock block, CheckpointState checkpoint) {
        consortium.checkpoint(block.height(), checkpoint);
        consortium.params.restorer.accept(block.height(), checkpoint);
        consortium.restore();
        processCheckpoint(block);
    }

    private Timer schedule(EnqueuedTransaction eqt) {
        eqt.cancel();
        Parameters params = consortium.params;
        Duration delta = eqt.transaction.getJoin() ? params.joinTimeout : params.submitTimeout;
        Timer timer = consortium.scheduler.schedule(Timers.TRANSACTION_TIMEOUT_1, () -> {
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
            return consortium.scheduler.schedule(k, action, delta);
        });
    }

    private void scheduleFlush() {
        schedule(Timers.FLUSH_BATCH, () -> generateBlock(), consortium.params.maxBatchDelay);
    }

    private void secondTimeout(EnqueuedTransaction transaction) {
        log.debug("Second timeout for: {} on: {}", transaction.hash, consortium.getMember());
        Parameters params = consortium.params;
        long span = transaction.transaction.getJoin() ? params.joinTimeout.toMillis() : params.submitTimeout.toMillis();
        List<EnqueuedTransaction> timedOut = toOrder.values()
                                                    .stream()
                                                    .filter(eqt -> eqt.getDelay() <= span)
                                                    .limit(99)
                                                    .peek(eqt -> eqt.cancel())
                                                    .collect(Collectors.toList());
        consortium.transitions.startRegencyChange(timedOut);
    }

    private boolean selfJoinRecorded() {
        Digest id = consortium.getMember().getId();
        return toOrder.values().stream().map(eqt -> eqt.transaction).map(t -> {
            try {
                return t.getTxn().unpack(JoinTransaction.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("Cannot generate genesis, unable to parse Join txn on: {}", consortium.getMember());
                return null;
            }
        }).filter(jt -> jt != null).anyMatch(jt -> id.equals(new Digest(jt.getMember().getId())));
    }

    private Store store() {
        return consortium.store;
    }

    private void synchronize(SynchronizedState state) {
        synchronizing.set(true);
        consortium.transitions.synchronizing();
        CertifiedBlock current1;
        if (state.lastCheckpoint == null) {
            log.info("Synchronizing from genesis: {} on: {}", state.genesis.hash, getMember());
            current1 = state.genesis.block;
        } else {
            log.info("Synchronizing from checkpoint: {} on: {}", state.lastCheckpoint.hash, getMember());
            restoreFrom(state.lastCheckpoint, state.checkpoint);
            current1 = consortium.store.getCertifiedBlock(state.lastCheckpoint.height() + 1);
        }
        while (current1 != null) {
            consortium.synchronizedProcess(current1, false);
            current1 = consortium.store.getCertifiedBlock(height(current1.getBlock()) + 1);
        }
        synchronizing.set(false);
        log.info("Synchronized, resuming view on: {}",
                 state.lastCheckpoint != null ? state.lastCheckpoint.hash : state.genesis.hash, getMember());
        this.consortium.view.resume();
        resolveStatus();
        log.info("Processing deferred blocks: {} on: {}", consortium.deferredCount(), consortium.deferredCount(),
                 getMember());
        consortium.processDeferred();
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

    private void validateCheckpoint(Digest hash, Block block, Member from) {
        Checkpoint checkpoint = checkpointBody(block);

        HashedCertifiedBlock current = consortium.getCurrent();
        if (current.height() >= height(block) - 1) {
            if (checkpoint(checkpoint, height(block)) == null) {
                log.error("Unable to generate checkpoint: {} on {}", height(block), getMember());
                consortium.transitions.fail();
                return;
            }
            Validate validation = consortium.view.getContext().generateValidation(hash, block);
            if (validation == null) {
                log.debug("Rejecting checkpoint block proposal: {}, cannot validate from: {} on: {}", hash, from,
                          consortium.getMember());
                workingBlocks.remove(hash);
                return;
            }
            log.debug("Validating checkpoint block: {} from: {} on: {}", hash, from, getMember());
            consortium.view.publish(validation);
            deliverValidate(validation);
            consortium.transitions.checkpointGenerated();
        } else {
            log.debug("Rescheduing checkpoint block: {} from: {} on: {}", hash, from, getMember());
            schedule(Timers.CHECKPOINTING, () -> validateCheckpoint(hash, block, from),
                     consortium.params.submitTimeout.dividedBy(4));
        }
    }
}
