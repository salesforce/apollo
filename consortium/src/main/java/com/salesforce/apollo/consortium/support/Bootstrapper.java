/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.BlockReplication;
import com.salesfoce.apollo.consortium.proto.Blocks;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Initial;
import com.salesfoce.apollo.consortium.proto.Synchronize;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.CollaboratorContext;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.Store;
import com.salesforce.apollo.consortium.comms.BootstrapClient;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.protocols.CountdownAction;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Pair;
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public class Bootstrapper {
    public static class GenesisNotResolved extends Exception {
        private static final long serialVersionUID = 1L;

    }

    private static final Logger log = LoggerFactory.getLogger(Bootstrapper.class);

    private static HashKey randomCut() {
        long[] cut = new long[HashKey.LONG_SIZE];
        for (int i = 0; i < HashKey.LONG_SIZE; i++) {
            cut[i] = Utils.secureEntropy().nextLong();
        }
        return new HashKey(cut);
    }

    private final Block                                                               anchor;
    private CompletableFuture<CheckpointState>                                        assembledFuture;
    private HashedCertifiedBlock                                                      checkpoint;
    private HashedCertifiedBlock                                                      checkpointView;
    private final CommonCommunications<BootstrapClient, Service>                      comms;
    private final Context<Member>                                                     context;
    private final Duration                                                            duration;
    private final double                                                              fpr;
    private volatile HashedCertifiedBlock                                             genesis;
    private final int                                                                 maxBlocks;
    private final int                                                                 maxViewBlocks;
    private final Member                                                              member;
    private final ScheduledExecutorService                                            scheduler;
    private final int                                                                 slice;
    private final Store                                                               store;
    private final CompletableFuture<Pair<HashedCertifiedBlock, HashedCertifiedBlock>> sync                  = new CompletableFuture<>();
    private final CompletableFuture<Boolean>                                          viewChainSynchronized = new CompletableFuture<>();

    public Bootstrapper(Block anchor, Member member, Context<Member> context,
            CommonCommunications<BootstrapClient, Service> comms, double falsePositiveRate, Store store, int slice,
            ScheduledExecutorService scheduler, int maxViewBlocks, Duration duration, int maxBlocks) {
        this.comms = comms;
        this.context = context;
        this.fpr = falsePositiveRate;
        this.member = member;
        this.store = store;
        this.slice = slice;
        this.scheduler = scheduler;
        this.duration = duration;
        this.anchor = anchor;
        this.maxBlocks = maxBlocks;
        this.maxViewBlocks = maxViewBlocks;
        viewChainSynchronized.whenComplete((r, t) -> {
            log.info("Viewchain complete on: {}", checkpoint.hash, member);
            if (assembledFuture.isDone()) {
                sync.complete(new Pair<>(genesis, checkpoint));
                return;
            }
        });
    }

    public CompletableFuture<Pair<HashedCertifiedBlock, HashedCertifiedBlock>> synchronize() {
        scheduleSample();
        return sync;
    }

    private void assemblyComplete() {
        log.info("Assembled checkpoint: {} on: {}", checkpoint.hash, member);
        if (viewChainSynchronized.isDone()) {
            sync.complete(new Pair<>(genesis, checkpoint));
            return;
        }
    }

    private void completeViewchain(long from, long target) {
        if (store.completeFrom(from)) {
            viewChainSynchronized.complete(true);
        } else {
            completeViewChain(from, target);
        }
    }

    private void completeViewChain(Iterator<Member> graphCut, long from, long to) {
        if (sync.isDone()) {
            return;
        }
        while (graphCut.hasNext()) {
            Member m = graphCut.next();
            BootstrapClient link = comms.apply(member, m);
            if (link == null) {
                log.info("No line for view chain completion: {} on: {}", m.getId(), member.getId());
                continue;
            }

            int seed = Utils.bitStreamEntropy().nextInt();
            BloomFilter<Long> blocksBff = new BloomFilter.LongBloomFilter(seed, maxViewBlocks, fpr);
            from = store.lastViewChainFrom(from);
            store.viewChainFrom(from, to).forEachRemaining(h -> blocksBff.add(h));
            BlockReplication replication = BlockReplication.newBuilder()
                                                           .setContext(context.getId().toByteString())
                                                           .setBlocksBff(blocksBff.toBff().toByteString())
                                                           .setFrom(from)
                                                           .setTo(to)
                                                           .build();

            log.info("Attempting view chain completion ({} to {}) with: {} on: {}", from, to, m.getId(),
                     member.getId());
            try {
                ListenableFuture<Blocks> future = link.fetchViewChain(replication);
                future.addListener(completeViewChain(m, graphCut, future, from, to), ForkJoinPool.commonPool());
            } finally {
                link.release();
            }
            return;
        }
    }

    private void completeViewChain(long from, long to) {
        List<Member> sample = context.successors(randomCut());
        CountdownAction countdown = new CountdownAction(() -> {
            completeViewchain(from, to);
        }, sample.size());
        completeViewChain(sample.iterator(), from, to);
    }

    private Runnable completeViewChain(Member m, Iterator<Member> graphCut, ListenableFuture<Blocks> future, long from,
                                       long to) {
        return () -> {
            if (sync.isDone()) {
                return;
            }
            try {
                Blocks blocks = future.get();
                log.info("View chain completion ({} to {}) from: {} on: {}", from, to, m.getId(), member.getId());
                blocks.getBlocksList()
                      .stream()
                      .map(cb -> new HashedCertifiedBlock(cb))
                      .forEach(cb -> store.put(cb.hash, cb.block));
            } catch (InterruptedException e) {
                log.debug("Error counting vote from: {} on: {}", m.getId(), member.getId());
            } catch (ExecutionException e) {
                log.debug("Error counting vote from: {} on: {}", m.getId(), member.getId());
            }
            countdown(graphCut, from, to);
        };
    }

    private void computeGenesis(Map<HashKey, Initial> votes) {

        log.info("Computing genesis with {} votes, required: {} on: {}", votes.size(), context.toleranceLevel() + 1,
                 member);
        Multiset<HashedCertifiedBlock> tally = HashMultiset.create();
        Map<HashKey, Initial> valid = votes.entrySet().stream().filter(e -> e.getValue().hasGenesis()).filter(e -> {
            if (!e.getValue().hasCheckpoint()) {
                return true;
            }
            if (!e.getValue().hasCheckpointView()) {
                return false;
            }
            long checkpointViewHeight = CollaboratorContext.height(e.getValue().getCheckpointView().getBlock());
            long recordedCheckpointViewHeight = e.getValue().getCheckpoint().getBlock().getHeader().getLastReconfig();
            log.info("checkpoint view height: {} recorded: {} on: {}", checkpointViewHeight,
                     recordedCheckpointViewHeight, member);
            return checkpointViewHeight == recordedCheckpointViewHeight;
        })
                                           .peek(e -> tally.add(new HashedCertifiedBlock(e.getValue().getGenesis())))
                                           .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        Pair<HashedCertifiedBlock, Integer> winner = null;
        int threshold = context.toleranceLevel();

        log.info("Tally: {} required: {} on: {}", tally, context.toleranceLevel() + 1, member);
        for (HashedCertifiedBlock cb : tally) {
            int count = tally.count(cb);
            if (count > threshold) {
                if (winner == null || count > winner.b) {
                    winner = new Pair<HashedCertifiedBlock, Integer>(cb, count);
                }
            }
        }

        if (winner == null) {
            log.info("No winner on: {}", member);
            scheduleSample();
            return;
        }

        genesis = winner.a;
        log.info("Winner: {} on: {}", genesis.hash, member);
        CertifiedBlock.getDefaultInstance();

        // get the most recent checkpoint.
        Initial mostRecent = valid.values()
                                  .stream()
                                  .filter(i -> i.hasGenesis())
                                  .filter(i -> genesis.hash.equals(new HashedCertifiedBlock(i.getGenesis()).hash))
                                  .filter(i -> i.hasCheckpoint())
                                  .filter(i -> i.getCheckpoint().getBlock().getBody().getType() == BodyType.CHECKPOINT)
                                  .max((a, b) -> Long.compare(a.getCheckpoint().getBlock().getHeader().getHeight(),
                                                              b.getCheckpoint().getBlock().getHeader().getHeight()))
                                  .orElse(null);
        store.put(genesis.hash, genesis.block);

        if (mostRecent == null) {
            // Nothing but Genesis
            sync.complete(new Pair<>(winner.a, null));
            return;
        }

        checkpoint = new HashedCertifiedBlock(mostRecent.getCheckpoint());
        store.put(checkpoint.hash, checkpoint.block);

        checkpointView = new HashedCertifiedBlock(mostRecent.getCheckpointView());
        store.put(checkpointView.hash, checkpointView.block);
        log.info("Checkpoint: {} on: {}", checkpoint.hash, member);

        CheckpointAssembler assembler = new CheckpointAssembler(
                CollaboratorContext.checkpointBody(checkpoint.block.getBlock()), member, store, comms, context,
                threshold);

        // assemble the checkpoint
        assembledFuture = assembler.assemble(scheduler, duration);
        assembledFuture.whenComplete((c, t) -> {
            if (t != null) {
                log.error("Unable to assemble checkpoint: {} on: {}", checkpoint.hash, member);
                sync.completeExceptionally(t);
            } else {
                assemblyComplete();
            }
        });

        // reconstruct chain to genesis
        mostRecent.getViewChainList()
                  .stream()
                  .filter(cb -> cb.getBlock().getBody().getType() == BodyType.RECONFIGURE)
                  .map(cb -> new HashedCertifiedBlock(cb))
                  .forEach(reconfigure -> {
                      store.put(reconfigure.hash, reconfigure.block);
                  }); 
        scheduleCompletion(checkpointView.height(), 0);
    }

    private void countdown(Iterator<Member> graphCut, long from, long target) {
        if (!graphCut.hasNext()) {
            completeViewchain(from, target);
        } else {
            completeViewChain(graphCut, from, target);
        }
    }

    private void initialize(List<Member> graphCut, Map<HashKey, Initial> votes, CountdownAction countdown) {
        if (sync.isDone()) {
            return;
        }
        Member m = graphCut.get(0);
        graphCut = graphCut.subList(1, graphCut.size());

        BootstrapClient link = comms.apply(member, m);
        if (link == null) {
            log.info("No link for {} on: {}", m, member);
            countdown.countdown();
            return;
        }
        Synchronize s = Synchronize.newBuilder()
                                   .setContext(context.getId().toByteString())
                                   .setHeight(anchor.getHeader().getHeight())
                                   .build();
        log.info("Attempting synchronization with: {} on: {}", m, member);
        try {
            ListenableFuture<Initial> future = link.sync(s);
            future.addListener(initialize(m, graphCut, future, votes, countdown), ForkJoinPool.commonPool());
        } finally {
            link.release();
        }
    }

    private Runnable initialize(Member m, List<Member> graphCut, ListenableFuture<Initial> future,
                                Map<HashKey, Initial> votes, CountdownAction countdown) {
        return () -> {
            if (sync.isDone()) {
                return;
            }

            try {
                votes.put(m.getId(), future.get());
                log.info("Synchronization vote from: {} recorded on: {}", m, member);
            } catch (InterruptedException e) {
                log.debug("Error counting vote from: {} on: {}", m.getId(), member.getId());
            } catch (ExecutionException e) {
                log.debug("Error counting vote from: {} on: {}", m.getId(), member.getId());
            }
            if (!countdown.countdown()) {
                initialize(graphCut, votes, countdown);
            }
        };
    }

    private void sample() {
        List<Member> sample = context.successors(randomCut());
        HashMap<HashKey, Initial> votes = new HashMap<>();
        CountdownAction countdown = new CountdownAction(() -> computeGenesis(votes), sample.size());
        initialize(sample, votes, countdown);
    }

    private void scheduleCompletion(long from, long to) {
        if (sync.isDone()) {
            return;
        }
        log.info("Scheduling view chain completion ({} to {}) duration: {} millis on: {}", from, to,
                 duration.toMillis(), member);
        scheduler.schedule(() -> {
            try {
                completeViewChain(from, to);
            } catch (Throwable e) {
                log.error("Cannot execute completeViewChain on: {}", member);
                sync.completeExceptionally(e);
            }
        }, duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void scheduleSample() {
        if (sync.isDone()) {
            return;
        }
        scheduler.schedule(() -> {
            try {
                sample();
            } catch (Throwable e) {
                log.error("Unable to sample sync state on: {}", member, e);
                sync.completeExceptionally(e);
            }
        }, duration.toMillis(), TimeUnit.MILLISECONDS);
    }
}
