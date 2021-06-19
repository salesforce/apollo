/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.BlockReplication;
import com.salesfoce.apollo.consortium.proto.Blocks;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Initial;
import com.salesfoce.apollo.consortium.proto.Synchronize;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.CollaboratorContext;
import com.salesforce.apollo.consortium.Consortium.BootstrappingService;
import com.salesforce.apollo.consortium.Parameters;
import com.salesforce.apollo.consortium.Store;
import com.salesforce.apollo.consortium.comms.BootstrapClient;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.CountdownAction;
import com.salesforce.apollo.utils.BloomFilter;
import com.salesforce.apollo.utils.Pair;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class Bootstrapper {
    public static class GenesisNotResolved extends Exception {
        private static final long serialVersionUID = 1L;

    }

    public static class SynchronizedState {
        public final CheckpointState      checkpoint;
        public final HashedCertifiedBlock genesis;
        public final HashedCertifiedBlock lastCheckpoint;
        public final HashedCertifiedBlock lastView;

        public SynchronizedState(HashedCertifiedBlock genesis, HashedCertifiedBlock lastView,
                HashedCertifiedBlock lastCheckpoint, CheckpointState checkpoint) {
            this.genesis = genesis;
            this.lastView = lastView;
            this.lastCheckpoint = lastCheckpoint;
            this.checkpoint = checkpoint;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Bootstrapper.class);

    private static Digest randomCut(DigestAlgorithm algo) {
        long[] cut = new long[algo.longLength()];
        for (int i = 0; i < cut.length; i++) {
            cut[i] = Utils.secureEntropy().nextLong();
        }
        return new Digest(algo, cut);
    }

    private final HashedCertifiedBlock                                        anchor;
    private final CompletableFuture<Boolean>                                  anchorSynchronized    = new CompletableFuture<>();
    private HashedCertifiedBlock                                              checkpoint;
    private CompletableFuture<CheckpointState>                                checkpointAssembled;
    private CheckpointState                                                   checkpointState;
    private HashedCertifiedBlock                                              checkpointView;
    private final CommonCommunications<BootstrapClient, BootstrappingService> comms;
    private volatile HashedCertifiedBlock                                     genesis;
    private final long                                                        lastCheckpoint;
    private final Parameters                                                  params;
    private final Store                                                       store;
    private final CompletableFuture<SynchronizedState>                        sync                  = new CompletableFuture<>();
    private final CompletableFuture<Boolean>                                  viewChainSynchronized = new CompletableFuture<>();

    public Bootstrapper(HashedCertifiedBlock anchor, Parameters params, Store store,
            CommonCommunications<BootstrapClient, BootstrappingService> bootstrapComm) {
        this.anchor = anchor;
        this.params = params;
        this.store = store;
        this.comms = bootstrapComm;
        CertifiedBlock g = store.getCertifiedBlock(0);
        store.put(anchor);
        if (g != null) {
            genesis = new HashedCertifiedBlock(params.digestAlgorithm, g);
            log.info("Restore using genesis: {} on: {}", genesis.hash, params.member);
            lastCheckpoint = store.getLastBlock().block.getBlock().getHeader().getLastCheckpoint();
        } else {
            log.info("Restore using no prior state on: {}", params.member);
            lastCheckpoint = -1;
        }
    }

    public CompletableFuture<SynchronizedState> synchronize() {
        scheduleSample();
        return sync;
    }

    private void checkpointCompletion(int threshold, Initial mostRecent) {
        checkpoint = new HashedCertifiedBlock(params.digestAlgorithm, mostRecent.getCheckpoint());
        store.put(checkpoint);

        checkpointView = new HashedCertifiedBlock(params.digestAlgorithm, mostRecent.getCheckpointView());
        store.put(checkpointView);
        log.info("Checkpoint: {}:{} on: {}", checkpoint.height(), checkpoint.hash, params.member);

        CheckpointAssembler assembler = new CheckpointAssembler(checkpoint.height(),
                CollaboratorContext.checkpointBody(checkpoint.block.getBlock()), params.member, store, comms,
                params.context, threshold, params.digestAlgorithm, params.dispatcher);

        // assemble the checkpoint
        checkpointAssembled = assembler.assemble(params.scheduler, params.synchronizeDuration)
                                       .whenComplete((cps, t) -> {
                                           log.info("Restored checkpoint: {} on: {}", checkpoint.height(),
                                                    params.member);
                                           checkpointState = cps;
                                       });
        // reconstruct chain to genesis
        mostRecent.getViewChainList()
                  .stream()
                  .filter(cb -> cb.getBlock().getBody().getType() == BodyType.RECONFIGURE)
                  .map(cb -> new HashedCertifiedBlock(params.digestAlgorithm, cb))
                  .forEach(reconfigure -> {
                      store.put(reconfigure);
                  });
        scheduleCompletion(checkpointView.height(), 0);
    }

    private void completeAnchor(Iterator<Member> graphCut, long from, long to) {
        if (sync.isDone() || anchorSynchronized.isDone()) {
            return;
        }
        if (!graphCut.hasNext()) {
            scheduleAnchorCompletion(store.firstGap(from, to), to);
            return;
        }
        while (graphCut.hasNext()) {
            Member m = graphCut.next();
            BootstrapClient link = comms.apply(m, params.member);
            if (link == null) {
                log.debug("No link for anchor completion: {} on: {}", m.getId(), params.member.getId());
                continue;
            }

            int seed = Utils.bitStreamEntropy().nextInt();
            BloomFilter<Long> blocksBff = new BloomFilter.LongBloomFilter(seed, params.maxViewBlocks,
                    params.msgParameters.falsePositiveRate);
            from = store.firstGap(from, to);
            store.blocksFrom(from, to, params.maxSyncBlocks).forEachRemaining(h -> blocksBff.add(h));
            BlockReplication replication = BlockReplication.newBuilder()
                                                           .setContext(params.context.getId().toByteString())
                                                           .setBlocksBff(blocksBff.toBff().toByteString())
                                                           .setFrom(from)
                                                           .setTo(to)
                                                           .build();

            log.debug("Attempting Anchor completion ({} to {}) with: {} on: {}", from, to, m.getId(),
                      params.member.getId());
            try {
                ListenableFuture<Blocks> future = link.fetchBlocks(replication);
                future.addListener(completeAnchor(m, graphCut, future, from, to), params.dispatcher);
            } finally {
                link.release();
            }
            return;
        }
    }

    private void completeAnchor(long from, long to) {
        List<Member> sample = params.context.successors(randomCut(params.digestAlgorithm));
        completeAnchor(sample.iterator(), from, to);
    }

    private Runnable completeAnchor(Member m, Iterator<Member> graphCut, ListenableFuture<Blocks> future, long from,
                                    long to) {
        return () -> {
            if (sync.isDone() || anchorSynchronized.isDone()) {
                return;
            }
            try {
                Blocks blocks = future.get();
                log.debug("Anchor completion ({} to {}) from: {} on: {}", from, to, m.getId(), params.member.getId());
                blocks.getBlocksList()
                      .stream()
                      .map(cb -> new HashedCertifiedBlock(params.digestAlgorithm, cb))
                      .peek(cb -> log.trace("Adding anchor completion: {} block[{}] from: {} on: {}", cb.height(),
                                            cb.hash, m, params.member))
                      .forEach(cb -> store.put(cb));
            } catch (InterruptedException e) {
                log.debug("Error completing Anchor from: {} on: {}", m.getId(), params.member.getId());
            } catch (ExecutionException e) {
                log.debug("Error completing Anchor from: {} on: {}", m.getId(), params.member.getId());
            }
            countdownAnchor(graphCut, from, to);
        };
    }

    private void completeViewChain(Iterator<Member> graphCut, long from, long to) {
        if (sync.isDone() || viewChainSynchronized.isDone()) {
            return;
        }
        if (!graphCut.hasNext()) {
            scheduleCompletion(store.lastViewChainFrom(from), to);
            return;
        }
        while (graphCut.hasNext()) {
            Member m = graphCut.next();
            BootstrapClient link = comms.apply(m, params.member);
            if (link == null) {
                log.info("No link for view chain completion: {} on: {}", m.getId(), params.member.getId());
                continue;
            }

            int seed = Utils.bitStreamEntropy().nextInt();
            BloomFilter<Long> blocksBff = new BloomFilter.LongBloomFilter(seed, params.maxViewBlocks,
                    params.msgParameters.falsePositiveRate);
            from = store.lastViewChainFrom(from);
            store.viewChainFrom(from, to).forEachRemaining(h -> blocksBff.add(h));
            BlockReplication replication = BlockReplication.newBuilder()
                                                           .setContext(params.context.getId().toByteString())
                                                           .setBlocksBff(blocksBff.toBff().toByteString())
                                                           .setFrom(from)
                                                           .setTo(to)
                                                           .build();

            log.debug("Attempting view chain completion ({} to {}) with: {} on: {}", from, to, m.getId(),
                      params.member.getId());
            try {
                ListenableFuture<Blocks> future = link.fetchViewChain(replication);
                future.addListener(completeViewChain(m, graphCut, future, from, to), params.dispatcher);
            } finally {
                link.release();
            }
            return;
        }
    }

    private void completeViewChain(long from, long to) {
        List<Member> sample = params.context.successors(randomCut(params.digestAlgorithm));
        completeViewChain(sample.iterator(), from, to);
    }

    private Runnable completeViewChain(Member m, Iterator<Member> graphCut, ListenableFuture<Blocks> future, long from,
                                       long to) {
        return () -> {
            if (sync.isDone() || viewChainSynchronized.isDone()) {
                return;
            }
            try {
                Blocks blocks = future.get();
                log.debug("View chain completion reply ({} to {}) from: {} on: {}", from, to, m.getId(),
                          params.member.getId());
                blocks.getBlocksList()
                      .stream()
                      .map(cb -> new HashedCertifiedBlock(params.digestAlgorithm, cb))
                      .peek(cb -> log.trace("Adding view completion: {} block[{}] from: {} on: {}", cb.height(),
                                            cb.hash, m, params.member))
                      .forEach(cb -> store.put(cb));
            } catch (InterruptedException e) {
                log.debug("Error counting vote from: {} on: {}", m.getId(), params.member.getId());
            } catch (ExecutionException e) {
                log.debug("Error counting vote from: {} on: {}", m.getId(), params.member.getId());
            }
            countdown(graphCut, from, to);
        };
    }

    private void computeGenesis(Map<Digest, Initial> votes) {

        log.info("Computing genesis with {} votes, required: {} on: {}", votes.size(),
                 params.context.toleranceLevel() + 1, params.member);
        Multiset<HashedCertifiedBlock> tally = HashMultiset.create();
        Map<Digest, Initial> valid = votes.entrySet()
                                          .stream()
                                          .filter(e -> e.getValue().hasGenesis()) // Has a genesis
                                          .filter(e -> genesis == null ? true : genesis.hash.equals(e.getKey())) // If
                                                                                                                 // restoring
                                                                                                                 // from
                                                                                                                 // known
                                                                                                                 // genesis...
                                          .filter(e -> {
                                              if (!e.getValue().hasCheckpoint() && lastCheckpoint <= 0) {
                                                  return true;
                                              }
                                              if (!e.getValue().hasCheckpointView()) {
                                                  return false; // if we have a checkpoint, we must have a view
                                              }

                                              long checkpointViewHeight = CollaboratorContext.height(e.getValue()
                                                                                                      .getCheckpointView()
                                                                                                      .getBlock());
                                              long recordedCheckpointViewHeight = e.getValue()
                                                                                   .getCheckpoint()
                                                                                   .getBlock()
                                                                                   .getHeader()
                                                                                   .getLastReconfig();
                                              // checkpoint's view should match
                                              return checkpointViewHeight == recordedCheckpointViewHeight;
                                          })
                                          .peek(e -> tally.add(new HashedCertifiedBlock(params.digestAlgorithm,
                                                  e.getValue().getGenesis())))
                                          .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        int threshold = params.context.toleranceLevel();
        if (genesis == null) {
            Pair<HashedCertifiedBlock, Integer> winner = null;

            log.info("Tally: {} required: {} on: {}", tally, params.context.toleranceLevel() + 1, params.member);
            for (HashedCertifiedBlock cb : tally) {
                int count = tally.count(cb);
                if (count > threshold) {
                    if (winner == null || count > winner.b) {
                        winner = new Pair<HashedCertifiedBlock, Integer>(cb, count);
                    }
                }
            }

            if (winner == null) {
                log.info("No winner on: {}", params.member);
                scheduleSample();
                return;
            }

            genesis = winner.a;
            log.info("Winner: {} on: {}", genesis.hash, params.member);
        }

        // get the most recent checkpoint.
        Initial mostRecent = valid.values()
                                  .stream()
                                  .filter(i -> i.hasGenesis())
                                  .filter(i -> genesis.hash.equals(new HashedCertifiedBlock(params.digestAlgorithm,
                                          i.getGenesis()).hash))
                                  .filter(i -> i.hasCheckpoint())
                                  .filter(i -> i.getCheckpoint().getBlock().getBody().getType() == BodyType.CHECKPOINT)
                                  .filter(i -> lastCheckpoint >= 0 ? true
                                          : CollaboratorContext.height(i.getCheckpoint()) > lastCheckpoint)
                                  .max((a, b) -> Long.compare(a.getCheckpoint().getBlock().getHeader().getHeight(),
                                                              b.getCheckpoint().getBlock().getHeader().getHeight()))
                                  .orElse(null);
        store.put(genesis);

        long anchorTo;
        if (mostRecent != null) {
            checkpointCompletion(threshold, mostRecent);
            anchorTo = checkpoint.height();
        } else {
            anchorTo = 0;
        }

        scheduleAnchorCompletion(anchor.height(), anchorTo);

        // Checkpoint must be assembled, view chain synchronized, and blocks spanning
        // the anchor block to the checkpoint must be filled
        CompletableFuture<Void> completion = mostRecent != null
                ? CompletableFuture.allOf(checkpointAssembled, viewChainSynchronized, anchorSynchronized)
                : CompletableFuture.allOf(anchorSynchronized);

        completion.whenComplete((v, t) -> {
            if (t == null) {
                log.info("Synchronized to: {} from: {} last view: {} on: {}", genesis.hash,
                         checkpoint == null ? genesis.hash : checkpoint.hash,
                         checkpointView == null ? genesis.hash : checkpoint.hash, params.member);
                sync.complete(new SynchronizedState(genesis, checkpointView, checkpoint, checkpointState));
            } else {
                log.error("Failed synchronizing to {} from: {} last view: {} on: {}", genesis.hash,
                          checkpoint == null ? genesis.hash : checkpoint.hash,
                          checkpointView == null ? genesis.hash : checkpoint.hash, t);
                sync.completeExceptionally(t);
            }
        }).exceptionally(t -> {
            log.error("Failed synchronizing to {} from: {} last view: {} on: {}", genesis.hash,
                      checkpoint == null ? genesis.hash : checkpoint.hash,
                      checkpointView == null ? genesis.hash : checkpoint.hash, t);
            sync.completeExceptionally(t);
            return null;
        });
    }

    private void countdown(Iterator<Member> graphCut, long from, long target) {
        if (store.completeFrom(from)) {
            validateViewChain();
        } else {
            completeViewChain(graphCut, from, target);
        }
    }

    private void countdownAnchor(Iterator<Member> graphCut, long from, long to) {
        if (store.firstGap(from, to) == to) {
            validateAnchor();
        } else {
            completeAnchor(graphCut, from, to);
        }
    }

    private void initialize(List<Member> graphCut, Map<Digest, Initial> votes, CountdownAction countdown) {
        final HashedCertifiedBlock established = genesis;
        if (sync.isDone() || established != null) {
            return;
        }
        Member m = graphCut.get(0);
        graphCut = graphCut.subList(1, graphCut.size());

        BootstrapClient link = comms.apply(m, params.member);
        if (link == null) {
            log.info("No link for {} on: {}", m, params.member);
            countdown.countdown();
            return;
        }
        Synchronize s = Synchronize.newBuilder()
                                   .setContext(params.context.getId().toByteString())
                                   .setHeight(anchor.height())
                                   .build();
        log.debug("Attempting synchronization with: {} on: {}", m, params.member);
        try {
            ListenableFuture<Initial> future = link.sync(s);
            future.addListener(initialize(m, graphCut, future, votes, countdown), params.dispatcher);
        } finally {
            link.release();
        }
    }

    private Runnable initialize(Member m, List<Member> graphCut, ListenableFuture<Initial> future,
                                Map<Digest, Initial> votes, CountdownAction countdown) {
        return () -> {
            try {
                final HashedCertifiedBlock established = genesis;
                if (sync.isDone() || established != null) {
                    return;
                }

                try {
                    Initial vote = future.get();
                    if (vote.hasGenesis()) {
                        HashedCertifiedBlock gen = new HashedCertifiedBlock(params.digestAlgorithm, vote.getGenesis());
                        if (gen.height() != 0) {
                            log.error("Returned genesis: {} is not height 0 from: {} on: {}", gen.hash, m,
                                      params.member);
                        }
                        votes.put(m.getId(), vote);
                        log.debug("Synchronization vote: {} from: {} recorded on: {}", gen.hash, m, params.member);
                    }
                } catch (InterruptedException e) {
                    log.debug("Error counting vote from: {} on: {}", m.getId(), params.member.getId());
                } catch (ExecutionException e) {
                    log.debug("Error counting vote from: {} on: {}", m.getId(), params.member.getId());
                }
                if (!countdown.countdown()) {
                    initialize(graphCut, votes, countdown);
                }
            } catch (Throwable t) {
                log.error("Failure in recording vote from: {} on: {}", m.getId(), params.member.getId(), t);
            }
        };
    }

    private void sample() {
        List<Member> sample = params.context.successors(randomCut(params.digestAlgorithm));
        HashMap<Digest, Initial> votes = new HashMap<>();
        CountdownAction countdown = new CountdownAction(() -> computeGenesis(votes), sample.size());
        initialize(sample, votes, countdown);
    }

    private void scheduleAnchorCompletion(long from, long to) {
        if (sync.isDone()) {
            return;
        }
        log.info("Scheduling Anchor completion ({} to {}) duration: {} millis on: {}", from, to,
                 params.synchronizeDuration.toMillis(), params.member);
        params.scheduler.schedule(() -> {
            try {
                completeAnchor(from, to);
            } catch (Throwable e) {
                log.error("Cannot execute completeViewChain on: {}", params.member);
                sync.completeExceptionally(e);
            }
        }, params.synchronizeDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void scheduleCompletion(long from, long to) {
        if (sync.isDone()) {
            return;
        }
        log.info("Scheduling view chain completion ({} to {}) duration: {} millis on: {}", from, to,
                 params.synchronizeDuration.toMillis(), params.member);
        params.scheduler.schedule(() -> {
            try {
                completeViewChain(from, to);
            } catch (Throwable e) {
                log.error("Cannot execute completeViewChain on: {}", params.member);
                sync.completeExceptionally(e);
            }
        }, params.synchronizeDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void scheduleSample() {
        if (sync.isDone()) {
            return;
        }
        log.info("Scheduling state sample on: {}", params.member);
        params.scheduler.schedule(() -> {
            try {
                sample();
            } catch (Throwable e) {
                log.error("Unable to sample sync state on: {}", params.member, e);
                sync.completeExceptionally(e);
            }
        }, params.synchronizeDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void validateAnchor() {
        long to = checkpoint == null ? 0 : checkpoint.height();
        try {
            store.validate(anchor.height(), to);
            anchorSynchronized.complete(true);
            log.info("Anchor chain to checkpoint synchronized on: {}", params.member);
        } catch (Throwable e) {
            log.error("Anchor chain from: {} to: {} does not validate on: {}", anchor.height(), to, params.member, e);
            anchorSynchronized.completeExceptionally(e);
        }
    }

    private void validateViewChain() {
        if (!viewChainSynchronized.isDone()) {
            try {
                store.validateViewChain(checkpointView.height());
                log.info("View chain synchronized on: {}", params.member);
                viewChainSynchronized.complete(true);
            } catch (Throwable t) {
                log.error("View chain from: {} to: {} does not validate on: {}", checkpointView.height(), 0,
                          params.member, t);
                viewChainSynchronized.completeExceptionally(t);
            }
        }
    }
}
