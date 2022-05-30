/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.joou.ULong;
import org.joou.Unsigned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Initial;
import com.salesfoce.apollo.choam.proto.Synchronize;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.comm.RingIterator;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Pair;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.ULongBloomFilter;

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

    public static Digest randomCut(DigestAlgorithm algo) {
        long[] cut = new long[algo.longLength()];
        for (int i = 0; i < cut.length; i++) {
            cut[i] = Entropy.nextSecureLong();
        }
        return new Digest(algo, cut);
    }

    private final HashedCertifiedBlock                      anchor;
    private final CompletableFuture<Boolean>                anchorSynchronized    = new CompletableFuture<>();
    private volatile HashedCertifiedBlock                   checkpoint;
    private volatile CompletableFuture<CheckpointState>     checkpointAssembled;
    private volatile CheckpointState                        checkpointState;
    private volatile HashedCertifiedBlock                   checkpointView;
    private final CommonCommunications<Terminal, Concierge> comms;
    private volatile HashedCertifiedBlock                   genesis;
    private final ULong                                     lastCheckpoint;
    private final Parameters                                params;
    private final Store                                     store;
    private final CompletableFuture<SynchronizedState>      sync                  = new CompletableFuture<>();
    private final CompletableFuture<Boolean>                viewChainSynchronized = new CompletableFuture<>();

    public Bootstrapper(HashedCertifiedBlock anchor, Parameters params, Store store,
                        CommonCommunications<Terminal, Concierge> bootstrapComm) {
        this.anchor = anchor;
        this.params = params;
        this.store = store;
        this.comms = bootstrapComm;
        CertifiedBlock g = store.getCertifiedBlock(ULong.valueOf(0));
        store.put(anchor);
        if (g != null) {
            genesis = new HashedCertifiedBlock(params.digestAlgorithm(), g);
            log.info("Restore using genesis: {} on: {}", genesis.hash, params.member().getId());
            lastCheckpoint = ULong.valueOf(store.getLastBlock().block.getHeader().getLastCheckpoint());
        } else {
            log.info("Restore using no prior state on: {}", params.member().getId());
            lastCheckpoint = null;
        }
    }

    public CompletableFuture<SynchronizedState> synchronize() {
        scheduleSample();
        return sync;
    }

    private void anchor(AtomicReference<ULong> start, ULong end) {
        new RingIterator<>(params.gossipDuration(), params.context(), params.member(), params.scheduler(), comms,
                           params.exec()).iterate(randomCut(params.digestAlgorithm()),
                                                  (link, ring) -> anchor(link, start, end),
                                                  (tally, futureSailor, link, ring) -> completeAnchor(futureSailor,
                                                                                                      start, end, link),
                                                  () -> scheduleAnchorCompletion(start, end));
    }

    private ListenableFuture<Blocks> anchor(Terminal link, AtomicReference<ULong> start, ULong end) {
        log.debug("Attempting Anchor completion ({} to {}) with: {} on: {}", start, end, link.getMember().getId(),
                  params.member().getId());
        long seed = Entropy.nextBitsStreamLong();
        BloomFilter<ULong> blocksBff = new BloomFilter.ULongBloomFilter(seed, params.bootstrap().maxViewBlocks(),
                                                                        params.combine().falsePositiveRate());

        start.set(store.firstGap(start.get(), end));
        store.blocksFrom(start.get(), end, params.bootstrap().maxSyncBlocks()).forEachRemaining(h -> blocksBff.add(h));
        BlockReplication replication = BlockReplication.newBuilder()
                                                       .setContext(params.context().getId().toDigeste())
                                                       .setBlocksBff(blocksBff.toBff())
                                                       .setFrom(start.get().longValue())
                                                       .setTo(end.longValue())
                                                       .build();
        return link.fetchBlocks(replication);
    }

    private void checkpointCompletion(int threshold, Initial mostRecent) {
        checkpoint = new HashedCertifiedBlock(params.digestAlgorithm(), mostRecent.getCheckpoint());
        store.put(checkpoint);

        checkpointView = new HashedCertifiedBlock(params.digestAlgorithm(), mostRecent.getCheckpointView());
        store.put(checkpointView);
        assert !checkpointView.height()
                              .equals(Unsigned.ulong(0)) : "Should not attempt when bootstrapping from genesis";
        log.info("Assembling from checkpoint: {}:{} on: {}", checkpoint.height(), checkpoint.hash,
                 params.member().getId());

        CheckpointAssembler assembler = new CheckpointAssembler(params.gossipDuration(), checkpoint.height(),
                                                                checkpoint.block.getCheckpoint(), params.member(),
                                                                store, comms, params.context(), threshold,
                                                                params.digestAlgorithm());

        // assemble the checkpoint
        checkpointAssembled = assembler.assemble(params.scheduler(), params.synchronizeDuration(), params.exec())
                                       .whenComplete((cps, t) -> {
                                           log.info("Restored checkpoint: {} on: {}", checkpoint.height(),
                                                    params.member().getId());
                                           checkpointState = cps;
                                       });
        // reconstruct chain to genesis
        mostRecent.getViewChainList()
                  .stream()
                  .filter(cb -> cb.getBlock().hasReconfigure())
                  .map(cb -> new HashedCertifiedBlock(params.digestAlgorithm(), cb))
                  .forEach(reconfigure -> {
                      store.put(reconfigure);
                  });
        scheduleViewChainCompletion(new AtomicReference<>(checkpointView.height()), ULong.valueOf(0));
    }

    private boolean completeAnchor(Optional<ListenableFuture<Blocks>> futureSailor, AtomicReference<ULong> start,
                                   ULong end, Terminal link) {
        if (sync.isDone() || anchorSynchronized.isDone()) {
            log.trace("Anchor synchronized isDone: {} anchor sync: {} on: {}", sync.isDone(),
                      anchorSynchronized.isDone(), params.member().getId());
            return false;
        }
        if (futureSailor.isEmpty()) {
            return true;
        }
        try {
            Blocks blocks = futureSailor.get().get();
            log.debug("View chain completion reply ({} to {}) from: {} on: {}", start.get(), end,
                      link.getMember().getId(), params.member().getId());
            blocks.getBlocksList()
                  .stream()
                  .map(cb -> new HashedCertifiedBlock(params.digestAlgorithm(), cb))
                  .peek(cb -> log.trace("Adding view completion: {} block[{}] from: {} on: {}", cb.height(), cb.hash,
                                        link.getMember().getId(), params.member().getId()))
                  .forEach(cb -> store.put(cb));
        } catch (InterruptedException e) {
            log.debug("Error counting vote from: {} on: {}", link.getMember().getId(), params.member().getId());
        } catch (ExecutionException e) {
            log.debug("Error counting vote from: {} on: {}", link.getMember().getId(), params.member().getId());
        }
        if (store.firstGap(start.get(), end).equals(end)) {
            validateAnchor();
            return false;
        }
        return true;
    }

    private void completeViewChain(AtomicReference<ULong> start, ULong end) {
        new RingIterator<>(params.gossipDuration(), params.context(), params.member(), params.scheduler(), comms,
                           params.exec()).iterate(randomCut(params.digestAlgorithm()),
                                                  (link, ring) -> completeViewChain(link, start, end),
                                                  (tally, futureSailor, link,
                                                   ring) -> completeViewChain(futureSailor, start, end, link),
                                                  () -> scheduleViewChainCompletion(start, end));
    }

    private boolean completeViewChain(Optional<ListenableFuture<Blocks>> futureSailor, AtomicReference<ULong> start,
                                      ULong end, Terminal link) {
        if (sync.isDone() || viewChainSynchronized.isDone()) {
            log.trace("View chain synchronized isDone: {} sync: {} on: {}", sync.isDone(),
                      viewChainSynchronized.isDone(), params.member().getId());
            return false;
        }
        if (futureSailor.isEmpty()) {
            return true;
        }

        try {
            Blocks blocks = futureSailor.get().get();
            log.debug("View chain completion reply ({} to {}) from: {} on: {}", start.get(), end,
                      link.getMember().getId(), params.member().getId());
            blocks.getBlocksList()
                  .stream()
                  .map(cb -> new HashedCertifiedBlock(params.digestAlgorithm(), cb))
                  .peek(cb -> log.trace("Adding view completion: {} block[{}] from: {} on: {}", cb.height(), cb.hash,
                                        link.getMember().getId(), params.member().getId()))
                  .forEach(cb -> store.put(cb));
        } catch (InterruptedException e) {
            log.debug("Error counting vote from: {} on: {}", link.getMember().getId(), params.member().getId());
        } catch (ExecutionException e) {
            log.debug("Error counting vote from: {} on: {}", link.getMember().getId(), params.member().getId());
        }
        if (store.completeFrom(start.get())) {
            validateViewChain();
            log.debug("View chain complete ({} to {}) from: {} on: {}", start.get(), end, link.getMember().getId(),
                      params.member().getId());
            return false;
        }
        return true;
    }

    private ListenableFuture<Blocks> completeViewChain(Terminal link, AtomicReference<ULong> start, ULong end) {
        log.debug("Attempting view chain completion ({} to {}) with: {} on: {}", start.get(), end,
                  link.getMember().getId(), params.member().getId());
        long seed = Entropy.nextBitsStreamLong();
        ULongBloomFilter blocksBff = new BloomFilter.ULongBloomFilter(seed, params.bootstrap().maxViewBlocks(),
                                                                      params.combine().falsePositiveRate());
        start.set(store.lastViewChainFrom(start.get()));
        store.viewChainFrom(start.get(), end).forEachRemaining(h -> blocksBff.add(h));
        BlockReplication replication = BlockReplication.newBuilder()
                                                       .setContext(params.context().getId().toDigeste())
                                                       .setBlocksBff(blocksBff.toBff())
                                                       .setFrom(start.get().longValue())
                                                       .setTo(end.longValue())
                                                       .build();

        return link.fetchViewChain(replication);
    }

    private void computeGenesis(Map<Digest, Initial> votes) {
        log.info("Computing genesis with {} votes, required: {} on: {}", votes.size(), params.majority(),
                 params.member().getId());
        Multiset<HashedCertifiedBlock> tally = TreeMultiset.create();
        Map<Digest, Initial> valid = votes.entrySet()
                                          .stream()
                                          .filter(e -> e.getValue().hasGenesis()) // Has a genesis
                                          .filter(e -> genesis == null ? true : genesis.hash.equals(e.getKey())) // If
                                                                                                                 // restoring
                                                                                                                 // from
                                                                                                                 // known
                                                                                                                 // genesis...
                                          .filter(e -> {
                                              if (e.getValue().hasGenesis()) {
                                                  if (lastCheckpoint != null &&
                                                      lastCheckpoint.compareTo(ULong.valueOf(0)) > 0) {
                                                      log.trace("Rejecting genesis: {} last checkpoint: {} > 0 on: {}",
                                                                e.getKey(), lastCheckpoint, params.member().getId());
                                                      return false;
                                                  }
                                                  log.trace("Accepting genesis: {} on: {}", e.getKey(),
                                                            params.member().getId());
                                                  return true;
                                              }
                                              if (!e.getValue().hasCheckpoint()) {
                                                  log.trace("Rejecting: {} has no checkpoint. last checkpoint: {} > 0 on: {}",
                                                            e.getKey(), lastCheckpoint, params.member().getId());
                                                  return false;
                                              }

                                              ULong checkpointViewHeight = HashedBlock.height(e.getValue()
                                                                                               .getCheckpointView()
                                                                                               .getBlock());
                                              ULong recordedCheckpointViewHeight = ULong.valueOf(e.getValue()
                                                                                                  .getCheckpoint()
                                                                                                  .getBlock()
                                                                                                  .getHeader()
                                                                                                  .getLastReconfig());
                                              // checkpoint's view should match
                                              log.trace("Accepting checkpoint: {} on: {}", e.getKey(),
                                                        params.member().getId());
                                              return checkpointViewHeight.equals(recordedCheckpointViewHeight);
                                          })
                                          .peek(e -> tally.add(new HashedCertifiedBlock(params.digestAlgorithm(),
                                                                                        e.getValue().getGenesis())))
                                          .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        int threshold = params.majority();
        if (genesis == null) {
            Pair<HashedCertifiedBlock, Integer> winner = null;

            log.info("Tally: {} required: {} on: {}", tally, params.majority(), params.member().getId());
            for (HashedCertifiedBlock cb : tally) {
                int count = tally.count(cb);
                if (count >= threshold) {
                    if (winner == null || count > winner.b) {
                        winner = new Pair<>(cb, count);
                    }
                }
            }

            if (winner == null) {
                log.debug("No winner on: {}", params.member().getId());
                scheduleSample();
                return;
            }

            genesis = winner.a;
            log.info("Winner: {} on: {}", genesis.hash, params.member().getId());
        }

        // get the most recent checkpoint.
        Initial mostRecent = valid.values()
                                  .stream()
                                  .filter(i -> i.hasGenesis())
                                  .filter(i -> genesis.hash.equals(new HashedCertifiedBlock(params.digestAlgorithm(),
                                                                                            i.getGenesis()).hash))
                                  .filter(i -> i.hasCheckpoint())
                                  .filter(i -> lastCheckpoint != null ? true
                                                              : lastCheckpoint != null
                                                                               ? HashedBlock.height(i.getCheckpoint())
                                                                                            .compareTo(lastCheckpoint) > 0
                                                              : true)
                                  .max((a, b) -> Long.compare(a.getCheckpoint().getBlock().getHeader().getHeight(),
                                                              b.getCheckpoint().getBlock().getHeader().getHeight()))
                                  .orElse(null);
        store.put(genesis);

        ULong anchorTo;
        boolean genesisBootstrap = mostRecent == null ||
                                   mostRecent.getCheckpointView().getBlock().getHeader().getHeight() == 0;
        if (!genesisBootstrap) {
            checkpointCompletion(threshold, mostRecent);
            anchorTo = checkpoint.height();
        } else {
            anchorTo = ULong.valueOf(0);
        }

        anchor(new AtomicReference<>(anchor.height()), anchorTo);

        // Checkpoint must be assembled, view chain synchronized, and blocks spanning
        // the anchor block to the checkpoint must be filled
        CompletableFuture<Void> completion = !genesisBootstrap ? CompletableFuture.allOf(checkpointAssembled,
                                                                                         viewChainSynchronized,
                                                                                         anchorSynchronized)
                                                               : CompletableFuture.allOf(anchorSynchronized);

        completion.whenComplete((v, t) -> {
            if (t == null) {
                log.info("Synchronized to: {} from: {} last view: {} on: {}", genesis.hash,
                         checkpoint == null ? genesis.hash : checkpoint.hash,
                         checkpointView == null ? genesis.hash : checkpoint.hash, params.member().getId());
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

    private void sample() {
        final HashedCertifiedBlock established = genesis;
        if (sync.isDone() || established != null) {
            log.trace("Synchronization isDone: {} genesis: {} on: {}", sync.isDone(),
                      established == null ? null : established.hash, params.member().getId());
            return;
        }
        HashMap<Digest, Initial> votes = new HashMap<>();
        Synchronize s = Synchronize.newBuilder()
                                   .setContext(params.context().getId().toDigeste())
                                   .setHeight(anchor.height().longValue())
                                   .build();
        final var randomCut = randomCut(params.digestAlgorithm());
        new RingIterator<>(params.gossipDuration(), params.context(), params.member(), comms, params.exec(), true,
                           params.scheduler()).iterate(randomCut, (link, ring) -> synchronize(s, link),
                                                       (tally, futureSailor, link, ring) -> synchronize(futureSailor,
                                                                                                        votes, link),
                                                       () -> computeGenesis(votes));
    }

    private void scheduleAnchorCompletion(AtomicReference<ULong> start, ULong anchorTo) {
        assert !start.get().equals(anchorTo) : "Should not schedule anchor completion on an empty interval: ["
        + start.get() + ":" + anchorTo + "]";
        if (sync.isDone()) {
            return;
        }
        log.info("Scheduling Anchor completion ({} to {}) duration: {} millis on: {}", start, anchorTo,
                 params.synchronizeDuration().toMillis(), params.member().getId());
        params.scheduler().schedule(() -> {
            try {
                anchor(start, anchorTo);
            } catch (Throwable e) {
                log.error("Cannot execute completeViewChain on: {}", params.member().getId());
                sync.completeExceptionally(e);
            }
        }, params.synchronizeDuration().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void scheduleSample() {
        if (sync.isDone()) {
            return;
        }
        log.info("Scheduling state sample on: {}", params.member().getId());
        params.scheduler().schedule(() -> {
            final HashedCertifiedBlock established = genesis;
            if (sync.isDone() || established != null) {
                log.trace("Synchronization isDone: {} genesis: {} on: {}", sync.isDone(),
                          established == null ? null : established.hash, params.member().getId());
                return;
            }
            try {
                sample();
            } catch (Throwable e) {
                log.error("Unable to sample sync state on: {}", params.member().getId(), e);
                sync.completeExceptionally(e);
            }
        }, params.synchronizeDuration().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void scheduleViewChainCompletion(AtomicReference<ULong> start, ULong to) {
        assert !start.get().equals(to) : "Should not schedule view chain completion on an empty interval: ["
        + start.get() + ":" + to + "]";
        if (sync.isDone()) {
            log.trace("View chain complete on: {}", params.member().getId());
            return;
        }
        log.info("Scheduling view chain completion ({} to {}) duration: {} millis on: {}", start, to,
                 params.synchronizeDuration().toMillis(), params.member().getId());
        params.scheduler().schedule(() -> {
            try {
                completeViewChain(start, to);
            } catch (Throwable e) {
                log.error("Cannot execute completeViewChain on: {}", params.member().getId());
                sync.completeExceptionally(e);
            }
        }, params.synchronizeDuration().toMillis(), TimeUnit.MILLISECONDS);
    }

    private boolean synchronize(Optional<ListenableFuture<Initial>> futureSailor, HashMap<Digest, Initial> votes,
                                Terminal link) {
        final HashedCertifiedBlock established = genesis;
        if (sync.isDone() || established != null) {
            log.trace("Terminating synchronization early isDone: {} genesis: {} cancelled: {} on: {}", sync.isDone(),
                      established == null ? null : established.hash, link.getMember().getId(), params.member().getId());
            return false;
        }
        if (futureSailor.isEmpty()) {
            log.trace("Empty synchronization response from: {} on: {}", link == null ? null : link.getMember().getId(),
                      params.member().getId());
            return true;
        }
        try {
            Initial vote = futureSailor.get().get();
            if (vote.hasGenesis()) {
                HashedCertifiedBlock gen = new HashedCertifiedBlock(params.digestAlgorithm(), vote.getGenesis());
                if (!gen.height().equals(ULong.valueOf(0))) {
                    log.error("Returned genesis: {} is not height 0 from: {} on: {}", gen.hash,
                              link.getMember().getId(), params.member().getId());
                }
                votes.put(link.getMember().getId(), vote);
                log.debug("Synchronization vote: {} count: {} from: {} recorded on: {}", gen.hash, votes.size(),
                          link.getMember().getId(), params.member().getId());
            }
        } catch (InterruptedException e) {
            log.warn("Error counting vote from: {} on: {}", link.getMember().getId(), params.member().getId());
        } catch (ExecutionException e) {
            log.warn("Error counting vote from: {} on: {}", link.getMember().getId(), params.member().getId());
        }
        log.trace("Continuing, processed sync response from: {} on: {}", link.getMember().getId(),
                  params.member().getId());
        return true;
    }

    private ListenableFuture<Initial> synchronize(Synchronize s, Terminal link) {
        if (params.member().equals(link.getMember())) {
            return null;
        }
        log.debug("Attempting synchronization with: {} on: {}", link.getMember().getId(), params.member().getId());
        return link.sync(s);
    }

    private void validateAnchor() {
        ULong to = checkpoint == null ? ULong.valueOf(0) : checkpoint.height();
        try {
            store.validate(anchor.height(), to);
            anchorSynchronized.complete(true);
            log.info("Anchor chain to checkpoint synchronized on: {}", params.member().getId());
        } catch (Throwable e) {
            log.error("Anchor chain from: {} to: {} does not validate on: {}", anchor.height(), to,
                      params.member().getId(), e);
            anchorSynchronized.completeExceptionally(e);
        }
    }

    private void validateViewChain() {
        if (!viewChainSynchronized.isDone()) {
            try {
                store.validateViewChain(checkpointView.height());
                log.info("View chain synchronized on: {}", params.member().getId());
                viewChainSynchronized.complete(true);
            } catch (Throwable t) {
                log.error("View chain from: {} to: {} does not validate on: {}", checkpointView.height(), 0,
                          params.member().getId(), t);
                viewChainSynchronized.completeExceptionally(t);
            }
        }
    }
}
