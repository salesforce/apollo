/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.BootstrapSync;
import com.salesfoce.apollo.consortium.proto.CheckpointSync;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.Store;
import com.salesforce.apollo.consortium.comms.BootstrapClient;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Pair;

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

    private HashedCertifiedBlock                                                      checkpoint;
    private final CommonCommunications<BootstrapClient, Service>                      comms;
    private final Context<Member>                                                     context;
    private final double                                                              fpr;
    private HashedCertifiedBlock                                                      genesis;
    private final Member                                                              member;
    private final int                                                                 slice;
    private final Store                                                               store;
    private final CompletableFuture<Pair<HashedCertifiedBlock, HashedCertifiedBlock>> sync  = new CompletableFuture<>();
    private final Map<HashKey, BootstrapSync>                                         votes = new ConcurrentHashMap<>();

    public Bootstrapper(Member member, Context<Member> context, CommonCommunications<BootstrapClient, Service> comms,
            double falsePositiveRate, Store store, int slice) {
        this.comms = comms;
        this.context = context;
        this.fpr = falsePositiveRate;
        this.member = member;
        this.store = store;
        this.slice = slice;
    }

    /**
     * Synchronize with the context. The Store of the receiver is synchronized with
     * the state of the view context, with Genesis and the selected Checkpoint
     * blocks. The Reconfigure chain from the Checkpoint back to the Genesis block
     * is also synchronized in the store.
     * 
     * @return a CompletableFuture that will contain a Pair of HashCertifiedBlocks.
     *         The first element of the pair is the Genesis block, the second
     *         element is the Checkpoint block to use to bootstrap the node
     * 
     *         If the returned Pair is NULL, then no member has a Genesis block. If
     *         there is not consensus on the Genesis block of the Context, the
     *         future will complete exceptionally with the GenesisNotResolved
     *         exception
     */
    public CompletableFuture<Pair<HashedCertifiedBlock, HashedCertifiedBlock>> synchronize(ScheduledExecutorService scheduler,
                                                                                           Duration duration,
                                                                                           BitsStreamGenerator entropy) {
        discoverGenesis(scheduler, duration, entropy);
        return sync;
    }

    private void computeGenesis() {
        Map<HashKey, Pair<HashedCertifiedBlock, Pair<HashedCertifiedBlock, List<Long>>>> consolidated = votes.entrySet()
                                                                                                             .stream()
                                                                                                             .collect(Collectors.toMap(e -> e.getKey(),
                                                                                                                                       e -> consolidate(e.getValue())));
        Map<HashKey, Integer> tally = new HashMap<>();
        consolidated.values().stream().forEach(s -> tally.compute(s.a.hash, (h, c) -> c == null ? 1 : c + 1));
        // Consensus is > tolerance level and max votes
        HashKey winner = tally.entrySet()
                              .stream()
                              .filter(e -> e.getValue() < context.toleranceLevel())
                              .max((e1, e2) -> Integer.compare(e1.getValue(), e2.getValue()))
                              .map(e -> e.getKey())
                              .orElse(null);
        if (winner == null) {
            if (votes.isEmpty()) {
                sync.complete(null); // No member has a genesis block
            } else {
                sync.completeExceptionally(new GenesisNotResolved()); // Cannot find agreement on a genesis block
            }
            return;
        }
        Pair<HashedCertifiedBlock, List<Long>> longest = consolidated.values()
                                                                     .stream()
                                                                     .map(p -> p.b)
                                                                     .filter(e -> winner.equals(e.a.hash))
                                                                     .max((a,
                                                                           b) -> Long.compare(a.a.block.getBlock()
                                                                                                       .getHeader()
                                                                                                       .getHeight(),
                                                                                              b.a.block.getBlock()
                                                                                                       .getHeader()
                                                                                                       .getHeight()))
                                                                     .orElse(null);

    }

    private Pair<HashedCertifiedBlock, Pair<HashedCertifiedBlock, List<Long>>> consolidate(BootstrapSync sync) {
        return new Pair<>(new HashedCertifiedBlock(sync.getGenesis()),
                new Pair<>(new HashedCertifiedBlock(sync.getView()), sync.getViewChainList()));
    }

    private void countdown(List<Member> remaining, AtomicInteger countdown, ScheduledExecutorService scheduler,
                           Duration duration, BitsStreamGenerator entropy) {
        if (sync.isDone()) {
            return;
        }
        if (countdown.decrementAndGet() == 0) {
            if (remaining.isEmpty()) {
                computeGenesis();
            } else {
                discoverGenesis(remaining, scheduler, duration, entropy);
            }
        }
    }

    private void discoverGenesis(List<Member> members, ScheduledExecutorService scheduler, Duration duration,
                                 BitsStreamGenerator entropy) {
        if (sync.isDone()) {
            return;
        }
        CheckpointSync request = CheckpointSync.newBuilder().setContext(context.getId().toByteString()).build();

        AtomicInteger countdown = new AtomicInteger();
        List<Member> slice = members.subList(0, this.slice);
        List<Member> remaining = members.subList(slice.size(), members.size());
        for (Member m : context.activeMembers()) {
            BootstrapClient link = comms.apply(m, member);
            if (link == null) {
                log.trace("No link for: {} on: {}", m, member);
                countdown(remaining, countdown, scheduler, duration, entropy);
            } else {
                ListenableFuture<BootstrapSync> future = link.checkpointSync(request);
                future.addListener(discoverGenesis(m, remaining, future, countdown, scheduler, duration, entropy),
                                   ForkJoinPool.commonPool());
            }
        }
    }

    private Runnable discoverGenesis(Member m, List<Member> remaining, ListenableFuture<BootstrapSync> future,
                                     AtomicInteger countdown, ScheduledExecutorService scheduler, Duration duration,
                                     BitsStreamGenerator entropy) {
        return () -> {
            if (sync.isDone()) {
                return;
            }
            try {
                BootstrapSync result = BootstrapSync.getDefaultInstance();
                try {
                    result = future.get();
                } catch (InterruptedException | ExecutionException e) {
                    log.info("Unable to get vote from: {} on: {}", e.getCause());
                }
                votes.put(m.getId(), result);
            } finally {
                countdown(remaining, countdown, scheduler, duration, entropy);
            }
        };
    }

    private void discoverGenesis(ScheduledExecutorService scheduler, Duration duration, BitsStreamGenerator entropy) {
        if (sync.isDone()) {
            return;
        }
        // Generate a random cut across the rings
        long[] cut = new long[HashKey.LONG_SIZE];
        for (int i = 0; i < HashKey.LONG_SIZE; i++) {
            cut[i] = entropy.nextLong();
        }
        discoverGenesis(context.successors(new HashKey(cut)), scheduler, duration, entropy);
    }
}
