/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.BootstrappingService;
import com.salesforce.apollo.consortium.Store;
import com.salesforce.apollo.consortium.comms.BootstrapClient;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class CheckpointAssembler {
    private static final Logger log = LoggerFactory.getLogger(CheckpointAssembler.class);

    private final CompletableFuture<CheckpointState>                          assembled = new CompletableFuture<>();
    private final Checkpoint                                                  checkpoint;
    private final CommonCommunications<BootstrapClient, BootstrappingService> comms;
    private final Context<Member>                                             context;
    private final double                                                      fpr;
    private final List<HashKey>                                               hashes    = new ArrayList<>();
    private final long                                                        height;
    private final Member                                                      member;
    private final MVMap<Integer, byte[]>                                      state;

    public CheckpointAssembler(long height, Checkpoint checkpoint, Member member, Store store,
            CommonCommunications<BootstrapClient, BootstrappingService> comms, Context<Member> context,
            double falsePositiveRate) {
        this.height = height;
        this.member = member;
        this.checkpoint = checkpoint;
        this.comms = comms;
        this.context = context;
        this.fpr = falsePositiveRate;
        state = store.createCheckpoint(height);
        checkpoint.getSegmentsList().stream().map(bs -> new HashKey(bs)).forEach(hash -> hashes.add(hash));
    }

    public CompletableFuture<CheckpointState> assemble(ScheduledExecutorService scheduler, Duration duration) {
        log.info("Scheduling assembly of checkpoint: {} segments: {} period: {} millis on: {}", height,
                 checkpoint.getSegmentsCount(), duration.toMillis(), member);
        if (checkpoint.getSegmentsCount() == 0) {
            log.info("Assembled checkpoint: {} segments: {} on: {}", height, checkpoint.getSegmentsCount(), member);
            assembled.complete(new CheckpointState(checkpoint, state));
        } else {
            scheduler.schedule(() -> gossip(scheduler, duration), duration.toMillis(), TimeUnit.MILLISECONDS);
        }
        return assembled;
    }

    private CheckpointReplication buildRequest() {
        int seed = Utils.bitStreamEntropy().nextInt();
        BloomFilter<Integer> segmentsBff = new BloomFilter.IntBloomFilter(seed, checkpoint.getSegmentsCount(), fpr);
        IntStream.range(0, checkpoint.getSegmentsCount()).filter(i -> state.containsKey(i)).forEach(i -> {
            segmentsBff.add(i);
        });
        CheckpointReplication.Builder request = CheckpointReplication.newBuilder()
                                                                     .setContext(context.getId().toByteString())
                                                                     .setCheckpoint(height);
        request.setCheckpointSegments(segmentsBff.toBff().toByteString());
        return request.build();
    }

    private Runnable gossip(BootstrapClient link, ListenableFuture<CheckpointSegments> futureSailor,
                            Runnable scheduler) {
        return () -> {
            link.release();

            try {
                if (process(futureSailor.get())) {
                    CheckpointState cs = new CheckpointState(checkpoint, state);
                    log.info("Assembled checkpoint: {} segments: {} on: {}", height, checkpoint.getSegmentsCount(),
                             member);
                    assembled.complete(cs);
                    return;
                }
            } catch (InterruptedException e) {
                log.trace("Failed to retrieve checkpoint {} segments from {} on: {}", height, member, e);
            } catch (ExecutionException e) {
                log.trace("Failed to retrieve checkpoint {} segments from {} on: {}", height, member, e.getCause());
            }
            scheduler.run();
        };
    }

    private void gossip(ScheduledExecutorService scheduler, Duration duration) {
        if (assembled.isDone()) {
            return;
        }
        Runnable s = () -> scheduler.schedule(() -> gossip(scheduler, duration), duration.toMillis(),
                                              TimeUnit.MILLISECONDS);
        try {
            List<Member> sample = context.sample(1, Utils.bitStreamEntropy(), member.getId());
            if (sample.size() < 1) {
                log.trace("No member sample for checkpoint assembly on: {}", member);
                s.run();
            }
            CheckpointReplication request = buildRequest();
            if (request == null) {
                log.trace("No member sample for checkpoint assembly on: {}", member);
                s.run();
                return;
            }
            Member m = sample.get(0);
            BootstrapClient link = comms.apply(m, member);
            if (link == null) {
                log.trace("No link for: {} on: {}", m, member);
                s.run();
                return;
            }

            log.info("Checkpoint assembly gossip with: {} on: {}", m, member);
            ListenableFuture<CheckpointSegments> fetched = link.fetch(request);
            fetched.addListener(gossip(link, fetched, s), ForkJoinPool.commonPool());
        } catch (Throwable e) {
            log.error("Error in scheduled checkpoint assembly gossip on: {}", member, e);
        }
    }

    private boolean process(CheckpointSegments segments) {
        segments.getSegmentsList().forEach(segment -> {
            HashKey hash = new HashKey(Conversion.hashOf(segment.getBlock()));
            int index = segment.getIndex();
            if (index >= 0 && index < hashes.size()) {
                if (hash.equals(hashes.get(index))) {
                    state.computeIfAbsent(index, i -> segment.getBlock().toByteArray());
                }
            }
        });
        return state.size() == hashes.size();
    }
}
