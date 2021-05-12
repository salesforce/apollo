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
import java.util.stream.LongStream;

import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.Store;
import com.salesforce.apollo.consortium.comms.ConsortiumClient;
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

    private final CompletableFuture<CheckpointState>             assembled = new CompletableFuture<>();
    private final Checkpoint                                     checkpoint;
    private final CommonCommunications<ConsortiumClient, Service> comms;
    private final Context<Member>                                context;
    private final double                                         fpr;
    private final List<HashKey>                                  hashes    = new ArrayList<>();
    private final Member                                         member;
    private final MVMap<Integer, byte[]>                         state;
    private final Store                                          store;

    public CheckpointAssembler(Checkpoint checkpoint, Member member, Store store,
            CommonCommunications<ConsortiumClient, Service> comms, Context<Member> context, double falsePositiveRate) {
        this.member = member;
        this.checkpoint = checkpoint;
        this.store = store;
        this.comms = comms;
        this.context = context;
        this.fpr = falsePositiveRate;
        state = store.createCheckpoint(checkpoint.getCheckpoint());
        checkpoint.getSegmentsList().stream().map(bs -> new HashKey(bs)).forEach(hash -> hashes.add(hash));
    }

    public CompletableFuture<CheckpointState> assemble(ScheduledExecutorService scheduler, Duration duration) {
        log.info("Scheduling assembly of checkpoint: {} segments: {} period: {} millis on: {}",
                 checkpoint.getCheckpoint(), checkpoint.getSegmentsCount(), duration.toMillis(), member);
        if (checkpoint.getSegmentsCount() == 0) {
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
                                                                     .setCheckpoint(checkpoint.getCheckpoint());
        request.setCheckpointSegments(segmentsBff.toBff().toByteString());

        BloomFilter<Long> blocksBff = new BloomFilter.LongBloomFilter(seed, checkpoint.getSegmentsCount(), fpr);
        LongStream.range(checkpoint.getCheckpoint() + 1, checkpoint.getSegmentsCount())
                  .filter(l -> store.containsBlock(l))
                  .forEach(l -> blocksBff.add(l));
        request.setBlocks(blocksBff.toBff().toByteString());
        return request.build();
    }

    private Runnable gossip(ConsortiumClient link, ListenableFuture<CheckpointSegments> futureSailor,
                            Runnable scheduler) {
        return () -> {
            link.release();

            try {
                if (process(futureSailor.get())) {
                    CheckpointState cs = new CheckpointState(checkpoint, state);
                    assembled.complete(cs);
                    return;
                }
            } catch (InterruptedException e) {
                log.trace("Failed to retrieve checkpoint {} segments from {} on: {}", checkpoint.getCheckpoint(),
                          member, e);
            } catch (ExecutionException e) {
                log.trace("Failed to retrieve checkpoint {} segments from {} on: {}", checkpoint.getCheckpoint(),
                          member, e.getCause());
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
            ConsortiumClient link = comms.apply(m, member);
            if (link == null) {
                log.trace("No link for: {} on: {}", m, member);
                s.run();
                return;
            }

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
