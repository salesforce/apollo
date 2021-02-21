/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.Store;
import com.salesforce.apollo.consortium.comms.ConsortiumClientCommunications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Bootstrapper {
    private static final Logger log = LoggerFactory.getLogger(Bootstrapper.class);

    private final CompletableFuture<CheckpointState>                            assembled = new CompletableFuture<>();
    private final Checkpoint                                                    checkpoint;
    private final CommonCommunications<ConsortiumClientCommunications, Service> comms;
    private final Context<Member>                                               context;
    private final double                                                        fpr;
    private final List<HashKey>                                                 hashes    = new ArrayList<>();
    private final Member                                                        member;
    private final MVMap<Integer, byte[]>                                        state;
    private final Store                                                         store;

    public Bootstrapper(Checkpoint checkpoint, Member member, Store store,
            CommonCommunications<ConsortiumClientCommunications, Service> comms, Context<Member> context,
            double falsePositiveRate) {
        this.member = member;
        this.checkpoint = checkpoint;
        this.store = store;
        this.comms = comms;
        this.context = context;
        this.fpr = falsePositiveRate;
        state = store.createCheckpoint(checkpoint.getCheckpoint());
        checkpoint.getSegmentsList().stream().map(bs -> new HashKey(bs)).forEach(hash -> hashes.add(hash));
    }

    public CompletableFuture<CheckpointState> assemble(ScheduledExecutorService scheduler, Duration duration,
                                                       BitsStreamGenerator entropy) {
        gossip(scheduler, duration, entropy);
        return assembled;
    }

    private CheckpointReplication buildRequest() {
        BloomFilter<Integer> segmentsBff = BloomFilter.create(Funnels.integerFunnel(), checkpoint.getSegmentsCount(),
                                                              fpr);
        IntStream.range(0, checkpoint.getSegmentsCount())
                 .filter(i -> !state.containsKey(i))
                 .forEach(i -> segmentsBff.put(i));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            segmentsBff.writeTo(baos);
        } catch (IOException e) {
            log.error("Error serializing BFF", e);
            return null;
        }
        CheckpointReplication.Builder request = CheckpointReplication.newBuilder()
                                                                     .setContext(context.getId().toByteString())
                                                                     .setCheckpoint(checkpoint.getCheckpoint());
        request.setCheckpointSegments(ByteString.copyFrom(baos.toByteArray()));

        BloomFilter<Long> blocksBff = BloomFilter.create(Funnels.longFunnel(), checkpoint.getSegmentsCount(), fpr);
        LongStream.range(checkpoint.getCheckpoint() + 1, checkpoint.getSegmentsCount())
                  .filter(l -> !store.containsBlock(l))
                  .forEach(l -> blocksBff.put(l));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try {
            blocksBff.writeTo(bytes);
        } catch (IOException e) {
            log.error("Error serializing BFF", e);
            return null;
        }
        request.setBlocks(ByteString.copyFrom(bytes.toByteArray()));
        return request.build();
    }

    private Runnable gossip(ConsortiumClientCommunications link, ListenableFuture<CheckpointSegments> futureSailor,
                            BitsStreamGenerator entropy, Runnable scheduler) {
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

    private void gossip(ScheduledExecutorService scheduler, Duration duration, BitsStreamGenerator entropy) {
        if (assembled.isDone()) {
            return;
        }
        Runnable s = () -> scheduler.schedule(() -> gossip(scheduler, duration, entropy), duration.toMillis(),
                                              TimeUnit.MILLISECONDS);
        List<Member> sample = context.sample(1, entropy, member.getId());
        if (sample.size() < 1) {
            s.run();
        }
        CheckpointReplication request = buildRequest();
        if (request == null) {
            s.run();
            return;
        }
        Member m = sample.get(0);
        ConsortiumClientCommunications link = comms.apply(m, member);
        if (link == null) {
            log.trace("No link for: {} on: {}", m, member);
            s.run();
            return;
        }

        ListenableFuture<CheckpointSegments> futureSailor = link.fetch(request);
        futureSailor.addListener(gossip(link, futureSailor, entropy, s), ForkJoinPool.commonPool());

    }

    private boolean process(CheckpointSegments segments) {
        segments.getSegmentsList().forEach(segment -> {
            HashKey hash = new HashKey(Conversion.hashOf(segment.getBlock()));
            int index = segment.getIndex();
            if (index <= 0 || index >= hashes.size()) {

            }
            if (hash.equals(hashes.get(index))) {
                state.computeIfAbsent(index, i -> segment.getBlock().toByteArray());
            }
        });
        return state.size() == hashes.size();
    }
}
