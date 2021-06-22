/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import static com.salesforce.apollo.consortium.support.Bootstrapper.randomCut;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
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
import com.salesforce.apollo.comm.RingIterator;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.Bootstrapping;
import com.salesforce.apollo.consortium.Store;
import com.salesforce.apollo.consortium.comms.BootstrapService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.BloomFilter;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class CheckpointAssembler {
    private static final Logger log = LoggerFactory.getLogger(CheckpointAssembler.class);

    private final CompletableFuture<CheckpointState>                    assembled = new CompletableFuture<>();
    private final Checkpoint                                            checkpoint;
    private final CommonCommunications<BootstrapService, Bootstrapping> comms;
    private final Context<Member>                                       context;
    private final DigestAlgorithm                                       digestAlgorithm;
    private final Executor                                              executor;
    private final double                                                fpr;
    private final List<Digest>                                          hashes    = new ArrayList<>();
    private final long                                                  height;
    private final SigningMember                                         member;
    private final MVMap<Integer, byte[]>                                state;

    public CheckpointAssembler(long height, Checkpoint checkpoint, SigningMember member, Store store,
            CommonCommunications<BootstrapService, Bootstrapping> comms, Context<Member> context,
            double falsePositiveRate, DigestAlgorithm digestAlgorithm, Executor executor) {
        this.height = height;
        this.member = member;
        this.checkpoint = checkpoint;
        this.comms = comms;
        this.context = context;
        this.fpr = falsePositiveRate;
        this.digestAlgorithm = digestAlgorithm;
        state = store.createCheckpoint(height);
        checkpoint.getSegmentsList().stream().map(bs -> new Digest(bs)).forEach(hash -> hashes.add(hash));
        this.executor = executor;
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

    private ListenableFuture<CheckpointSegments> gossip(BootstrapService link) {
        if (member.equals(link.getMember())) {
            log.info("Ignoring loopback checkpoint assembly gossip on: {}", link.getMember(), member);
            return null;
        }
        log.info("Checkpoint assembly gossip with: {} on: {}", link.getMember(), member);
        return link.fetch(buildRequest());
    }

    private boolean gossip(Optional<ListenableFuture<CheckpointSegments>> futureSailor) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        try {
            if (process(futureSailor.get().get())) {
                CheckpointState cs = new CheckpointState(checkpoint, state);
                log.info("Assembled checkpoint: {} segments: {} on: {}", height, checkpoint.getSegmentsCount(), member);
                assembled.complete(cs);
                return false;
            }
        } catch (InterruptedException e) {
            log.trace("Failed to retrieve checkpoint {} segments from {} on: {}", height, member, e);
        } catch (ExecutionException e) {
            log.trace("Failed to retrieve checkpoint {} segments from {} on: {}", height, member, e.getCause());
        }
        return true;
    }

    private void gossip(ScheduledExecutorService scheduler, Duration duration) {
        if (assembled.isDone()) {
            return;
        }
        log.info("Scheduling assembly of checkpoint: {} segments: {} on: {}", height, checkpoint.getSegmentsCount(),
                 member);
        RingIterator<BootstrapService> ringer = new RingIterator<>(context, member, comms, executor);
        ringer.iterate(randomCut(digestAlgorithm), (link, ring) -> gossip(link),
                       (tally, futureSailor, link, ring) -> gossip(futureSailor),
                       () -> scheduler.schedule(() -> gossip(scheduler, duration), duration.toMillis(),
                                                TimeUnit.MILLISECONDS));

    }

    private boolean process(CheckpointSegments segments) {
        segments.getSegmentsList().forEach(segment -> {
            Digest hash = digestAlgorithm.digest(segment.getBlock());
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
