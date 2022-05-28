/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import static com.salesforce.apollo.choam.support.Bootstrapper.randomCut;

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
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.choam.proto.Checkpoint;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.comm.RingIterator;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class CheckpointAssembler {
    private static final Logger log = LoggerFactory.getLogger(CheckpointAssembler.class);

    private final CompletableFuture<CheckpointState>        assembled = new CompletableFuture<>();
    private final Checkpoint                                checkpoint;
    private final CommonCommunications<Terminal, Concierge> comms;
    private final Context<Member>                           context;
    private final DigestAlgorithm                           digestAlgorithm;
    private final double                                    fpr;
    private final List<Digest>                              hashes    = new ArrayList<>();
    private final ULong                                     height;
    private final SigningMember                             member;
    private final MVMap<Integer, byte[]>                    state;

    public CheckpointAssembler(ULong height, Checkpoint checkpoint, SigningMember member, Store store,
                               CommonCommunications<Terminal, Concierge> comms, Context<Member> context,
                               double falsePositiveRate, DigestAlgorithm digestAlgorithm) {
        this.height = height;
        this.member = member;
        this.checkpoint = checkpoint;
        this.comms = comms;
        this.context = context;
        this.fpr = falsePositiveRate;
        this.digestAlgorithm = digestAlgorithm;
        state = store.createCheckpoint(height);
        checkpoint.getSegmentsList().stream().map(bs -> new Digest(bs)).forEach(hash -> hashes.add(hash));
    }

    public CompletableFuture<CheckpointState> assemble(ScheduledExecutorService scheduler, Duration duration,
                                                       Executor exec) {
        if (checkpoint.getSegmentsCount() == 0) {
            log.info("Assembled checkpoint: {} segments: {} on: {}", height, checkpoint.getSegmentsCount(), member);
            assembled.complete(new CheckpointState(checkpoint, state));
        } else {
            gossip(scheduler, duration, exec);
        }
        return assembled;
    }

    private CheckpointReplication buildRequest() {
        long seed = Entropy.nextBitsStreamLong();
        BloomFilter<Integer> segmentsBff = new BloomFilter.IntBloomFilter(seed, checkpoint.getSegmentsCount(), fpr);
        IntStream.range(0, checkpoint.getSegmentsCount()).filter(i -> state.containsKey(i)).forEach(i -> {
            segmentsBff.add(i);
        });
        CheckpointReplication.Builder request = CheckpointReplication.newBuilder()
                                                                     .setContext(context.getId().toDigeste())
                                                                     .setCheckpoint(height.longValue());
        request.setCheckpointSegments(segmentsBff.toBff());
        return request.build();
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

    private void gossip(ScheduledExecutorService scheduler, Duration duration, Executor exec) {
        if (assembled.isDone()) {
            return;
        }
        log.info("Assembly of checkpoint: {} segments: {} on: {}", height, checkpoint.getSegmentsCount(), member);
        RingIterator<Terminal> ringer = new RingIterator<>(context, member, comms, exec, true);
        ringer.iterate(randomCut(digestAlgorithm), (link, ring) -> gossip(link),
                       (tally, futureSailor, link, ring) -> gossip(futureSailor),
                       () -> scheduler.schedule(() -> gossip(scheduler, duration, exec), duration.toMillis(),
                                                TimeUnit.MILLISECONDS));

    }

    private ListenableFuture<CheckpointSegments> gossip(Terminal link) {
        if (member.equals(link.getMember())) {
            log.info("Ignoring loopback checkpoint assembly gossip on: {}", link.getMember(), member);
            return null;
        }
        log.debug("Checkpoint assembly gossip with: {} on: {}", link.getMember(), member);
        return link.fetch(buildRequest());
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
