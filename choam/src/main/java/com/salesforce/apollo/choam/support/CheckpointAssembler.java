/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.salesfoce.apollo.choam.proto.Checkpoint;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.ring.RingIterator;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.bloomFilters.BloomFilter;
import org.h2.mvstore.MVMap;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.salesforce.apollo.choam.support.Bootstrapper.randomCut;

/**
 * @author hal.hildebrand
 */
public class CheckpointAssembler {
    private static final Logger log = LoggerFactory.getLogger(CheckpointAssembler.class);

    private final CompletableFuture<CheckpointState>        assembled = new CompletableFuture<>();
    private final Checkpoint                                checkpoint;
    private final CommonCommunications<Terminal, Concierge> comms;
    private final Context<Member>                           context;
    private final DigestAlgorithm                           digestAlgorithm;
    private final double                                    fpr;
    private final Duration                                  frequency;
    private final List<Digest>                              hashes    = new ArrayList<>();
    private final ULong                                     height;
    private final SigningMember                             member;
    private final MVMap<Integer, byte[]>                    state;

    public CheckpointAssembler(Duration frequency, ULong height, Checkpoint checkpoint, SigningMember member,
                               Store store, CommonCommunications<Terminal, Concierge> comms, Context<Member> context,
                               double falsePositiveRate, DigestAlgorithm digestAlgorithm) {
        this.height = height;
        this.member = member;
        this.checkpoint = checkpoint;
        this.comms = comms;
        this.context = context;
        this.fpr = falsePositiveRate;
        this.digestAlgorithm = digestAlgorithm;
        this.frequency = frequency;
        state = store.createCheckpoint(height);
        checkpoint.getSegmentsList().stream().map(bs -> new Digest(bs)).forEach(hash -> hashes.add(hash));
    }

    public CompletableFuture<CheckpointState> assemble(ScheduledExecutorService scheduler, Duration duration) {
        if (checkpoint.getSegmentsCount() == 0) {
            log.info("Assembled checkpoint: {} segments: {} on: {}", height, checkpoint.getSegmentsCount(),
                     member.getId());
            assembled.complete(new CheckpointState(checkpoint, state));
        } else {
            gossip(scheduler, duration);
        }
        return assembled;
    }

    private CheckpointReplication buildRequest() {
        long seed = Entropy.nextBitsStreamLong();
        BloomFilter<Integer> segmentsBff = new BloomFilter.IntBloomFilter(seed, checkpoint.getSegmentsCount(), fpr);
        IntStream.range(0, checkpoint.getSegmentsCount()).filter(i -> state.containsKey(i)).forEach(i -> {
            segmentsBff.add(i);
        });
        return CheckpointReplication.newBuilder()
                                    .setCheckpoint(height.longValue())
                                    .setCheckpointSegments(segmentsBff.toBff())
                                    .build();
    }

    private boolean gossip(Optional<CheckpointSegments> futureSailor) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        if (process(futureSailor.get())) {
            CheckpointState cs = new CheckpointState(checkpoint, state);
            log.info("Assembled checkpoint: {} segments: {} on: {}", height, checkpoint.getSegmentsCount(),
                     member.getId());
            assembled.complete(cs);
            return false;
        }
        return true;
    }

    private void gossip(ScheduledExecutorService scheduler, Duration duration) {
        if (assembled.isDone()) {
            return;
        }
        log.info("Assembly of checkpoint: {} segments: {} on: {}", height, checkpoint.getSegmentsCount(),
                 member.getId());
        var ringer = new RingIterator<>(frequency, context, member, comms, true, scheduler);
        ringer.iterate(randomCut(digestAlgorithm), (link, ring) -> gossip(link),
                       (tally, result, destination) -> gossip(result),
                       t -> scheduler.schedule(() -> gossip(scheduler, duration), duration.toMillis(),
                                               TimeUnit.MILLISECONDS));

    }

    private CheckpointSegments gossip(Terminal link) {
        if (member.equals(link.getMember())) {
            log.trace("Ignoring loopback checkpoint assembly gossip on: {}", link.getMember(), member.getId());
            return null;
        }
        log.debug("Checkpoint assembly gossip with: {} on: {}", link.getMember(), member.getId());
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
