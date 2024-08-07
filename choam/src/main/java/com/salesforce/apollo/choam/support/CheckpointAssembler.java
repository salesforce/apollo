/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.bloomFilters.BloomFilter;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.proto.Checkpoint;
import com.salesforce.apollo.choam.proto.CheckpointReplication;
import com.salesforce.apollo.choam.proto.CheckpointSegments;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.HexBloom;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.ring.SliceIterator;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
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
    private final ULong                                     height;
    private final SigningMember                             member;
    private final MVMap<Integer, byte[]>                    state;
    private final HexBloom                                  diadem;
    private final List<Member>                              committee;

    public CheckpointAssembler(List<Member> committee, Duration frequency, ULong height, Checkpoint checkpoint,
                               SigningMember member, Store store, CommonCommunications<Terminal, Concierge> comms,
                               Context<Member> context, double falsePositiveRate, DigestAlgorithm digestAlgorithm) {
        this.committee = new ArrayList<>(committee);
        this.height = height;
        this.member = member;
        this.checkpoint = checkpoint;
        this.comms = comms;
        this.context = context;
        this.fpr = falsePositiveRate;
        this.digestAlgorithm = digestAlgorithm;
        this.frequency = frequency;
        state = store.createCheckpoint(height);
        diadem = HexBloom.from(checkpoint.getCrown());
    }

    public CompletableFuture<CheckpointState> assemble(ScheduledExecutorService scheduler, Duration duration) {
        if (checkpoint.getCount() == 0) {
            assembled(new CheckpointState(checkpoint, state));
        } else {
            gossip(scheduler, duration);
        }
        return assembled;
    }

    private void assembled(CheckpointState cs) {
        log.info("Assembled checkpoint: {} segments: {} crown: {} on: {}", height, checkpoint.getCount(),
                 diadem.compactWrapped(), member.getId());
        assembled.complete(cs);
    }

    private CheckpointReplication buildRequest() {
        long seed = Entropy.nextBitsStreamLong();
        BloomFilter<Integer> segmentsBff = new BloomFilter.IntBloomFilter(seed, checkpoint.getCount(), fpr);
        IntStream.range(0, checkpoint.getCount()).filter(i -> state.containsKey(i)).forEach(i -> {
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
            assembled(cs);
            return false;
        }
        return true;
    }

    private void gossip(ScheduledExecutorService scheduler, Duration duration) {
        if (assembled.isDone()) {
            return;
        }
        log.info("Assembly of checkpoint: {} segments: {} crown: {} on: {}", height, checkpoint.getCount(),
                 diadem.compactWrapped(), member.getId());

        var ringer = new SliceIterator<>("Assembly[%s:%s]".formatted(diadem.compactWrapped(), member.getId()), member,
                                         committee, comms, scheduler);
        ringer.iterate((link) -> {
            log.debug("Requesting Seeding from: {} on: {}", link.getMember().getId(), member.getId());
            return gossip(link);
        }, (result, _, _, _) -> gossip(result), () -> {
            if (!assembled.isDone()) {
                scheduler.schedule(
                () -> Thread.ofVirtual().start(Utils.wrapped(() -> gossip(scheduler, duration), log)),
                duration.toMillis(), TimeUnit.MILLISECONDS);
            }
        }, duration);

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
            if (diadem.contains(hash)) {
                state.computeIfAbsent(segment.getIndex(), i -> segment.getBlock().toByteArray());
            }
        });
        return state.size() == checkpoint.getCount();
    }
}
