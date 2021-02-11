/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
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

    private final CompletableFuture<File>                                       assembled = new CompletableFuture<>();
    private final Checkpoint                                                    checkpoint;
    private final File                                                          checkpointDirectory;
    private final int                                                           checkpointSpan;
    private final CommonCommunications<ConsortiumClientCommunications, Service> comms;
    private final Context<Member>                                               context;
    private final double                                                        fpr;
    private final List<HashKey>                                                 hashes    = new ArrayList<>();
    private final Member                                                        member;
    private final int                                                           sampleSize;
    private final MVMap<Integer, byte[]>                                        state;
    private final Store                                                         store;

    public Bootstrapper(Member member, int sampleSize, Checkpoint checkpoint, Store store,
            CommonCommunications<ConsortiumClientCommunications, Service> comms, int checkpointSpan,
            Context<Member> context, double falsePositiveRate, File checkpointDirectory) {
        this.member = member;
        this.checkpoint = checkpoint;
        this.store = store;
        this.comms = comms;
        this.context = context;
        this.sampleSize = sampleSize;
        this.fpr = falsePositiveRate;
        state = store.createCheckpoint(checkpoint.getCheckpoint());
        this.checkpointSpan = checkpointSpan;
        checkpoint.getSegmentsList().stream().map(bs -> new HashKey(bs)).forEach(hash -> hashes.add(hash));
        this.checkpointDirectory = checkpointDirectory;
    }

    public CompletableFuture<File> assemble(ScheduledExecutorService scheduler, Duration duration,
                                            BitsStreamGenerator entropy) {
        scheduler.schedule(() -> gossip(scheduler, duration, entropy), duration.toMillis(), TimeUnit.MILLISECONDS);
        return assembled;
    }

    private void create(File file) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            for (byte[] bytes : IntStream.range(0, hashes.size())
                                         .mapToObj(hk -> state.get(hk))
                                         .collect(Collectors.toList())) {
                fos.write(bytes);
            }
            fos.flush();
        }
    }

    private void gossip(ScheduledExecutorService scheduler, Duration duration, BitsStreamGenerator entropy) {
        for (Member m : context.sample(sampleSize, entropy, member.getId())) {
            ConsortiumClientCommunications link = comms.apply(m, member);
            CheckpointReplication.Builder request = CheckpointReplication.newBuilder()
                                                                         .setContext(context.getId().toByteString())
                                                                         .setCheckpoint(checkpoint.getCheckpoint());
            BloomFilter<Integer> segmentsBff = BloomFilter.create(Funnels.integerFunnel(),
                                                                  checkpoint.getSegmentsCount(), fpr);
            IntStream.range(0, checkpoint.getSegmentsCount())
                     .filter(i -> !state.containsKey(i))
                     .forEach(i -> segmentsBff.put(i));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                segmentsBff.writeTo(baos);
            } catch (IOException e) {
                log.error("Error serializing BFF", e);
                continue;
            }
            request.setCheckpointSegments(ByteString.copyFrom(baos.toByteArray()));

            BloomFilter<Long> blocksBff = BloomFilter.create(Funnels.longFunnel(), checkpointSpan, fpr);
            LongStream.range(checkpoint.getCheckpoint() + 1, checkpoint.getSegmentsCount())
                      .filter(l -> !store.containsBlock(l))
                      .forEach(l -> blocksBff.put(l));
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try {
                blocksBff.writeTo(bytes);
            } catch (IOException e) {
                log.error("Error serializing BFF", e);
                continue;
            }
            request.setBlocks(ByteString.copyFrom(bytes.toByteArray()));
            boolean complete = false;
            try {
                complete = process(link.fetch(request.build()));
            } catch (Throwable t) {
                log.debug("Unable to fetch from: {} on: {}", m, member);
            }
            if (complete) {
                try {
                    File spFile = File.createTempFile("snapshot-" + checkpoint.getCheckpoint() + "-", "zip",
                                                      checkpointDirectory);
                    create(spFile);
                    assembled.complete(spFile);
                } catch (IOException e) {
                    log.info("Failed to assemble checkpoint {} on: {}", checkpoint.getCheckpoint(), member, e);
                    assembled.completeExceptionally(e);
                }
                return;
            }
        }
        scheduler.schedule(() -> gossip(scheduler, duration, entropy), duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private boolean process(CheckpointSegments segments) {
        segments.getSegmentsList().forEach(segment -> {
            HashKey hash = new HashKey(Conversion.hashOf(segment.getBlock()));
            int index = segment.getIndex();
            if (index <= 0 || index >= hashes.size()) {

            }
            if (hash.equals(hashes.get(index))) {
                state.put(index, segment.getBlock().toByteArray());
            }
        });
        return state.size() == hashes.size();
    }
}
