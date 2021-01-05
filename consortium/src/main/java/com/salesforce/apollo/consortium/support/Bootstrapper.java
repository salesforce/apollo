/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.Store;
import com.salesforce.apollo.consortium.comms.ConsortiumClientCommunications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class Bootstrapper {
    private static final Logger                                                 log = LoggerFactory.getLogger(Bootstrapper.class);
    private final Checkpoint                                                    checkpoint;
    private final CommonCommunications<ConsortiumClientCommunications, Service> comms;
    private final Context<Member>                                               context;
    private final Member                                                        member;
    private final int                                                           sampleSize;
    private final MVMap<Integer, byte[]>                                        state;
    private final Store                                                         store;
    private final double                                                        fpr;
    private final int                                                           checkpointSpan;

    public Bootstrapper(Member member, int sampleSize, Checkpoint checkpoint, Store store,
            CommonCommunications<ConsortiumClientCommunications, Service> comms, int checkpointSpan,
            Context<Member> context, double falsePositiveRate) {
        this.member = member;
        this.checkpoint = checkpoint;
        this.store = store;
        this.comms = comms;
        this.context = context;
        this.sampleSize = sampleSize;
        this.fpr = falsePositiveRate;
        state = store.createCheckpoint(checkpoint.getCheckpoint());
        this.checkpointSpan = checkpointSpan;
    }

    public void assemble(ScheduledExecutorService scheduler, Duration duration, SecureRandom entropy) {
        scheduler.schedule(() -> gossip(scheduler, duration, entropy), duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void gossip(ScheduledExecutorService scheduler, Duration gossipDuration, SecureRandom entropy) {
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
            baos = new ByteArrayOutputStream();
            try {
                blocksBff.writeTo(baos);
            } catch (IOException e) {
                log.error("Error serializing BFF", e);
                continue;
            }
            request.setBlocks(ByteString.copyFrom(baos.toByteArray()));

            link.fetch(request.build());
        }
    }
}
