/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.Store;
import com.salesforce.apollo.consortium.comms.ConsortiumClient;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Pair;

/**
 * @author hal.hildebrand
 *
 */
public class Bootstrapper {
    private static final Logger log = LoggerFactory.getLogger(Bootstrapper.class);

    private final CompletableFuture<Pair<CertifiedBlock, CertifiedBlock>>       sync = new CompletableFuture<>();
    private final CommonCommunications<ConsortiumClient, Service> comms;
    private final Context<Member>                                               context;
    private final double                                                        fpr;
    private final Member                                                        member;
    private final Store                                                         store;

    public Bootstrapper(Member member, CommonCommunications<ConsortiumClient, Service> comms,
            Context<Member> context, double falsePositiveRate, Store store) {
        this.comms = comms;
        this.context = context;
        this.fpr = falsePositiveRate;
        this.member = member;
        this.store = store;
    }

    public CompletableFuture<Pair<CertifiedBlock, CertifiedBlock>> synchronize(ScheduledExecutorService scheduler,
                                                                               Duration duration,
                                                                               BitsStreamGenerator entropy) {
        gossip(scheduler, duration, entropy);
        return sync;
    }

    private Runnable gossip(ConsortiumClient link, ListenableFuture<CheckpointSegments> futureSailor,
                            BitsStreamGenerator entropy, Runnable scheduler) {
        return () -> {
            link.release();

            try {
                if (process(futureSailor.get())) {
                    CheckpointState cs = new CheckpointState(checkpoint, state);
                    sync.complete(cs);
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
        if (sync.isDone()) {
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
        ConsortiumClient link = comms.apply(m, member);
        if (link == null) {
            log.trace("No link for: {} on: {}", m, member);
            s.run();
            return;
        }

        link.fetch(request).addListener(gossip(link, link.fetch(request), entropy, s), ForkJoinPool.commonPool());

    }

}
