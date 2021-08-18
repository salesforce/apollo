/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class TestCHOAM {
    private static final int CARDINALITY = 51;

    private Map<Digest, CHOAM>             choams;
    private List<SigningMember>            members;
    private Map<Digest, Router>            routers;
    private Map<Digest, List<HashedBlock>> blocks;

    @AfterEach
    public void after() {
        if (routers != null) {
            routers.values().forEach(e -> e.close());
            routers = null;
        }
        if (choams != null) {
            choams.values().forEach(e -> e.stop());
            choams = null;
        }
        members = null;
    }

    @BeforeEach
    public void before() {
        blocks = new ConcurrentHashMap<>();
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 0.33, CARDINALITY);
        Parameters.Builder params = Parameters.newBuilder().setContext(context)
                                              .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin().prefix(0x1638))
                                              .setGossipDuration(Duration.ofMillis(20))
                                              .setScheduler(Executors.newScheduledThreadPool(CARDINALITY));
//        params.getCoordination().setBufferSize(1500);
//        params.getCombineParams().setBufferSize(1500);
        members = IntStream.range(0, CARDINALITY).mapToObj(i -> Utils.getMember(i))
                           .map(cpk -> new SigningMemberImpl(cpk)).map(e -> (SigningMember) e)
                           .peek(m -> context.activate(m)).toList();
        routers = members.stream()
                         .collect(Collectors.toMap(m -> m.getId(),
                                                   m -> new LocalRouter(m,
                                                                        ServerConnectionCache.newBuilder()
                                                                                             .setMetrics(params.getMetrics()),
                                                                        ForkJoinPool.commonPool())));
        choams = members.stream()
                        .collect(Collectors.toMap(m -> m.getId(),
                                                  m -> new CHOAM(params.setMember(m)
                                                                       .setCommunications(routers.get(m.getId()))
                                                                       .setProcessor(b -> blocks.computeIfAbsent(m.getId(),
                                                                                                                 d -> new CopyOnWriteArrayList<>())
                                                                                                .add(b))
                                                                       .build(),
                                                                 MVStore.open(null))));
    }

    @Test
    public void regenerateGenesis() throws Exception {
        routers.values().forEach(r -> r.start());
        choams.values().forEach(ch -> ch.start());
        final int expected = 88 + (30 * 3);
        Utils.waitForCondition(120_000, () -> blocks.values().stream().mapToInt(l -> l.size())
                                                    .filter(s -> s >= expected).count() == choams.size());
        assertEquals(choams.size(), blocks.values().stream().mapToInt(l -> l.size()).filter(s -> s >= expected).count(),
                     "Failed: " + blocks.get(members.get(0).getId()).size());
    }
}
