/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.ExecutedTransaction;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
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

    private Map<Digest, CHOAM>                     choams;
    private List<SigningMember>                    members;
    private Map<Digest, Router>                    routers;
    private Map<Digest, List<ExecutedTransaction>> transactions;
    private Map<Digest, List<Digest>>              blocks;

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
        transactions = new ConcurrentHashMap<>();
        blocks = new ConcurrentHashMap<>();
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 0.33, CARDINALITY);
        Parameters.Builder params = Parameters.newBuilder().setContext(context)
                                              .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin().prefix(0x1638))
                                              .setGossipDuration(Duration.ofMillis(20))
                                              .setScheduler(Executors.newScheduledThreadPool(CARDINALITY));

        members = IntStream.range(0, CARDINALITY).mapToObj(i -> Utils.getMember(i))
                           .map(cpk -> new SigningMemberImpl(cpk)).map(e -> (SigningMember) e)
                           .peek(m -> context.activate(m)).toList();
        routers = members.stream()
                         .collect(Collectors.toMap(m -> m.getId(),
                                                   m -> new LocalRouter(m,
                                                                        ServerConnectionCache.newBuilder()
                                                                                             .setMetrics(params.getMetrics()),
                                                                        ForkJoinPool.commonPool())));
        choams = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            final TransactionExecutor processor = new TransactionExecutor() {

                @Override
                public void beginBlock(long height, Digest hash) {
                    blocks.computeIfAbsent(m.getId(), d -> new CopyOnWriteArrayList<>()).add(hash);
                }

                @SuppressWarnings({ "unchecked", "rawtypes" })
                @Override
                public void execute(ExecutedTransaction t, CompletableFuture f) {
                    transactions.computeIfAbsent(m.getId(), d -> new CopyOnWriteArrayList<>()).add(t);
                    if (f != null) {
                        f.complete(new Object());
                    }
                }
            };
            return new CHOAM(params.setMember(m).setCommunications(routers.get(m.getId())).setProcessor(processor)
                                   .build(),
                             MVStore.open(null));
        }));
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

    @Test
    public void submitTxn() throws Exception {
        routers.values().forEach(r -> r.start());
        choams.values().forEach(ch -> ch.start());
        final int expected = 88;
        var session = choams.get(members.get(0).getId()).getSession();
        Transaction tx = Transaction.newBuilder()
                                    .setContent(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                    .build();

        Utils.waitForCondition(120_000, () -> blocks.values().stream().mapToInt(l -> l.size())
                                                    .filter(s -> s >= expected).count() == choams.size());
        assertEquals(choams.size(), blocks.values().stream().mapToInt(l -> l.size()).filter(s -> s >= expected).count(),
                     "Failed: " + blocks.get(members.get(0).getId()).size());
        CompletableFuture<?> result = session.submit(tx, null);
        while (true) {
            final var r = result;
            Utils.waitForCondition(1_000, () -> r.isDone());
            if (result.isDone()) {
                if (result.isCompletedExceptionally()) {
                    result.exceptionally(t -> {
                        System.out.println("Failed with: " + t.toString());
                        return null;
                    });
                    result = session.submit(tx, null);
                } else {
                    System.out.println("Success!!!!");
                    var completion = result.get();
                    assertNotNull(completion);
                    break;
                }
            }
        }
    }
}
