/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;
import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.avalanche.Processor.TimedProcessor;
import com.salesforce.apollo.avalanche.WorkingSet.KnownNode;
import com.salesforce.apollo.avalanche.WorkingSet.NoOpNode;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 * @since 222
 */
abstract public class AvalancheFunctionalTest {

    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static final FirefliesParameters              parameters;
    private static final int                              CARDINALITY = 100;

    static {
        parameters = FirefliesParameters.newBuilder()
                                        .setCardinality(CARDINALITY)
                                        .setCertificateValidator(CertificateValidator.NONE)
                                        .build();
    }

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(0, CARDINALITY)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    protected File                baseDir;
    protected MetricRegistry      registry;
    protected BitsStreamGenerator entropy        = new MersenneTwister();
    protected List<Node>          members;
    private Map<Digest, Router>   communications = new HashMap<>();
    protected MetricRegistry      node0registry;

    @AfterEach
    public void after() {
        communications.values().forEach(e -> e.close());
        communications.clear();
    }

    @BeforeEach
    public void before() {
        baseDir = new File(System.getProperty("user.dir"), "target/cluster");
        Utils.clean(baseDir);
        baseDir.mkdirs();
        node0registry = new MetricRegistry();
        registry = new MetricRegistry();

        int testCardinality = testCardinality();
        members = new ArrayList<>();
        for (CertificateWithPrivateKey cert : certs.values()) {
            if (members.size() < testCardinality) {
                members.add(new Node(new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                        cert.getX509Certificate(), cert.getPrivateKey(), new Signer(0, cert.getPrivateKey()),
                        cert.getX509Certificate().getPublicKey()), parameters));
            } else {
                break;
            }
        }

        assertEquals(testCardinality, members.size());

        System.out.println("Test cardinality: " + testCardinality);
        boolean first = true;
        Executor serverThreads = ForkJoinPool.commonPool();
        for (Node node : members) {
            communications.put(node.getId(), getCommunications(node, first, serverThreads));
            first = false;
        }
    }

    protected abstract int testCardinality();

    @Test
    public void smoke() throws Exception {
        Digest vid = DigestAlgorithm.DEFAULT.getOrigin().prefix(1, 2, 3);
        Context<Node> context = new Context<>(vid, 9);
        members.forEach(n -> context.activate(n));
        AtomicBoolean frist = new AtomicBoolean(true);
        ForkJoinPool fjPool = ForkJoinPool.commonPool();
        List<TimedProcessor> processors = members.stream().map(m -> {
            return createAva(m, context, frist, fjPool);
        }).collect(Collectors.toList());

        // # of txns per node
        int target = 1_000;
        int outstanding = 100;

        ScheduledExecutorService avaScheduler = Executors.newScheduledThreadPool(2);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

        communications.values().forEach(e -> e.start());
        processors.forEach(p -> p.getAvalanche().start(avaScheduler, Duration.ofMillis(50)));

        // generate the genesis transaction
        TimedProcessor master = processors.get(0);
        CompletableFuture<Digest> genesis = master.createGenesis(ByteMessage.newBuilder()
                                                                            .setContents(ByteString.copyFromUtf8("Genesis"))
                                                                            .build(),
                                                                 Duration.ofSeconds(90), scheduler);
        Digest genesisKey = null;
        try {
            genesisKey = genesis.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            processors.forEach(p -> p.getAvalanche().stop());
            communications.values().forEach(c -> c.close());
            fail("Genesis timeout");

            // Graphviz.fromGraph(DagViz.visualize("smoke", nodes.get(0).getDag(),
            // false)).render(Format.PNG).toFile(new
            // File("smoke.png"));
        }
        Digest k = genesisKey;
        processors.stream().map(e -> e.getAvalanche()).forEach(a -> {
            assertTrue(Utils.waitForCondition(5_000, () -> a.getDagDao().isFinalized(k)),
                       "Failed to finalize genesis on: " + a.getNode().getId());
        });
        assertNotNull(genesisKey);

        seed(processors);

        long now = System.currentTimeMillis();

        CountDownLatch latch = new CountDownLatch(processors.size());
        List<Transactioneer> transactioneers = processors.stream()
                                                         .map(a -> new Transactioneer(a, target, latch))
                                                         .collect(Collectors.toList());

        transactioneers.stream().forEach(t -> t.transact(Duration.ofSeconds(120), outstanding, scheduler));

        boolean finalized = latch.await(360, TimeUnit.SECONDS);

        long duration = System.currentTimeMillis() - now;
        transactioneers.forEach(t -> t.stop());
        processors.forEach(p -> p.getAvalanche().stop());
        Thread.sleep(2_000); // drain the swamp

        System.out.println();
        System.out.println("Node 0 Metrics");
        ConsoleReporter.forRegistry(node0registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();

        System.out.println();
        System.out.println();
        System.out.println("Completed in " + duration + " ms");
        System.out.println("Global tps: "
                + transactioneers.stream().mapToInt(e -> e.getSuccess()).sum() / (duration / 1000));

        System.out.println("Max tps per node: "
                + processors.stream().mapToInt(p -> p.getAvalanche().getDag().getFinalizedCount()).max().orElse(0)
                        / (duration / 1000));
        processors.forEach(p -> summary(p.getAvalanche()));

        System.out.println("wanted: ");
        System.out.println(master.getAvalanche().getDag().getWanted(entropy, 100_000));
        System.out.println();
        transactioneers.forEach(t -> {
            System.out.println("finalized " + t.getSuccess() + " and failed to finalize " + t.getFailed() + " for "
                    + t.getId());
        });

        // System.out.println();
        // System.out.println("Comm Metrics");
        // ConsoleReporter.forRegistry(commRegistry)
        // .convertRatesTo(TimeUnit.SECONDS)
        // .convertDurationsTo(TimeUnit.MILLISECONDS)
        // .build()
        // .report();
//        FileSerializer.serialize(DagViz.visualize("smoke", processors.get(0).getAvalanche().getDag(), true),
//                                 new File("smoke.dot"));
//
//        Graphviz.fromGraph(DagViz.visualize("smoke", processors.get(0).getAvalanche().getDag(), true))
//                .render(Format.PNG)
//                .toFile(new File("smoke.png"));
        assertTrue(finalized, "failed to finalize " + target + " txns: " + transactioneers);
    }

    private TimedProcessor createAva(Node m, Context<Node> context, AtomicBoolean frist, ForkJoinPool fjPool) {
        AvalancheParameters aParams = new AvalancheParameters();

        // Avalanche protocol parameters
        aParams.core.alpha = 0.6;
        aParams.core.k = 10;
        aParams.core.beta1 = 3;
        aParams.core.beta2 = 5;

        // Avalanche implementation parameters
        // parent selection target for avalanche dag voting
        aParams.parentCount = 3;
        aParams.queryBatchSize = 400;
        aParams.noOpsPerRound = 2;
        aParams.maxNoOpParents = 50;
        aParams.outstandingQueries = 5;
        aParams.noOpQueryFactor = 20;

        AvaMetrics avaMetrics = new AvaMetrics(frist.get() ? node0registry : registry);
        frist.set(false);
        TimedProcessor processor = new TimedProcessor();
        MVStore s = new MVStore.Builder().open();
        Avalanche avalanche = new Avalanche(m, context, communications.get(m.getId()), aParams, avaMetrics, processor,
                s, fjPool);
        processor.setAvalanche(avalanche);
        return processor;
    }

    abstract protected Router getCommunications(Node node, boolean first, Executor serverThreads);

    private void seed(List<TimedProcessor> nodes) {
        long then = System.currentTimeMillis();
        List<Transactioneer> transactioneers = nodes.stream()
                                                    .map(a -> new Transactioneer(a, 5, null))
                                                    .collect(Collectors.toList());

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        transactioneers.forEach(t -> t.transact(Duration.ofSeconds(120), 2, scheduler));

        boolean seeded = Utils.waitForCondition(10_000, 500, () -> {
            return transactioneers.stream()
                                  .mapToInt(t -> t.getSuccess())
                                  .filter(s -> s >= 2)
                                  .count() == transactioneers.size();
        });
        scheduler.shutdown();

        assertTrue(seeded, "could not seed initial txn set");
        System.out.println("seeded initial txns in " + (System.currentTimeMillis() - then) + " ms");
    }

    private void summary(Avalanche node) {
        System.out.println(node.getNode().getId() + " : ");

        Integer finalized = node.getDag().getFinalizedCount();
        Integer unfinalizedUser = node.getDag()
                                      .getUnfinalized()
                                      .values()
                                      .stream()
                                      .filter(n -> n instanceof KnownNode)
                                      .mapToInt(n -> n.isFinalized() ? 0 : 1)
                                      .sum();
        long unqueried = node.getDag()
                             .getUnqueried()
                             .stream()
                             .map(key -> node.getDag().get(key))
                             .filter(n -> n instanceof KnownNode)
                             .count();

        System.out.println("    User txns finalized: " + finalized + " unfinalized: " + unfinalizedUser + " unqueried: "
                + unqueried);
        System.out.println("    No Op txns: "
                + node.getDag().getUnfinalized().values().stream().filter(n -> n instanceof NoOpNode).count());
    }
}
