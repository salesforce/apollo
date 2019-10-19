/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.dagwood.schema.Tables.DAG;
import static com.salesforce.apollo.dagwood.schema.Tables.UNQUERIED;
import static com.salesforce.apollo.fireflies.PregenPopulation.getCa;
import static com.salesforce.apollo.fireflies.PregenPopulation.getMember;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.avalanche.communications.AvalancheCommunications;
import com.salesforce.apollo.avalanche.communications.AvalancheLocalCommSim;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.fireflies.CertWithKey;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.communications.FfLocalCommSim;
import com.salesforce.apollo.fireflies.stats.DropWizardStatsPlugin;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 * @since 222
 */
public class AvalancheFunctionalTest {

    private static final RootCertificate ca = getCa();
    private static Map<UUID, CertWithKey> certs;
    private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

    @BeforeClass
    public static void beforeClass() {
        certs = IntStream.range(1, 14)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberId(cert.getCertificate()),
                                                   cert -> cert));
    }

    private Random entropy;
    private List<Node> members;
    private ScheduledExecutorService scheduler;
    private List<X509Certificate> seeds;
    private List<View> views;
    private DropWizardStatsPlugin rpcStats;
    private MetricRegistry commRegistry;
    private MetricRegistry node0Registry;

    @After
    public void after() {
        views.forEach(e -> e.getService().stop());
    }

    @Before
    public void before() {
        commRegistry = new MetricRegistry();
        node0Registry = new MetricRegistry();
        rpcStats = new DropWizardStatsPlugin(commRegistry);
        entropy = new Random(0x666);

        seeds = new ArrayList<>();
        members = certs.values().parallelStream().map(cert -> new Node(cert, parameters)).collect(Collectors.toList());
        FfLocalCommSim ffComms = new FfLocalCommSim(rpcStats);
        assertEquals(certs.size(), members.size());

        while (seeds.size() < Math.min(parameters.toleranceLevel + 1, certs.size())) {
            CertWithKey cert = certs.get(members.get(entropy.nextInt(certs.size())).getId());
            if (!seeds.contains(cert.getCertificate())) {
                seeds.add(cert.getCertificate());
            }
        }

        System.out.println("Seeds: " + seeds.stream().map(e -> Member.getMemberId(e)).collect(Collectors.toList()));
        scheduler = Executors.newScheduledThreadPool(members.size());

        views = members.stream().map(node -> new View(node, ffComms, seeds, scheduler)).collect(Collectors.toList());
    }

    @Test
    public void smoke() throws Exception {
        File baseDir = new File(System.getProperty("user.dir"), "target/cluster");
        baseDir.mkdirs();
        AvaMetrics avaMetrics = new AvaMetrics(node0Registry);
        AvalancheCommunications comm = new AvalancheLocalCommSim(rpcStats);
        AtomicInteger index = new AtomicInteger(0);
        AtomicBoolean frist = new AtomicBoolean(true);
        List<Avalanche> nodes = views.stream().map(view -> {
            AvalancheParameters aParams = new AvalancheParameters();
            // Avalanche protocol parameters
            aParams.alpha = 0.6;
            aParams.k = 3;
            aParams.beta1 = 3;
            aParams.beta2 = 5;
            // parent selection target for avalanche dag voting
            aParams.parentCount = 3;

            // Avalanche implementation parameters
            aParams.queryBatchSize = 100;
            aParams.insertBatchSize = 800;
            aParams.preferBatchSize = 100;
            aParams.finalizeBatchSize = 100;
            aParams.noOpsPerRound = 1;
            aParams.maxNoOpParents = 100;
            aParams.maxQueries = 100;

            // # of firefly rounds per avalanche round
            aParams.epsilon = 1;
            // # of FF rounds per NoOp generation
            aParams.delta = 3;
            // # of Avalanche queries per FF round
            aParams.gamma = 10;

            aParams.dbConnect = "jdbc:h2:mem:test-" + index.getAndIncrement()
                    + ";LOCK_MODE=0;EARLY_FILTER=TRUE;MULTI_THREADED=1;MVCC=TRUE";
            if (frist.get()) {
                frist.set(false);
                return new Avalanche(view, comm, aParams, avaMetrics);
            }
            return new Avalanche(view, comm, aParams);
        }).collect(Collectors.toList());

        // # of txns per node
        int target = 400;

        views.forEach(view -> view.getService().start(Duration.ofMillis(500)));

        assertTrue("Could not stabilize view membership)", Utils.waitForCondition(30_000, 1_000, () -> {
            return views.stream()
                        .map(view -> view.getLive().size() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }));
        nodes.forEach(node -> node.start());
        ScheduledExecutorService txnScheduler = Executors.newScheduledThreadPool(nodes.size());
        // Profiler profiler = new Profiler();
        // profiler.startCollecting();

        // generate the genesis transaction
        Avalanche master = nodes.get(0);
        System.out.println("Start round: " + master.getRoundCounter());
        CompletableFuture<HashKey> genesis = nodes.get(0)
                                                  .createGenesis("Genesis".getBytes(), Duration.ofSeconds(90),
                                                                 txnScheduler);
        HashKey genesisKey = null;
        try {
            genesisKey = genesis.get(60, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            nodes.forEach(node -> node.stop());
            views.forEach(v -> v.getService().stop());

            System.out.println("Rounds: " + master.getRoundCounter());
            // Graphviz.fromGraph(DagViz.visualize("smoke", master.getDslContext(), false))
            // .render(Format.PNG)
            // .toFile(new File("smoke.png"));

            System.out.println("wanted: ");
            System.out.println(master.getDag()
                                     .getWanted(Integer.MAX_VALUE, master.getDslContext())
                                     .stream()
                                     .map(h -> new HashKey(h))
                                     .collect(Collectors.toList()));
        }
        System.out.println("Rounds: " + master.getRoundCounter());
        assertNotNull(genesisKey);

        long now = System.currentTimeMillis();
        List<Transactioneer> transactioneers = new ArrayList<>();
        HASH k = genesisKey.toHash();
        for (Avalanche a : nodes) {
            assertTrue("Failed to finalize genesis on: " + a.getNode().getId(),
                       Utils.waitForCondition(60_000, () -> a.getDagDao().isFinalized(k)));
            transactioneers.add(new Transactioneer(a));
        }

        transactioneers.forEach(t -> t.transact(Duration.ofSeconds(120), target * 40, txnScheduler));

        boolean finalized = Utils.waitForCondition(600_000, 1_000, () -> {
            return transactioneers.stream()
                                  .mapToInt(t -> t.getSuccess())
                                  .filter(s -> s >= target)
                                  .count() == transactioneers.size();
        });

        long duration = System.currentTimeMillis() - now;

        System.out.println("Completed in " + duration + " ms");
        transactioneers.forEach(t -> t.stop());
        views.forEach(v -> v.getService().stop());
        nodes.forEach(node -> node.stop());
        // System.out.println(profiler.getTop(3));

        System.out.println("Max tps: " + nodes.stream()
                                              .mapToInt(n -> n.getDslContext()
                                                              .selectCount()
                                                              .from(DAG)
                                                              .where(DAG.NOOP.isFalse())
                                                              .and(DAG.FINALIZED.isTrue())
                                                              .fetchOne()
                                                              .value1())
                                              .max()
                                              .orElse(0)
                / (duration / 1000));
        nodes.forEach(node -> summary(node));

        // Graphviz.fromGraph(DagViz.visualize("smoke", master.getDslContext(), true))
        // .render(Format.XDOT)
        // .toFile(new File("smoke.dot"));

        System.out.println("wanted: ");
        System.out.println(master.getDag()
                                 .getWantedSlow(Integer.MAX_VALUE, master.getDslContext())
                                 .stream()
                                 .map(e -> new HashKey(e))
                                 .collect(Collectors.toList()));
        System.out.println();
        System.out.println("Node 0 Metrics");
        ConsoleReporter.forRegistry(node0Registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
        System.out.println();
        // System.out.println("Comm Metrics");
        // ConsoleReporter.forRegistry(metricsRegistry)
        // .convertRatesTo(TimeUnit.SECONDS)
        // .convertDurationsTo(TimeUnit.MILLISECONDS)
        // .build()
        // .report();
        assertTrue("failed to finalize " + target + " txns: " + transactioneers, finalized);
        transactioneers.forEach(t -> {
            System.out.println("failed to finalize " + t.getFailed() + " for " + t.getId());
        });
    }

    private void summary(Avalanche node) {
        System.out.println(node.getNode().getId() + " : ");
        System.out.println("    Rounds: " + node.getRoundCounter());
        System.out.println("    User txns: "
                + node.getDslContext().selectCount().from(DAG).where(DAG.NOOP.isFalse()).fetchOne().value1()
                + " finalized: "
                + node.getDslContext()
                      .selectCount()
                      .from(DAG)
                      .where(DAG.NOOP.isFalse())
                      .and(DAG.FINALIZED.isTrue())
                      .fetchOne()
                      .value1()
                + " unqueried: "
                + node.getDslContext()
                      .selectCount()
                      .from(DAG)
                      .join(UNQUERIED)
                      .on(UNQUERIED.HASH.eq(DAG.HASH))
                      .where(DAG.NOOP.isFalse())
                      .fetchOne()
                      .value1());
        System.out.println("    No Op txns: "
                + node.getDslContext().selectCount().from(DAG).where(DAG.NOOP.isTrue()).fetchOne().value1()
                + " finalized: "
                + node.getDslContext()
                      .selectCount()
                      .from(DAG)
                      .where(DAG.NOOP.isTrue())
                      .and(DAG.FINALIZED.isTrue())
                      .fetchOne()
                      .value1()
                + " unqueried: "
                + node.getDslContext()
                      .selectCount()
                      .from(DAG)
                      .join(UNQUERIED)
                      .on(UNQUERIED.HASH.eq(DAG.HASH))
                      .where(DAG.NOOP.isTrue())
                      .fetchOne()
                      .value1());
    }
}
