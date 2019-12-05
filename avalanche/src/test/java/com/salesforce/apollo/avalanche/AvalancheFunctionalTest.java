/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.fireflies.PregenPopulation.getCa;
import static com.salesforce.apollo.fireflies.PregenPopulation.getMember;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.salesforce.apollo.avalanche.WorkingSet.KnownNode;
import com.salesforce.apollo.avalanche.WorkingSet.NoOpNode;
import com.salesforce.apollo.avalanche.communications.AvalancheCommunications;
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
abstract public class AvalancheFunctionalTest {

    private static final RootCertificate     ca         = getCa();
    private static Map<UUID, CertWithKey>    certs;
    private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

    @BeforeClass
    public static void beforeClass() {
        certs = IntStream.range(1, 14)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberId(cert.getCertificate()), cert -> cert));
    }

    protected File                     baseDir;
    protected MetricRegistry           commRegistry;
    protected Random                   entropy;
    protected List<Node>               members;
    protected MetricRegistry           node0Registry;
    protected DropWizardStatsPlugin    rpcStats;
    protected ScheduledExecutorService scheduler;
    protected List<X509Certificate>    seeds;
    protected List<View>               views;

    @After
    public void after() {
        views.forEach(e -> e.getService().stop());
    }

    @Before
    public void before() {
        baseDir = new File(System.getProperty("user.dir"), "target/cluster");
        Utils.clean(baseDir);
        baseDir.mkdirs();
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
        AvaMetrics avaMetrics = new AvaMetrics(node0Registry);

        List<Avalanche> nodes = views.stream().map(view -> {
            AvalancheParameters aParams = new AvalancheParameters();
            aParams.dagWood.maxCache = 1_000_000;

            // Avalanche protocol parameters
            aParams.core.alpha = 0.6;
            aParams.core.k = 10;
            aParams.core.beta1 = 3;
            aParams.core.beta2 = 5;

            // Avalanche implementation parameters
            // parent selection target for avalanche dag voting
            aParams.parentCount = 5;
            aParams.queryBatchSize = 400;
            aParams.noOpsPerRound = 10;
            aParams.maxNoOpParents = 10;
            aParams.outstandingQueries = 5;
            aParams.noOpQueryFactor = 40;

            // # of firefly rounds per noOp generation round
            aParams.delta = 1;

            AvalancheCommunications comm = getCommunications();
            return new Avalanche(view, comm, aParams, avaMetrics);
        }).collect(Collectors.toList());

        // # of txns per node
        int target = 4_000;
        Duration ffRound = Duration.ofMillis(500);
        int outstanding = 400;
        int runtime = (int) Duration.ofSeconds(100).toMillis();

        views.parallelStream().forEach(view -> view.getService().start(ffRound));

        assertTrue("Could not stabilize view membership)", Utils.waitForCondition(30_000, 3_000, () -> {
            return views.stream()
                        .map(view -> view.getLive().size() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }));
        nodes.forEach(node -> node.start());
        ScheduledExecutorService txnScheduler = Executors.newScheduledThreadPool(nodes.size());

        // generate the genesis transaction
        Avalanche master = nodes.get(0);
        CompletableFuture<HashKey> genesis = nodes.get(0)
                                                  .createGenesis("Genesis".getBytes(), Duration.ofSeconds(90),
                                                                 txnScheduler);
        HashKey genesisKey = null;
        try {
            genesisKey = genesis.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            nodes.forEach(node -> node.stop());
            views.forEach(v -> v.getService().stop());

//            Graphviz.fromGraph(DagViz.visualize("smoke", nodes.get(0).getDag(), false)).render(Format.PNG).toFile(new File("smoke.png"));
        }
        assertNotNull(genesisKey);

        seed(nodes);

        Slf4jReporter.forRegistry(node0Registry)
                     .outputTo(LoggerFactory.getLogger("func-metrics"))
                     .convertRatesTo(TimeUnit.SECONDS)
                     .convertDurationsTo(TimeUnit.MILLISECONDS)
                     .build()
                     .start(30, TimeUnit.SECONDS);

        ConsoleReporter.forRegistry(node0Registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .start(30, TimeUnit.SECONDS);

        long now = System.currentTimeMillis();
        HashKey k = genesisKey;
        for (Avalanche a : nodes) {
            assertTrue("Failed to finalize genesis on: " + a.getNode().getId(),
                       Utils.waitForCondition(10_000, () -> a.getDagDao().isFinalized(k)));
        }

        List<Transactioneer> transactioneers = nodes.stream()
                                                    .map(a -> new Transactioneer(a, 700_000))
                                                    .collect(Collectors.toList());

        ArrayList<Transactioneer> startUp = new ArrayList<>(transactioneers);
        Collections.shuffle(startUp, entropy);
        transactioneers.parallelStream().forEach(t -> t.transact(Duration.ofSeconds(120), outstanding, txnScheduler));

        boolean finalized = Utils.waitForCondition(runtime, 1_000, () -> {
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
        Thread.sleep(2_000); // drain the swamp

        System.out.println("Global tps: "
                + transactioneers.stream().mapToInt(e -> e.getSuccess()).sum() / (duration / 1000));

        System.out.println("Max tps per node: "
                + nodes.stream().mapToInt(n -> n.getDag().getFinalized().size()).max().orElse(0) / (duration / 1000));
        nodes.forEach(node -> summary(node));

        System.out.println("wanted: ");
        System.out.println(master.getDag().getWanted());
        System.out.println();
        transactioneers.forEach(t -> {
            System.out.println("finalized " + t.getSuccess() + " and failed to finalize " + t.getFailed() + " for "
                    + t.getId());
        });
        System.out.println();
        System.out.println("Global Metrics");
        ConsoleReporter.forRegistry(node0Registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
//        System.out.println();
//        System.out.println("Comm Metrics");
//        ConsoleReporter.forRegistry(commRegistry)
//                       .convertRatesTo(TimeUnit.SECONDS)
//                       .convertDurationsTo(TimeUnit.MILLISECONDS)
//                       .build()
//                       .report();
//        FileSerializer.serialize(DagViz.visualize("smoke", nodes.get(0).getDag(), true), new File("smoke.dot"));

//        Graphviz.fromGraph(DagViz.visualize("smoke", nodes.get(0).getDag(), true)).render(Format.PNG).toFile(new File("smoke.png"));
        assertTrue("failed to finalize " + target + " txns: " + transactioneers, finalized);
    }

    abstract protected AvalancheCommunications getCommunications();

    private void seed(List<Avalanche> nodes) {
        long then = System.currentTimeMillis();
        List<Transactioneer> transactioneers = nodes.stream()
                                                    .map(a -> new Transactioneer(a, 5))
                                                    .collect(Collectors.toList());

        transactioneers.forEach(t -> t.transact(Duration.ofSeconds(120), 2, scheduler));

        boolean seeded = Utils.waitForCondition(10_000, 500, () -> {
            return transactioneers.stream()
                                  .mapToInt(t -> t.getSuccess())
                                  .filter(s -> s >= 2)
                                  .count() == transactioneers.size();
        });

        assertTrue("could not seed initial txn set", seeded);
        System.out.println("seeded initial txns in " + (System.currentTimeMillis() - then) + " ms");
    }

    private void summary(Avalanche node) {
        System.out.println(node.getNode().getId() + " : ");

        Integer finalized = node.getDag().getFinalized().size();
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
