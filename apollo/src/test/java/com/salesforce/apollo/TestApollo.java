/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.math3.random.MersenneTwister;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.ByteMessage;
import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avalanche.Processor.TimedProcessor;
import com.salesforce.apollo.avalanche.WorkingSet.KnownNode;
import com.salesforce.apollo.avalanche.WorkingSet.NoOpNode;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hal.hildebrand
 * @since 218
 */
public class TestApollo {

    public static void summarize(List<Apollo> nodes) {
        int finalized = nodes.stream()
                             .map(a -> a.getAvalanche())
                             .map(n -> n.getDag().getFinalizedCount())
                             .reduce(0, (a, b) -> a + b);
        System.out.println("Total finalized : " + finalized);
        System.out.println();
    }

    public static void summary(Avalanche node) {
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

    private List<Apollo> oracles;

    @AfterEach
    public void after() {
        if (oracles != null) {
            oracles.forEach(o -> o.stop());
            oracles.clear();
        }
    }

    @Test
    public void configuration() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.readValue(getClass().getResource("/test.yml"), ApolloConfiguration.class);
    }

    @Test
    public void smoke() throws Exception {
        File baseDir = new File(System.getProperty("user.dir"), "target/cluster");
        Utils.clean(baseDir);
        baseDir.mkdirs();
        oracles = new ArrayList<>();

        for (int i = 1; i < PregenPopulation.getCardinality() + 1; i++) {
            ApolloConfiguration config = new ApolloConfiguration();
            config.avalanche.core.alpha = 0.6;
            config.avalanche.core.k = 3;
            config.avalanche.core.beta1 = 3;
            config.avalanche.core.beta2 = 5;
            config.gossipInterval = Duration.ofMillis(100);
            config.communications = new ApolloConfiguration.SimCommunicationsFactory();
            ApolloConfiguration.ResourceIdentitySource ks = new ApolloConfiguration.ResourceIdentitySource();
            ks.store = PregenPopulation.memberKeystoreResource(i);
            config.source = ks;
            oracles.add(new Apollo(config));
        }
        long then = System.currentTimeMillis();

        oracles.forEach(oracle -> {
            try {
                oracle.start();
            } catch (Exception e) {
                throw new IllegalStateException("unable to start oracle", e);
            }
        });

        assertTrue(Utils.waitForCondition(15_000, 1_000, () -> {
            return oracles.stream()
                          .map(o -> o.getView())
                          .map(view -> view.getLive().size() != oracles.size() ? view : null)
                          .filter(view -> view != null)
                          .count() == 0;
        }), "Did not stabilize the view");

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
                + oracles.size() + " members");
        TimedProcessor master = oracles.get(0).getProcessor();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        CompletableFuture<HashKey> genesis = master.createGenesis(ByteMessage.newBuilder()
                                                                             .setContents(ByteString.copyFromUtf8("Genesis"))
                                                                             .build(),
                                                                  Duration.ofSeconds(90), scheduler);
        HashKey genesisKey = null;
        try {
            genesisKey = genesis.get(60, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            oracles.forEach(node -> node.stop());
        }
        assertNotNull(genesisKey);

        long now = System.currentTimeMillis();
        List<Transactioneer> transactioneers = new ArrayList<>();
        HashKey k = genesisKey;
        for (Apollo o : oracles) {
            assertTrue(Utils.waitForCondition(15_000, () -> o.getAvalanche().getDagDao().isFinalized(k)),
                       "Failed to finalize genesis on: " + o.getAvalanche().getNode().getId());
            transactioneers.add(new Transactioneer(o.getProcessor()));
        }

        // # of txns per node
        int target = 15;
        transactioneers.forEach(t -> t.transact(Duration.ofSeconds(120), 20, scheduler));

        boolean finalized = Utils.waitForCondition(300_000, 1_000, () -> {
            return transactioneers.stream()
                                  .mapToInt(t -> t.getSuccess())
                                  .filter(s -> s >= target)
                                  .count() == transactioneers.size();
        });

        System.out.println("Completed in " + (System.currentTimeMillis() - now) + " ms");
        transactioneers.forEach(t -> t.stop());
        oracles.forEach(node -> node.stop());
        // System.out.println(profiler.getTop(3));

        summarize(oracles);
        oracles.forEach(node -> summary(node.getAvalanche()));

        System.out.println("wanted: ");
        System.out.println(master.getAvalanche()
                                 .getDag()
                                 .getWanted(new MersenneTwister(), 100_000)
                                 .stream()
                                 .collect(Collectors.toList()));
        System.out.println();
        System.out.println();
        assertTrue(finalized, "failed to finalize " + target + " txns: " + transactioneers);
        transactioneers.forEach(t -> {
            System.out.println("failed to finalize " + t.getFailed() + " for " + t.getId());
        });

    }
}
