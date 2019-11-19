/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.salesforce.apollo.avalanche.Avalanche;
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
                             .map(n -> n.getDag().getFinalized().size())
                             .reduce(0, (a, b) -> a + b);
        System.out.println("Total finalized : " + finalized);
        System.out.println();
    }

    public static void summary(Avalanche node) {
        System.out.println(node.getNode().getId() + " : ");
        System.out.println("    Rounds: " + node.getRoundCounter());

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
        ApolloConfiguration.SimCommunicationsFactory.reset();
        List<Apollo> oracles = new ArrayList<>();

        for (int i = 1; i < PregenPopulation.getCardinality() + 1; i++) {
            ApolloConfiguration config = new ApolloConfiguration();
            config.avalanche.alpha = 0.6;
            config.avalanche.k = 3;
            config.avalanche.beta1 = 3;
            config.avalanche.beta2 = 5;
            config.gossipInterval = Duration.ofMillis(100);
            config.communications = new ApolloConfiguration.SimCommunicationsFactory();
            ApolloConfiguration.FileIdentitySource ks = new ApolloConfiguration.FileIdentitySource();
            ks.store = new File(PregenPopulation.getMemberDir(), PregenPopulation.memberKeystoreFile(i));
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

        assertTrue("Did not stabilize the view", Utils.waitForCondition(15_000, 1_000, () -> {
            return oracles.stream()
                          .map(o -> o.getView())
                          .map(view -> view.getLive().size() != oracles.size() ? view : null)
                          .filter(view -> view != null)
                          .count() == 0;
        }));

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
                + oracles.size() + " members");
        Avalanche master = oracles.get(0).getAvalanche();
        System.out.println("Start round: " + master.getRoundCounter());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        CompletableFuture<HashKey> genesis = master.createGenesis("Genesis".getBytes(), Duration.ofSeconds(90),
                                                                  scheduler);
        HashKey genesisKey = null;
        try {
            genesisKey = genesis.get(60, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            oracles.forEach(node -> node.stop());
        }
        System.out.println("Rounds: " + master.getRoundCounter());
        assertNotNull(genesisKey);

        long now = System.currentTimeMillis();
        List<Transactioneer> transactioneers = new ArrayList<>();
        HashKey k = genesisKey;
        for (Apollo o : oracles) {
            assertTrue("Failed to finalize genesis on: " + o.getAvalanche().getNode().getId(),
                       Utils.waitForCondition(15_000, () -> o.getAvalanche().getDagDao().isFinalized(k)));
            transactioneers.add(new Transactioneer(o.getAvalanche()));
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
        System.out.println(master.getDag().getWanted().stream().collect(Collectors.toList()));
        System.out.println();
        System.out.println();
        assertTrue("failed to finalize " + target + " txns: " + transactioneers, finalized);
        transactioneers.forEach(t -> {
            System.out.println("failed to finalize " + t.getFailed() + " for " + t.getId());
        });

    }
}
