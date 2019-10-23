/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo;

import static com.salesforce.apollo.dagwood.schema.Tables.DAG;
import static com.salesforce.apollo.dagwood.schema.Tables.UNQUERIED;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.Ignore;
import org.junit.Test;

import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hal.hildebrand
 * @since 218
 */
@Ignore
public class TestApollo {

    static {
        ApolloConfiguration.SimCommunicationsFactory.reset();
    }

    public static void summarize(List<Apollo> nodes) {
        int finalized = nodes
                             .stream()
                             .map(a -> a.getAvalanche())
                             .map(n -> n.getDslContext()
                                        .selectCount()
                                        .from(DAG)
                                        .where(DAG.NOOP.isFalse())
                                        .and(DAG.FINALIZED.isTrue())
                                        .fetchOne()
                                        .value1())
                             .reduce(0, (a, b) -> a + b);
        System.out.println("Total finalized : " + finalized);
        System.out.println();
    }

    public static void summary(Avalanche node) {
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
                + " unqueried: " + node.getDslContext()
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
                + " unqueried: " + node.getDslContext()
                                       .selectCount()
                                       .from(DAG)
                                       .join(UNQUERIED)
                                       .on(UNQUERIED.HASH.eq(DAG.HASH))
                                       .where(DAG.NOOP.isTrue())
                                       .fetchOne()
                                       .value1());
    }

    @Test
    public void smoke() throws Exception {
        List<Apollo> oracles = new ArrayList<>();

        for (int i = 1; i <= PregenPopulation.getCardinality(); i++) {
            ApolloConfiguration config = new ApolloConfiguration();
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
        HASH k = genesisKey.toHash();
        for (Apollo o : oracles) {
            assertTrue("Failed to finalize genesis on: " + o.getAvalanche().getNode().getId(),
                       Utils.waitForCondition(15_000, () -> o.getAvalanche().getDagDao().isFinalized(k)));
            transactioneers.add(new Transactioneer(o.getAvalanche()));
        }

        // # of txns per node
        int target = 15;
        transactioneers.forEach(t -> t.transact(Duration.ofSeconds(120), target * 40, scheduler));

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
        System.out.println(master.getDag()
                                 .getWanted(Integer.MAX_VALUE, master.getDslContext())
                                 .stream()
                                 .map(e -> new HashKey(e))
                                 .collect(Collectors.toList()));
        System.out.println();
        System.out.println();
        assertTrue("failed to finalize " + target + " txns: " + transactioneers, finalized);
        transactioneers.forEach(t -> {
            System.out.println("failed to finalize " + t.getFailed() + " for " + t.getId());
        });

    }
}
