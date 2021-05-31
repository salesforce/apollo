/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.math3.random.MersenneTwister;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.ByteMessage;
import com.salesforce.apollo.ApolloConfiguration.CommunicationsFactory;
import com.salesforce.apollo.avalanche.Processor.TimedProcessor;
import com.salesforce.apollo.bootstrap.BootstrapCA;
import com.salesforce.apollo.bootstrap.BootstrapConfiguration;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.fireflies.FireflyMetricsImpl;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.utils.Utils;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;

/**
 * @author hhildebrand
 */
@ExtendWith(DropwizardExtensionsSupport.class)
public class BootstrapTest {

    private static DropwizardAppExtension<BootstrapConfiguration> EXT = new DropwizardAppExtension<BootstrapConfiguration>(
            BootstrapCA.class, ResourceHelpers.resourceFilePath("bootstrap.yml"));
    private List<Apollo>                                          oracles;

    @AfterEach
    public void after() {
        if (oracles != null) {
            oracles.forEach(o -> o.stop());
            oracles.clear();
        }
    }

    @Test
    public void smoke() throws Exception {
        File baseDir = new File("target/bootstrap");
        Utils.clean(baseDir);
        baseDir.mkdirs();
        oracles = new ArrayList<>();
        URL endpoint = new URL(String.format("http://localhost:%d/api/cnc/mint", EXT.getLocalPort()));

        for (int i = 0; i < 14; i++) {
            ApolloConfiguration config = new ApolloConfiguration();
            config.avalanche.core.alpha = 0.6;
            config.avalanche.core.k = 6;
            config.avalanche.core.beta1 = 3;
            config.avalanche.core.beta2 = 5;
            config.gossipInterval = Duration.ofMillis(50);
            config.communications = new CommunicationsFactory() {
                @Override
                public Router getComms(MetricRegistry metrics, Node node, ForkJoinPool executor) {
                    return new LocalRouter(node,
                            ServerConnectionCache.newBuilder()
                                                 .setTarget(30)
                                                 .setMetrics(new FireflyMetricsImpl(metrics)),
                            ForkJoinPool.commonPool());
                }
            };
            BootstrapIdSource ks = new BootstrapIdSource();
            ks.endpoint = endpoint;
            config.source = ks;
            oracles.add(new Apollo(config));
        }
        long then = System.currentTimeMillis();

        oracles.get(0).start();
        Thread.sleep(500);
        oracles.forEach(oracle -> {
            try {
                oracle.start();
            } catch (Exception e) {
                throw new IllegalStateException("unable to start oracle", e);
            }
        });

        System.out.println(oracles.size() + " apollo instances started");
        boolean stable = Utils.waitForCondition(30_000, 1_000, () -> {
            return oracles.stream()
                          .map(o -> o.getView())
                          .map(view -> view.getLive().size() < oracles.size() ? view : null)
                          .filter(view -> view != null)
                          .count() == 0;
        });
        if (!stable) {
            Set<Member> fullSet = oracles.stream().map(a -> a.getView().getNode()).collect(Collectors.toSet());
            Map<Member, Collection<Member>> missing = new HashMap<>();
            oracles.forEach(a -> {
                SetView<Member> difference = Sets.difference(fullSet, new HashSet<>(a.getView().getLive()));
                if (!difference.isEmpty()) {
                    missing.put(a.getAvalanche().getNode(), difference);
                }
            });
            fail("Did not stabilize view: " + missing);
        }

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
                + oracles.size() + " members");
        TimedProcessor master = oracles.get(0).getProcessor();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(20);
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
        HashKey key = genesisKey;
        for (Apollo o : oracles) {
            assertTrue(Utils.waitForCondition(15_000, () -> o.getAvalanche().getDagDao().isFinalized(key)),
                       "Failed to finalize genesis on: " + o.getAvalanche().getNode().getId());
            transactioneers.add(new Transactioneer(o.getProcessor()));
        }

        // # of txns per node
        int target = 15;
        transactioneers.forEach(t -> t.transact(Duration.ofSeconds(120), target * 40, scheduler));

        boolean finalized = Utils.waitForCondition(30_000, 1_000, () -> {
            return transactioneers.stream()
                                  .mapToInt(t -> t.getSuccess())
                                  .filter(s -> s >= target)
                                  .count() == transactioneers.size();
        });

        System.out.println("Completed in " + (System.currentTimeMillis() - now) + " ms");
        transactioneers.forEach(t -> t.stop());
        oracles.forEach(node -> node.stop());
        // System.out.println(profiler.getTop(3));

        TestApollo.summarize(oracles);
        oracles.forEach(node -> TestApollo.summary(node.getAvalanche()));

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
