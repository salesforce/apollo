/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.proto.FoundationSeal;
import com.salesforce.apollo.choam.support.ExponentialBackoffPolicy;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.context.DynamicContextImpl;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 */
public class FireFliesTest {
    private static final int    CARDINALITY     = 5;
    private static final Digest GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest(
    "Give me food or give me slack or kill me".getBytes());

    private final List<ProcessDomain>        domains = new ArrayList<>();
    private final Map<ProcessDomain, Router> routers = new HashMap<>();

    @AfterEach
    public void after() {
        domains.forEach(n -> n.stop());
        domains.clear();
        routers.values().forEach(r -> r.close(Duration.ofSeconds(1)));
        routers.clear();
    }

    @BeforeEach
    public void before() throws Exception {
        var ffParams = com.salesforce.apollo.fireflies.Parameters.newBuilder();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        final var prefix = UUID.randomUUID().toString();
        Path checkpointDirBase = Path.of("target", "ct-chkpoints-" + Entropy.nextBitsStreamLong());
        Utils.clean(checkpointDirBase.toFile());
        var params = params();
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(params.getDigestAlgorithm()), entropy);

        var identities = IntStream.range(0, CARDINALITY).mapToObj(i -> {
            return stereotomy.newIdentifier();
        }).collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(), controlled -> controlled));

        Digest group = DigestAlgorithm.DEFAULT.getOrigin();
        var sealed = FoundationSeal.newBuilder().build();
        identities.forEach((digest, id) -> {
            var context = new DynamicContextImpl<>(DigestAlgorithm.DEFAULT.getLast(), CARDINALITY, 0.2, 3);
            final var member = new ControlledIdentifierMember(id);
            var localRouter = new LocalServer(prefix, member).router(ServerConnectionCache.newBuilder().setTarget(30));
            var dbUrl = String.format("jdbc:h2:mem:sql-%s-%s;DB_CLOSE_DELAY=-1", member.getId(), UUID.randomUUID());
            var pdParams = new ProcessDomain.ProcessDomainParameters(dbUrl, Duration.ofSeconds(5),
                                                                     "jdbc:h2:mem:%s-state".formatted(digest),
                                                                     checkpointDirBase, Duration.ofMillis(10), 0.00125,
                                                                     Duration.ofSeconds(5), 3, 10, 0.1);
            var node = new ProcessDomain(group, member, pdParams, params.clone(), RuntimeParameters.newBuilder()
                                                                                                   .setFoundation(
                                                                                                   sealed)
                                                                                                   .setContext(context)
                                                                                                   .setCommunications(
                                                                                                   localRouter),
                                         new InetSocketAddress(0), ffParams, null);
            domains.add(node);
            routers.put(node, localRouter);
            localRouter.start();
        });
    }

    @Test
    public void smokin() throws Exception {
        final var gossipDuration = Duration.ofMillis(10);
        long then = System.currentTimeMillis();
        final var countdown = new CountDownLatch(domains.size());
        final var seeds = Collections.singletonList(
        new Seed(domains.getFirst().getMember().getIdentifier().getIdentifier(), new InetSocketAddress(0)));
        domains.forEach(d -> {
            BiConsumer<Context, Digest> c = (context, viewId) -> {
                if (context.cardinality() == CARDINALITY) {
                    System.out.printf("Full view: %s members: %s on: %s%n", viewId, context.cardinality(),
                                      d.getMember().getId());
                    countdown.countDown();
                } else {
                    System.out.printf("Members joining: %s members: %s on: %s%n", viewId, context.cardinality(),
                                      d.getMember().getId());
                }
            };
            d.foundation.register(c);
        });
        // start seed
        final var started = new AtomicReference<>(new CountDownLatch(1));

        domains.getFirst()
               .getFoundation()
               .start(() -> started.get().countDown(), gossipDuration, Collections.emptyList());
        assertTrue(started.get().await(10, TimeUnit.SECONDS), "Cannot start up kernel");

        started.set(new CountDownLatch(CARDINALITY - 1));
        domains.subList(1, domains.size())
               .forEach(d -> Thread.ofVirtual()
                                   .start(() -> d.getFoundation()
                                                 .start(() -> started.get().countDown(), gossipDuration, seeds)));
        assertTrue(started.get().await(30, TimeUnit.SECONDS), "could not start views");

        assertTrue(countdown.await(30, TimeUnit.SECONDS), "Could not join all members in all views");

        assertTrue(Utils.waitForCondition(60_000, 1_000, () -> {
            return domains.stream().filter(d -> d.getFoundation().getContext().activeCount() != domains.size()).count()
            == 0;
        }));
        System.out.println();
        System.out.println("******");
        System.out.println(
        "View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all " + domains.size()
        + " members");
        System.out.println("******");
        System.out.println();
        domains.forEach(n -> n.start());
        final var activated = Utils.waitForCondition(20_000, 1_000,
                                                     () -> domains.stream().filter(c -> !c.active()).count() == 0);
        assertTrue(activated, "Domains did not become active : " + (domains.stream()
                                                                           .filter(c -> !c.active())
                                                                           .map(d -> d.logState())
                                                                           .toList()));
        System.out.println();
        System.out.println("******");
        System.out.println(
        "Domains have activated in " + (System.currentTimeMillis() - then) + " Ms across all " + domains.size()
        + " members");
        System.out.println("******");
        System.out.println();
        var oracle = domains.get(0).getDelphi();
        oracle.add(new Oracle.Namespace("test")).get();
        DomainTest.smoke(oracle);
    }

    private Builder params() {
        var template = Parameters.newBuilder()
                                 .setGenerateGenesis(true)
                                 .setGenesisViewId(GENESIS_VIEW_ID)
                                 .setBootstrap(Parameters.BootstrapParameters.newBuilder()
                                                                             .setGossipDuration(Duration.ofMillis(50))
                                                                             .build())
                                 .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin())
                                 .setGossipDuration(Duration.ofMillis(50))
                                 .setProducer(Parameters.ProducerParameters.newBuilder()
                                                                           .setGossipDuration(Duration.ofMillis(50))
                                                                           .setBatchInterval(Duration.ofMillis(50))
                                                                           .setMaxBatchByteSize(1024 * 1024)
                                                                           .setMaxBatchCount(10_000)
                                                                           .setEthereal(Config.newBuilder()
                                                                                              .setNumberOfEpochs(3)
                                                                                              .setEpochLength(7))
                                                                           .build())
                                 .setCheckpointBlockDelta(200)
                                 .setDrainPolicy(ExponentialBackoffPolicy.newBuilder()
                                                                         .setInitialBackoff(Duration.ofMillis(1))
                                                                         .setMaxBackoff(Duration.ofMillis(1)));
        return template;
    }
}
