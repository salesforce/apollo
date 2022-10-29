/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.domain;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.choam.proto.Foundation;
import com.salesfoce.apollo.choam.proto.FoundationSeal;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.delphinius.Oracle.Assertion;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.Domain.TransactionConfiguration;
import com.salesforce.apollo.model.ProcessDomain;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class Smokin {
    private static final int                        CARDINALITY     = 5;
    private final static List<ProcessDomain>        domains         = new ArrayList<>();
    private static final Digest                     GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());
    private final static Map<ProcessDomain, Router> routers         = new HashMap<>();

    public static void before() throws Exception {
        var scheduler = Executors.newScheduledThreadPool(2);
        var ffParams = com.salesforce.apollo.fireflies.Parameters.newBuilder();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        final var prefix = UUID.randomUUID().toString();
        Path checkpointDirBase = Path.of("target", "ct-chkpoints-" + Entropy.nextBitsStreamLong());
        Utils.clean(checkpointDirBase.toFile());
        var params = params();
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(params.getDigestAlgorithm()), entropy);

        var identities = IntStream.range(0, CARDINALITY).mapToObj(i -> {
            try {
                return stereotomy.newIdentifier().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }).collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(), controlled -> controlled));

        Digest group = DigestAlgorithm.DEFAULT.getOrigin();
        var foundation = Foundation.newBuilder();
        identities.keySet().forEach(d -> foundation.addMembership(d.toDigeste()));
        var sealed = FoundationSeal.newBuilder().setFoundation(foundation).build();
        TransactionConfiguration txnConfig = new TransactionConfiguration(Executors.newFixedThreadPool(2),
                                                                          Executors.newSingleThreadScheduledExecutor());
        identities.forEach((digest, id) -> {
            var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getLast(), CARDINALITY, 0.2, 3);
            final var member = new ControlledIdentifierMember(id);
            var localRouter = new LocalServer(prefix, member,
                                              Executors.newSingleThreadExecutor()).router(ServerConnectionCache.newBuilder().setTarget(30), ForkJoinPool.commonPool());
            var node = new ProcessDomain(group, member, params, "jdbc:h2:mem:", checkpointDirBase,
                                         RuntimeParameters.newBuilder()
                                                          .setFoundation(sealed)
                                                          .setScheduler(scheduler)
                                                          .setContext(context)
                                                          .setExec(ForkJoinPool.commonPool())
                                                          .setCommunications(localRouter),
                                         new InetSocketAddress(0), ffParams, txnConfig);
            domains.add(node);
            routers.put(node, localRouter);
            localRouter.start();
        });
    }

    public static void main(String[] argv) {
        final var demesnes = new Demesne();
        LoggerFactory.getLogger(Smokin.class).info("Check: {}", demesnes.active());
        try {
            before();
            smokin();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static void smoke(Oracle oracle) throws Exception {
        // Namespace
        var ns = Oracle.namespace("my-org");

        // relations
        var member = ns.relation("member");
        var flag = ns.relation("flag");

        // Group membersip
        var userMembers = ns.subject("Users", member);
        var adminMembers = ns.subject("Admins", member);
        var helpDeskMembers = ns.subject("HelpDesk", member);
        var managerMembers = ns.subject("Managers", member);
        var technicianMembers = ns.subject("Technicians", member);
        var abcTechMembers = ns.subject("ABCTechnicians", member);
        var flaggedTechnicianMembers = ns.subject(abcTechMembers.name(), flag);

        // Flagged subjects for testing
        var egin = ns.subject("Egin", flag);
        var ali = ns.subject("Ali", flag);
        var gl = ns.subject("G l", flag);
        var fuat = ns.subject("Fuat", flag);

        // Subjects
        var jale = ns.subject("Jale");
        var irmak = ns.subject("Irmak");
        var hakan = ns.subject("Hakan");
        var demet = ns.subject("Demet");
        var can = ns.subject("Can");
        var burcu = ns.subject("Burcu");

        // Map direct edges. Transitive edges added as a side effect
        CompletableFuture.allOf(oracle.map(helpDeskMembers, adminMembers), oracle.map(ali, adminMembers),
                                oracle.map(ali, userMembers), oracle.map(burcu, userMembers),
                                oracle.map(can, userMembers), oracle.map(managerMembers, userMembers),
                                oracle.map(technicianMembers, userMembers), oracle.map(demet, helpDeskMembers),
                                oracle.map(egin, helpDeskMembers), oracle.map(egin, userMembers),
                                oracle.map(fuat, managerMembers), oracle.map(gl, managerMembers),
                                oracle.map(hakan, technicianMembers), oracle.map(irmak, technicianMembers),
                                oracle.map(abcTechMembers, technicianMembers),
                                oracle.map(flaggedTechnicianMembers, technicianMembers),
                                oracle.map(jale, abcTechMembers))
                         .get();

        // Protected resource namespace
        var docNs = Oracle.namespace("Document");
        // Permission
        var view = docNs.relation("View");
        // Protected Object
        var object123View = docNs.object("123", view);

        // Users can View Document 123
        Assertion tuple = userMembers.assertion(object123View);
        oracle.add(tuple).get();

        // Direct subjects that can View the document
        oracle.read(object123View);

        // Direct objects that can User member can view
        oracle.read(userMembers);

        // Assert flagged technicians can directly view the document
        Assertion grantTechs = flaggedTechnicianMembers.assertion(object123View);
        oracle.add(grantTechs).get();

        // Now have 2 direct subjects that can view the doc
        oracle.read(object123View);

        // flagged has direct view
        oracle.read(flaggedTechnicianMembers);

        // Filter direct on flagged relation
        oracle.read(flag, object123View);

        // Transitive subjects that can view the document
        oracle.expand(object123View);

        // Transitive subjects filtered by flag predicate
        oracle.expand(flag, object123View);

        // Remove them
        oracle.remove(abcTechMembers, technicianMembers).get();

        // Remove our assertion
        oracle.delete(tuple).get();

        // Some deletes
        oracle.delete(abcTechMembers).get();
        oracle.delete(flaggedTechnicianMembers).get();
    }

    public static void smokin() throws Exception {
        long then = System.currentTimeMillis();
        final var gossipDuration = Duration.ofMillis(10);
        final var countdown = new CountDownLatch(domains.size());
        final var seeds = Collections.singletonList(new Seed(domains.get(0).getMember().getEvent().getCoordinates(),
                                                             new InetSocketAddress(0)));
        domains.forEach(d -> {
            d.getFoundation().register((context, viewId, joins, leaves) -> {
                if (context.totalCount() == CARDINALITY) {
                    System.out.println(String.format("Full view: %s members: %s on: %s", viewId, context.totalCount(),
                                                     d.getMember().getId()));
                    countdown.countDown();
                } else {
                    System.out.println(String.format("Members joining: %s members: %s on: %s", viewId,
                                                     context.totalCount(), d.getMember().getId()));
                }
            });
        });
        // start seed
        final var scheduler = Executors.newScheduledThreadPool(2);
        final var started = new AtomicReference<>(new CountDownLatch(1));

        domains.get(0)
               .getFoundation()
               .start(() -> started.get().countDown(), gossipDuration, Collections.emptyList(), scheduler);

        started.set(new CountDownLatch(CARDINALITY - 1));
        domains.subList(1, domains.size()).forEach(d -> {
            d.getFoundation().start(() -> started.get().countDown(), gossipDuration, seeds, scheduler);
        });
        System.out.println();
        System.out.println("******");
        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
        + domains.size() + " members");
        System.out.println("******");
        System.out.println();
        domains.forEach(n -> n.start());
        final var activated = Utils.waitForCondition(60_000, 1_000,
                                                     () -> domains.stream().filter(c -> !c.active()).count() == 0);

        if (!activated) {
            System.out.println("Domains did not become active : "
            + (domains.stream().filter(c -> !c.active()).toList()));
        }
        System.out.println();
        System.out.println("******");
        System.out.println("Domains have activated in " + (System.currentTimeMillis() - then) + " Ms across all "
        + domains.size() + " members");
        System.out.println("******");
        System.out.println();
        var oracle = domains.get(0).getDelphi();
        oracle.add(new Oracle.Namespace("test")).get();
        smoke(oracle);
    }

    private static Parameters.Builder params() {
        var params = Parameters.newBuilder()
                               .setGenesisViewId(GENESIS_VIEW_ID)
                               .setGossipDuration(Duration.ofMillis(50))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(50))
                                                              .setBatchInterval(Duration.ofMillis(100))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(3000)
                                                              .build())
                               .setCheckpointBlockDelta(200);

        params.getProducer().ethereal().setNumberOfEpochs(5);
        return params;
    }
}