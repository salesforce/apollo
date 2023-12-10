/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demesnes;

import com.salesforce.apollo.choam.proto.Foundation;
import com.salesforce.apollo.choam.proto.FoundationSeal;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.delphinius.Oracle.Assertion;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.ProcessDomain;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author hal.hildebrand
 */
public class FireFliesTrace {
    private static final int                        CARDINALITY     = 5;
    private static final Digest                     GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest(
    "Give me food or give me slack or kill me".getBytes());
    private final        List<ProcessDomain>        domains         = new ArrayList<>();
    private final        Map<ProcessDomain, Router> routers         = new HashMap<>();

    public static void main(String[] argv) throws Exception {
        var t = new FireFliesTrace();
        try {
            t.before();
            t.smokin();
        } finally {
            t.after();
            System.exit(0);
        }
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
                                oracle.map(jale, abcTechMembers)).get();

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
        // Check some assertions
        oracle.check(object123View.assertion(jale));
        oracle.check(object123View.assertion(egin));
        oracle.check(object123View.assertion(helpDeskMembers));

        // Remove them
        oracle.remove(abcTechMembers, technicianMembers).get();

        oracle.check(object123View.assertion(jale));
        oracle.check(object123View.assertion(egin));
        oracle.check(object123View.assertion(helpDeskMembers));

        // Remove our assertion
        oracle.delete(tuple).get();

        oracle.check(object123View.assertion(jale));
        oracle.check(object123View.assertion(egin));
        oracle.check(object123View.assertion(helpDeskMembers));

        // Some deletes
        oracle.delete(abcTechMembers).get();
        oracle.delete(flaggedTechnicianMembers).get();
    }

    public void after() {
        domains.forEach(n -> n.stop());
        domains.clear();
        routers.values().forEach(r -> r.close(Duration.ofSeconds(1)));
        routers.clear();
    }

    public void before() throws Exception {

        final var commsDirectory = Path.of("target/comms");
        commsDirectory.toFile().mkdirs();

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
        var foundation = Foundation.newBuilder();
        identities.keySet().forEach(d -> foundation.addMembership(d.toDigeste()));
        var sealed = FoundationSeal.newBuilder().setFoundation(foundation).build();
        identities.forEach((digest, id) -> {
            var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getLast(), CARDINALITY, 0.2, 3);
            final var member = new ControlledIdentifierMember(id);
            var localRouter = new LocalServer(prefix, member).router(ServerConnectionCache.newBuilder().setTarget(30));
            var node = new ProcessDomain(group, member, params, "jdbc:h2:mem:", checkpointDirBase,
                                         RuntimeParameters.newBuilder()
                                                          .setFoundation(sealed)
                                                          .setContext(context)
                                                          .setCommunications(localRouter), new InetSocketAddress(0),
                                         commsDirectory, ffParams, EventValidation.NONE,
                                         IdentifierSpecification.newBuilder());
            domains.add(node);
            routers.put(node, localRouter);
            localRouter.start();
        });
    }

    public void smokin() throws Exception {
        final var gossipDuration = Duration.ofMillis(10);
        long then = System.currentTimeMillis();
        final var countdown = new CountDownLatch(domains.size());
        final var seeds = Collections.singletonList(
        new Seed(domains.get(0).getMember().getEvent().getCoordinates(), new InetSocketAddress(0)));
        domains.forEach(d -> {
            var listener = new View.ViewLifecycleListener() {

                @Override
                public void update(EventCoordinates update) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void viewChange(Context<Participant> context, Digest viewId, List<EventCoordinates> joins,
                                       List<Digest> leaves) {
                    if (context.totalCount() == CARDINALITY) {
                        System.out.println(
                        String.format("Full view: %s members: %s on: %s", viewId, context.totalCount(),
                                      d.getMember().getId()));
                        countdown.countDown();
                    } else {
                        System.out.println(
                        String.format("Members joining: %s members: %s on: %s", viewId, context.totalCount(),
                                      d.getMember().getId()));
                    }
                }
            };
            d.getFoundation().register(listener);
        });
        // start seed
        final var started = new AtomicReference<>(new CountDownLatch(1));

        domains.get(0)
               .getFoundation()
               .start(() -> started.get().countDown(), gossipDuration, Collections.emptyList(),
                      Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory()));
        if (!started.get().await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Cannot start up kernel");
        }

        started.set(new CountDownLatch(CARDINALITY - 1));
        domains.subList(1, domains.size()).forEach(d -> {
            d.getFoundation()
             .start(() -> started.get().countDown(), gossipDuration, seeds,
                    Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory()));
        });
        if (!started.get().await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Cannot start views");
        }
        if (!countdown.await(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Could not join all members in all views");
        }

        Utils.waitForCondition(60_000, 1_000, () -> {
            return domains.stream().filter(d -> d.getFoundation().getContext().activeCount() != domains.size()).count()
            == 0;
        });
        System.out.println();
        System.out.println("******");
        System.out.println(
        "View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all " + domains.size()
        + " members");
        System.out.println("******");
        System.out.println();
        domains.forEach(n -> n.start());
        Utils.waitForCondition(60_000, 1_000, () -> domains.stream().filter(c -> !c.active()).count() == 0);
        System.out.println();
        System.out.println("******");
        System.out.println(
        "Domains have activated in " + (System.currentTimeMillis() - then) + " Ms across all " + domains.size()
        + " members");
        System.out.println("******");
        System.out.println();
        var oracle = domains.get(0).getDelphi();
        oracle.add(new Oracle.Namespace("test")).get();
        smoke(oracle);
    }

    private Builder params() {
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
