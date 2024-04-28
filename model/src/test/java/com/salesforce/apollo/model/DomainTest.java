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
import com.salesforce.apollo.context.DynamicContextImpl;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.delphinius.Oracle.Assertion;
import com.salesforce.apollo.ethereal.Config;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.salesforce.apollo.choam.Session.retryNesting;
import static java.util.concurrent.CompletableFuture.allOf;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author hal.hildebrand
 */
public class DomainTest {
    private static final int               CARDINALITY     = 5;
    private static final Digest            GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest(
    "Give me food or give me slack or kill me".getBytes());
    private final        ArrayList<Domain> domains         = new ArrayList<>();
    private final        ArrayList<Router> routers         = new ArrayList<>();

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

        allOf(retryNesting(() -> oracle.map(helpDeskMembers, adminMembers), 3),
              retryNesting(() -> oracle.map(ali, adminMembers), 3), retryNesting(() -> oracle.map(ali, userMembers), 3),
              retryNesting(() -> oracle.map(burcu, userMembers), 3),
              retryNesting(() -> oracle.map(can, userMembers), 3),
              retryNesting(() -> oracle.map(managerMembers, userMembers), 3),
              retryNesting(() -> oracle.map(technicianMembers, userMembers), 3),
              retryNesting(() -> oracle.map(demet, helpDeskMembers), 3),
              retryNesting(() -> oracle.map(egin, helpDeskMembers), 3),
              retryNesting(() -> oracle.map(egin, userMembers), 3),
              retryNesting(() -> oracle.map(fuat, managerMembers), 3),
              retryNesting(() -> oracle.map(gl, managerMembers), 3),
              retryNesting(() -> oracle.map(hakan, technicianMembers), 3),
              retryNesting(() -> oracle.map(irmak, technicianMembers), 3),
              retryNesting(() -> oracle.map(abcTechMembers, technicianMembers), 3),
              retryNesting(() -> oracle.map(flaggedTechnicianMembers, technicianMembers), 3),
              retryNesting(() -> oracle.map(jale, abcTechMembers), 3)).get(60, TimeUnit.SECONDS);

        // Protected resource namespace
        var docNs = Oracle.namespace("Document");
        // Permission
        var view = docNs.relation("View");
        // Protected Object
        var object123View = docNs.object("123", view);

        // Users can View Document 123
        Assertion tuple = userMembers.assertion(object123View);
        retryNesting(() -> oracle.add(tuple), 3).get();

        // Direct subjects that can View the document
        var viewers = oracle.read(object123View);
        assertEquals(1, viewers.size());
        assertTrue(viewers.contains(userMembers), "Should contain: " + userMembers);

        // Direct objects that can User member can view
        var viewable = oracle.read(userMembers);
        assertEquals(1, viewable.size());
        assertTrue(viewable.contains(object123View), "Should contain: " + object123View);

        // Assert flagged technicians can directly view the document
        Assertion grantTechs = flaggedTechnicianMembers.assertion(object123View);
        retryNesting(() -> oracle.add(grantTechs), 3).get();

        // Now have 2 direct subjects that can view the doc
        viewers = oracle.read(object123View);
        assertEquals(2, viewers.size());
        assertTrue(viewers.contains(userMembers), "Should contain: " + userMembers);
        assertTrue(viewers.contains(flaggedTechnicianMembers), "Should contain: " + flaggedTechnicianMembers);

        // flagged has direct view
        viewable = oracle.read(flaggedTechnicianMembers);
        assertEquals(1, viewable.size());
        assertTrue(viewable.contains(object123View), "Should contain: " + object123View);

        // Filter direct on flagged relation
        var flaggedViewers = oracle.read(flag, object123View);
        assertEquals(1, flaggedViewers.size());
        assertTrue(flaggedViewers.contains(flaggedTechnicianMembers), "Should contain: " + flaggedTechnicianMembers);

        // Transitive subjects that can view the document
        var inferredViewers = oracle.expand(object123View);
        assertEquals(14, inferredViewers.size());
        for (var s : Arrays.asList(ali, jale, egin, irmak, hakan, gl, fuat, can, burcu, managerMembers,
                                   technicianMembers, abcTechMembers, userMembers, flaggedTechnicianMembers)) {
            assertTrue(inferredViewers.contains(s), "Should contain: " + s);
        }

        // Transitive subjects filtered by flag predicate
        var inferredFlaggedViewers = oracle.expand(flag, object123View);
        assertEquals(5, inferredFlaggedViewers.size());
        for (var s : Arrays.asList(egin, ali, gl, fuat, flaggedTechnicianMembers)) {
            assertTrue(inferredFlaggedViewers.contains(s), "Should contain: " + s);
        }

        // Check some assertions
        assertTrue(oracle.check(object123View.assertion(jale)));
        assertTrue(oracle.check(object123View.assertion(egin)));
        assertFalse(oracle.check(object123View.assertion(helpDeskMembers)));

        // Remove them
        retryNesting(() -> oracle.remove(abcTechMembers, technicianMembers), 3).get();

        assertFalse(oracle.check(object123View.assertion(jale)));
        assertTrue(oracle.check(object123View.assertion(egin)));
        assertFalse(oracle.check(object123View.assertion(helpDeskMembers)));

        // Remove our assertion
        retryNesting(() -> oracle.delete(tuple), 3).get();

        assertFalse(oracle.check(object123View.assertion(jale)));
        assertFalse(oracle.check(object123View.assertion(egin)));
        assertFalse(oracle.check(object123View.assertion(helpDeskMembers)));

        // Some deletes
        retryNesting(() -> oracle.delete(abcTechMembers), 3).get();
        retryNesting(() -> oracle.delete(flaggedTechnicianMembers), 3).get();
    }

    @AfterEach
    public void after() {
        domains.forEach(Domain::stop);
        domains.clear();
        routers.forEach(r -> r.close(Duration.ofSeconds(1)));
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
        var context = new DynamicContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), CARDINALITY, 0.2, 3);
        var params = params();
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(params.getDigestAlgorithm()), entropy);

        var identities = IntStream.range(0, CARDINALITY)
                                  .mapToObj(i -> stereotomy.newIdentifier())
                                  .collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(),
                                                            controlled -> controlled));

        var sealed = FoundationSeal.newBuilder().build();
        final var group = DigestAlgorithm.DEFAULT.getOrigin();
        identities.forEach((d, id) -> {
            final var member = new ControlledIdentifierMember(id);
            var localRouter = new LocalServer(prefix, member).router(ServerConnectionCache.newBuilder().setTarget(30));
            routers.add(localRouter);
            var dbUrl = String.format("jdbc:h2:mem:sql-%s-%s;DB_CLOSE_DELAY=-1", member.getId(), UUID.randomUUID());
            var pdParams = new ProcessDomain.ProcessDomainParameters(dbUrl, Duration.ofMinutes(1),
                                                                     "jdbc:h2:mem:%s-state;DB_CLOSE_DELAY=-1".formatted(
                                                                     d), checkpointDirBase, Duration.ofMillis(10),
                                                                     0.00125, Duration.ofMinutes(1), 3, 10, 0.1);
            var domain = new ProcessDomain(group, member, pdParams, params.clone(), RuntimeParameters.newBuilder()
                                                                                                     .setFoundation(
                                                                                                     sealed)
                                                                                                     .setContext(
                                                                                                     context)
                                                                                                     .setCommunications(
                                                                                                     localRouter),
                                           new InetSocketAddress(0), ffParams, null);
            domains.add(domain);
            localRouter.start();
        });

        domains.forEach(domain -> context.activate(domain.getMember()));
    }

    @Test
    public void smoke() throws Exception {
        domains.forEach(Domain::start);
        final var activated = Utils.waitForCondition(60_000, 1_000, () -> domains.stream().allMatch(Domain::active));
        assertTrue(activated, "Domains did not fully activate: " + (domains.stream()
                                                                           .filter(c -> !c.active())
                                                                           .map(Domain::logState)
                                                                           .toList()));
        var oracle = domains.getFirst().getDelphi();
        oracle.add(new Oracle.Namespace("test")).get();
        smoke(oracle);
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
