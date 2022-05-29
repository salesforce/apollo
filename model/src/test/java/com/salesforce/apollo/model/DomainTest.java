/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.choam.proto.Foundation;
import com.salesfoce.apollo.choam.proto.FoundationSeal;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.delphinius.Oracle.Assertion;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class DomainTest {
    private static final int    CARDINALITY     = 5;
    private static final Digest GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());

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
        var viewers = oracle.read(object123View);
        assertEquals(1, viewers.size());
        assertTrue(viewers.contains(userMembers), "Should contain: " + userMembers);

        // Direct objects that can User member can view
        var viewable = oracle.read(userMembers);
        assertEquals(1, viewable.size());
        assertTrue(viewable.contains(object123View), "Should contain: " + object123View);

        // Assert flagged technicians can directly view the document
        Assertion grantTechs = flaggedTechnicianMembers.assertion(object123View);
        oracle.add(grantTechs).get();

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
        oracle.remove(abcTechMembers, technicianMembers).get();

        assertFalse(oracle.check(object123View.assertion(jale)));
        assertTrue(oracle.check(object123View.assertion(egin)));
        assertFalse(oracle.check(object123View.assertion(helpDeskMembers)));

        // Remove our assertion
        oracle.delete(tuple).get();

        assertFalse(oracle.check(object123View.assertion(jale)));
        assertFalse(oracle.check(object123View.assertion(egin)));
        assertFalse(oracle.check(object123View.assertion(helpDeskMembers)));

        // Some deletes
        oracle.delete(abcTechMembers).get();
        oracle.delete(flaggedTechnicianMembers).get();
    }

    private final ArrayList<Domain> domains = new ArrayList<>();

    private final ArrayList<LocalRouter> routers = new ArrayList<>();

    @AfterEach
    public void after() {
        domains.forEach(n -> n.stop());
        domains.clear();
        routers.forEach(r -> r.close());
        routers.clear();
    }

    @BeforeEach
    public void before() throws SQLException {
        final var prefix = UUID.randomUUID().toString();
        Path checkpointDirBase = Path.of("target", "ct-chkpoints-" + Entropy.nextBitsStreamLong());
        Utils.clean(checkpointDirBase.toFile());
        var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), CARDINALITY, 0.2, 3);
        var params = params();
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(params.getDigestAlgorithm()),
                                            new SecureRandom());

        var identities = IntStream.range(0, CARDINALITY)
                                  .parallel()
                                  .mapToObj(i -> stereotomy.newIdentifier().get())
                                  .map(ci -> {
                                      @SuppressWarnings("unchecked")
                                      var casted = (ControlledIdentifier<SelfAddressingIdentifier>) ci;
                                      return casted;
                                  })
                                  .collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(),
                                                            controlled -> controlled));

        var foundation = Foundation.newBuilder();
        identities.keySet().forEach(d -> foundation.addMembership(d.toDigeste()));
        var sealed = FoundationSeal.newBuilder().setFoundation(foundation).build();
        final var group = DigestAlgorithm.DEFAULT.getOrigin();
        identities.forEach((member, id) -> {
            var localRouter = new LocalRouter(prefix, ServerConnectionCache.newBuilder().setTarget(30),
                                              Executors.newFixedThreadPool(1), null);
            routers.add(localRouter);
            var domain = new ProcessDomain(group, id, params, "jdbc:h2:mem:", checkpointDirBase,
                                           RuntimeParameters.newBuilder()
                                                            .setFoundation(sealed)
                                                            .setScheduler(Executors.newSingleThreadScheduledExecutor())
                                                            .setContext(context)
                                                            .setExec(Executors.newSingleThreadExecutor())
                                                            .setCommunications(localRouter),
                                           new InetSocketAddress(0));
            domains.add(domain);
            localRouter.setMember(domain.getMember());
            localRouter.start();
        });

        domains.forEach(domain -> context.activate(domain.getMember()));
    }

    @Test
    public void smoke() throws Exception {
        domains.forEach(n -> n.start());
        assertTrue(Utils.waitForCondition(60_000, 1_000, () -> domains.stream().filter(d -> !d.active()).count() == 0),
                   "Domains did not fully activate");
        var oracle = domains.get(0).getDelphi();
        oracle.add(new Oracle.Namespace("test")).get();
        smoke(oracle);
    }

    private Builder params() {
        var params = Parameters.newBuilder()
                               .setSynchronizationCycles(2)
                               .setSynchronizeTimeout(Duration.ofSeconds(1))
                               .setGenesisViewId(GENESIS_VIEW_ID)
                               .setGossipDuration(Duration.ofMillis(10))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(20))
                                                              .setBatchInterval(Duration.ofMillis(100))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(3000)
                                                              .build())
                               .setCheckpointBlockDelta(200);
        params.getProducer().ethereal().setNumberOfEpochs(5);
        return params;
    }
}
