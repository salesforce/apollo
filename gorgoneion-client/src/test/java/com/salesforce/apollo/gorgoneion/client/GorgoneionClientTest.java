/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.client;

import com.google.protobuf.Any;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.gorgoneion.Gorgoneion;
import com.salesforce.apollo.gorgoneion.Parameters;
import com.salesforce.apollo.gorgoneion.client.client.comm.Admissions;
import com.salesforce.apollo.gorgoneion.client.client.comm.AdmissionsClient;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsServer;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsService;
import com.salesforce.apollo.gorgoneion.proto.SignedNonce;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author hal.hildebrand
 */
public class GorgoneionClientTest {

    private Router gorgonRouter;
    private Router clientRouter;

    @Test
    public void clientSmoke() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        final var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        var stereotomy = new StereotomyImpl(new MemKeyStore(), kerl, entropy);
        final var prefix = UUID.randomUUID().toString();
        var member = new ControlledIdentifierMember(stereotomy.newIdentifier());
        var context = Context.<Member>newBuilder().setCardinality(1).build();
        context.activate(member);

        // Gorgoneion service comms
        gorgonRouter = new LocalServer(prefix, member).router(ServerConnectionCache.newBuilder().setTarget(2));
        gorgonRouter.start();

        // The kerl observer to publish admitted client KERLs to
        var observer = mock(ProtoEventObserver.class);
        final var parameters = Parameters.newBuilder().setKerl(kerl).build();
        @SuppressWarnings("unused")
        var gorgon = new Gorgoneion(parameters, member, context, observer, gorgonRouter,
                                    Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory()), null);

        // The registering client
        var client = new ControlledIdentifierMember(stereotomy.newIdentifier());

        // Registering client comms
        clientRouter = new LocalServer(prefix, client).router(ServerConnectionCache.newBuilder().setTarget(2));
        var admissions = mock(AdmissionsService.class);
        var clientComminications = clientRouter.create(client, context.getId(), admissions, ":admissions",
                                                       r -> new AdmissionsServer(
                                                       clientRouter.getClientIdentityProvider(), r, null),
                                                       AdmissionsClient.getCreate(null),
                                                       Admissions.getLocalLoopback(client));
        clientRouter.start();

        // Admin client link
        var admin = clientComminications.connect(member);

        assertNotNull(admin);
        Function<SignedNonce, Any> attester = sn -> Any.getDefaultInstance();

        var gorgoneionClient = new GorgoneionClient(client, attester, parameters.clock(), admin);

        var invitation = gorgoneionClient.apply(Duration.ofSeconds(60));

        gorgonRouter.close(Duration.ofSeconds(1));
        clientRouter.close(Duration.ofSeconds(1));

        assertNotNull(invitation);
        assertNotEquals(Validations.getDefaultInstance(), invitation);
        assertEquals(1, invitation.getValidationsCount());

        // Verify client KERL published

        // Because this is a minimal test, the notarization is not published
        //        verify(observer, times(3)).publish(client.kerl().get(), Collections.singletonList(invitation));
    }

    @AfterEach
    public void closeRouters() {
        if (gorgonRouter != null) {
            gorgonRouter.close(Duration.ofSeconds(3));
        }
        if (clientRouter != null) {
            clientRouter.close(Duration.ofSeconds(3));
        }
    }

    @Test
    public void multiSmoke() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        final var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        var stereotomy = new StereotomyImpl(new MemKeyStore(), kerl, entropy);
        final var prefix = UUID.randomUUID().toString();
        final var members = IntStream.range(0, 10)
                                     .mapToObj(i -> new ControlledIdentifierMember(stereotomy.newIdentifier()))
                                     .toList();

        var countdown = new CountDownLatch(3);
        // The kerl observer to publish admitted client KERLs to
        var observer = mock(ProtoEventObserver.class);
        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                countdown.countDown();
                return null;
            }
        }).when(observer).publish(Mockito.any(), Mockito.anyList());

        var context = Context.<Member>newBuilder().setCardinality(members.size()).build();
        for (ControlledIdentifierMember member : members) {
            context.activate(member);
        }
        final var parameters = Parameters.newBuilder().setKerl(kerl).build();
        final var gorgoneions = members.stream()
                                       .map(m -> {
                                           final var router = new LocalServer(prefix, m).router(
                                           ServerConnectionCache.newBuilder().setTarget(2));
                                           router.start();
                                           return router;
                                       })
                                       .map(r -> new Gorgoneion(parameters, (ControlledIdentifierMember) r.getFrom(),
                                                                context, observer, r,
                                                                Executors.newScheduledThreadPool(2, Thread.ofVirtual()
                                                                                                          .factory()),
                                                                null))
                                       .toList();

        // The registering client
        var client = new ControlledIdentifierMember(stereotomy.newIdentifier());

        // Registering client comms
        var clientRouter = new LocalServer(prefix, client).router(ServerConnectionCache.newBuilder().setTarget(2));
        AdmissionsService admissions = mock(AdmissionsService.class);
        var clientComminications = clientRouter.create(client, context.getId(), admissions, ":admissions",
                                                       r -> new AdmissionsServer(
                                                       clientRouter.getClientIdentityProvider(), r, null),
                                                       AdmissionsClient.getCreate(null),
                                                       Admissions.getLocalLoopback(client));
        clientRouter.start();

        // Admin client link
        var admin = clientComminications.connect(members.get(0));

        assertNotNull(admin);
        Function<SignedNonce, Any> attester = sn -> {
            return Any.getDefaultInstance();
        };

        var gorgoneionClient = new GorgoneionClient(client, attester, parameters.clock(), admin);
        var invitation = gorgoneionClient.apply(Duration.ofSeconds(2_000));
        assertNotNull(invitation);
        assertNotEquals(Validations.getDefaultInstance(), invitation);
        assertTrue(invitation.getValidationsCount() >= context.majority());

        assertTrue(countdown.await(1, TimeUnit.SECONDS));
    }
}
