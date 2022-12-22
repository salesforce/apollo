/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.protobuf.Any;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.gorgoneion.Gorgoneion;
import com.salesforce.apollo.gorgoneion.Parameters;
import com.salesforce.apollo.gorgoneion.client.client.comm.Admissions;
import com.salesforce.apollo.gorgoneion.client.client.comm.AdmissionsClient;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsServer;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsService;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;

/**
 * @author hal.hildebrand
 *
 */
public class GorgoneionClientTest {

    @Test
    public void clientSmoke() throws Exception {
        final var exec = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        final var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        var stereotomy = new StereotomyImpl(new MemKeyStore(), kerl, entropy);
        final var prefix = UUID.randomUUID().toString();
        var member = new ControlledIdentifierMember(stereotomy.newIdentifier().get());
        var context = Context.<Member>newBuilder().setCardinality(1).build();
        context.activate(member);

        // Gorgoneion service comms
        var gorgonRouter = new LocalServer(prefix, member,
                                           Executors.newFixedThreadPool(2, Thread.ofVirtual().factory()))
                                                                                                         .router(ServerConnectionCache.newBuilder()
                                                                                                                                      .setTarget(2),
                                                                                                                 Executors.newFixedThreadPool(2,
                                                                                                                                              Thread.ofVirtual()
                                                                                                                                                    .factory()));
        gorgonRouter.start();

        // The kerl observer to publish admitted client KERLs to
        var observer = mock(ProtoEventObserver.class);
        final var parameters = Parameters.newBuilder().setKerl(kerl).build();
        @SuppressWarnings("unused")
        var gorgon = new Gorgoneion(parameters, member, context, observer, gorgonRouter,
                                    Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory()), null,
                                    Executors.newFixedThreadPool(2, Thread.ofVirtual().factory()));

        // The registering client
        var client = new ControlledIdentifierMember(stereotomy.newIdentifier().get());

        // Registering client comms
        var clientRouter = new LocalServer(prefix, client, exec).router(ServerConnectionCache.newBuilder().setTarget(2),
                                                                        exec);
        var admissions = mock(AdmissionsService.class);
        var clientComminications = clientRouter.create(client, context.getId(), admissions, ":admissions",
                                                       r -> new AdmissionsServer(clientRouter.getClientIdentityProvider(),
                                                                                 r, null),
                                                       AdmissionsClient.getCreate(null),
                                                       Admissions.getLocalLoopback(client));
        clientRouter.start();

        // Admin client link
        var admin = clientComminications.connect(member);

        assertNotNull(admin);
        Function<SignedNonce, CompletableFuture<Any>> attester = sn -> {
            var fs = new CompletableFuture<Any>();
            fs.complete(Any.getDefaultInstance());
            return fs;
        };

        var gorgoneionClient = new GorgoneionClient(client, attester, parameters.clock(), admin);

        var invitation = gorgoneionClient.apply(Duration.ofSeconds(60)).get(120, TimeUnit.SECONDS);

        gorgonRouter.close(Duration.ofSeconds(1));
        clientRouter.close(Duration.ofSeconds(1));

        assertNotNull(invitation);
        assertNotEquals(Validations.getDefaultInstance(), invitation);
        assertEquals(1, invitation.getValidationsCount());

        // Verify client KERL published

        // Because this is a minimal test, the notarization is not published
//        verify(observer, times(3)).publish(client.kerl().get(), Collections.singletonList(invitation));
    }

    @Test
    public void multiSmoke() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        final var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        var stereotomy = new StereotomyImpl(new MemKeyStore(), kerl, entropy);
        final var prefix = UUID.randomUUID().toString();
        final var members = IntStream.range(0, 10).mapToObj(i -> {
            try {
                return new ControlledIdentifierMember(stereotomy.newIdentifier().get());
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }).toList();

        // The kerl observer to publish admitted client KERLs to
        var observer = mock(ProtoEventObserver.class);
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        when(observer.publish(Mockito.any(), Mockito.any())).thenReturn(fs);

        var context = Context.<Member>newBuilder().setCardinality(members.size()).build();
        members.forEach(m -> context.activate(m));
        final var parameters = Parameters.newBuilder().setKerl(kerl).build();
        final var exec = Executors.newVirtualThreadPerTaskExecutor();
        @SuppressWarnings("unused")
        final var gorgons = members.stream().map(m -> {
            final var router = new LocalServer(prefix, m, exec).router(ServerConnectionCache.newBuilder().setTarget(2),
                                                                       exec);
            router.start();
            return router;
        })
                                   .map(r -> new Gorgoneion(parameters, (ControlledIdentifierMember) r.getFrom(),
                                                            context, observer, r,
                                                            Executors.newScheduledThreadPool(2,
                                                                                             Thread.ofVirtual()
                                                                                                   .factory()),
                                                            null, exec))
                                   .toList();

        // The registering client
        var client = new ControlledIdentifierMember(stereotomy.newIdentifier().get());

        // Registering client comms
        var clientRouter = new LocalServer(prefix, client, exec).router(ServerConnectionCache.newBuilder().setTarget(2),
                                                                        exec);
        AdmissionsService admissions = mock(AdmissionsService.class);
        var clientComminications = clientRouter.create(client, context.getId(), admissions, ":admissions",
                                                       r -> new AdmissionsServer(clientRouter.getClientIdentityProvider(),
                                                                                 r, null),
                                                       AdmissionsClient.getCreate(null),
                                                       Admissions.getLocalLoopback(client));
        clientRouter.start();

        // Admin client link
        var admin = clientComminications.connect(members.get(0));

        assertNotNull(admin);
        Function<SignedNonce, CompletableFuture<Any>> attester = sn -> {
            var complete = new CompletableFuture<Any>();
            complete.complete(Any.getDefaultInstance());
            return complete;
        };

        var gorgoneionClient = new GorgoneionClient(client, attester, parameters.clock(), admin);

        final var apply = gorgoneionClient.apply(Duration.ofSeconds(2_000));
        var invitation = apply.get(30000, TimeUnit.SECONDS);
        assertNotNull(invitation);
        assertNotEquals(Validations.getDefaultInstance(), invitation);
        assertTrue(invitation.getValidationsCount() >= context.majority());

        // Verify client KERL published
        verify(observer, times(3)).publish(client.kerl().get(), Collections.singletonList(invitation));
    }
}
