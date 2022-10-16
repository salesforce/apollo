/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.gorgoneion.proto.Application;
import com.salesfoce.apollo.gorgoneion.proto.Attestation;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.Invitation;
import com.salesfoce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesforce.apollo.archipeligo.LocalServer;
import com.salesforce.apollo.archipeligo.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.gorgoneion.comm.admissions.Admissions;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsClient;
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
public class GorgoneionTest {

    @Test
    public void clientSmoke() throws Exception {
        final var exec = Executors.newSingleThreadExecutor();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        final var prefix = UUID.randomUUID().toString();
        var member = new ControlledIdentifierMember(stereotomy.newIdentifier().get());
        var context = Context.<Member>newBuilder().setCardinality(1).build();
        context.activate(member);

        // Gorgoneion service comms
        var gorgonRouter = new LocalServer(prefix, member,
                                           ForkJoinPool.commonPool()).router(ServerConnectionCache.newBuilder().setTarget(2), ForkJoinPool.commonPool());
        gorgonRouter.start();

        // The kerl observer to publish admitted client KERLs to
        var observer = mock(ProtoEventObserver.class);
        final var parameters = Parameters.newBuilder().build();
        @SuppressWarnings("unused")
        var gorgon = new Gorgoneion(parameters, member, context, observer, gorgonRouter,
                                    Executors.newSingleThreadScheduledExecutor(), null, ForkJoinPool.commonPool());

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
        var admin = clientComminications.connect(member);

        assertNotNull(admin);
        Function<SignedNonce, CompletableFuture<Any>> attester = sn -> {
            var fs = new CompletableFuture<Any>();
            fs.complete(Any.getDefaultInstance());
            return fs;
        };

        var gorgoneionClient = new GorgoneionClient(client, context.getId(), attester, parameters.clock(), admin);

        Invitation invitation = gorgoneionClient.apply(Duration.ofSeconds(2)).get(3, TimeUnit.SECONDS);
        assertNotNull(invitation);
        assertNotEquals(Invitation.getDefaultInstance(), invitation);
        assertEquals(1, invitation.getValidations().getValidationsCount());

        // Verify client KERL published
        verify(observer, times(3)).publish(client.kerl().get(), Collections.singletonList(invitation.getValidations()));
    }

    @Test
    public void multiSmoke() throws Exception {
        final var exec = Executors.newSingleThreadExecutor();
        final var scheduler = Executors.newSingleThreadScheduledExecutor();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
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

        var context = Context.<Member>newBuilder().setCardinality(members.size()).build();
        members.forEach(m -> context.activate(m));
        final var parameters = Parameters.newBuilder().build();
        @SuppressWarnings("unused")
        final var gorgons = members.stream().map(m -> {
            final var router = new LocalServer(prefix, m,
                                               ForkJoinPool.commonPool()).router(ServerConnectionCache.newBuilder().setTarget(2), ForkJoinPool.commonPool());
            router.start();
            return router;
        })
                                   .map(r -> new Gorgoneion(parameters, (ControlledIdentifierMember) r.getFrom(),
                                                            context, observer, r, scheduler, null,
                                                            ForkJoinPool.commonPool()))
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
            var fs = new CompletableFuture<Any>();
            fs.complete(Any.getDefaultInstance());
            return fs;
        };

        var gorgoneionClient = new GorgoneionClient(client, context.getId(), attester, parameters.clock(), admin);

        final var apply = gorgoneionClient.apply(Duration.ofSeconds(2));
        Invitation invitation = apply.get(3, TimeUnit.SECONDS);
        assertNotNull(invitation);
        assertNotEquals(Invitation.getDefaultInstance(), invitation);
        assertTrue(invitation.getValidations().getValidationsCount() >= context.majority());

        // Verify client KERL published
        verify(observer, times(3)).publish(client.kerl().get(), Collections.singletonList(invitation.getValidations()));
    }

    @Test
    public void smokin() throws Exception {
        final var exec = Executors.newSingleThreadExecutor();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        final var prefix = UUID.randomUUID().toString();
        var member = new ControlledIdentifierMember(stereotomy.newIdentifier().get());
        var context = Context.<Member>newBuilder().setCardinality(1).build();
        context.activate(member);

        // Gorgoneion service comms
        var gorgonRouter = new LocalServer(prefix, member, exec).router(ServerConnectionCache.newBuilder().setTarget(2),
                                                                        ForkJoinPool.commonPool());
        gorgonRouter.start();

        // The kerl observer to publish admitted client KERLs to
        var observer = mock(ProtoEventObserver.class);
        @SuppressWarnings("unused")
        var gorgon = new Gorgoneion(Parameters.newBuilder().build(), member, context, observer, gorgonRouter,
                                    Executors.newSingleThreadScheduledExecutor(), null, ForkJoinPool.commonPool());

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
        var admin = clientComminications.connect(member);

        assertNotNull(admin);

        // Apply for registration of the client's KERL, receiving the signed nonce from
        // the server
        final KERL_ kerl = client.kerl().get();
        ListenableFuture<SignedNonce> fs = admin.apply(Application.newBuilder()
                                                                  .setContext(context.getId().toDigeste())
                                                                  .setKerl(kerl)
                                                                  .build(),
                                                       Duration.ofSeconds(1));
        assertNotNull(fs);
        var signedNonce = fs.get();
        assertNotNull(signedNonce.getNonce());
        assertEquals(client.getIdentifier().getIdentifier().toIdent(), signedNonce.getNonce().getMember());

        // Create attestation
        final var now = Instant.now();
        // Attestation document from fundamental identity service (AWS, PAL, GCM, etc)
        final var attestationDocument = Any.getDefaultInstance();
        final var attestation = Attestation.newBuilder()
                                           .setTimestamp(Timestamp.newBuilder()
                                                                  .setSeconds(now.getEpochSecond())
                                                                  .setNanos(now.getNano()))
                                           .setNonce(client.sign(signedNonce.toByteString()).toSig())
                                           .setKerl(client.kerl().get())
                                           .setAttestation(attestationDocument)
                                           .build();

        Invitation invitation = admin.register(Credentials.newBuilder()
                                                          .setContext(context.getId().toDigeste())
                                                          .setAttestation(SignedAttestation.newBuilder()
                                                                                           .setAttestation(attestation)
                                                                                           .setSignature(client.sign(attestation.toByteString())
                                                                                                               .toSig())
                                                                                           .build())
                                                          .setNonce(signedNonce)
                                                          .build(),
                                               Duration.ofSeconds(1))
                                     .get(1, TimeUnit.SECONDS);
        assertNotNull(invitation);
        assertNotEquals(Invitation.getDefaultInstance(), invitation);
        assertEquals(1, invitation.getValidations().getValidationsCount());

        // Verify client KERL published
        verify(observer, times(3)).publish(kerl, Collections.singletonList(invitation.getValidations()));
    }
}
