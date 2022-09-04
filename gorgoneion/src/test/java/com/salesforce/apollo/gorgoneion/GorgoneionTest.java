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
import static org.mockito.Mockito.mock;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.google.common.base.Function;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.gorgoneion.proto.Attestation;
import com.salesfoce.apollo.gorgoneion.proto.Registration;
import com.salesfoce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.gorgoneion.Gorgoneion.Parameters;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.thoth.grpc.admission.Admission;
import com.salesforce.apollo.thoth.grpc.admission.AdmissionClient;
import com.salesforce.apollo.thoth.grpc.admission.AdmissionServer;

/**
 * @author hal.hildebrand
 *
 */
public class GorgoneionTest {

    @Test
    public void smokin() throws Exception {
        final var scheduler = Executors.newSingleThreadScheduledExecutor();
        final var exec = Executors.newSingleThreadExecutor();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        final var prefix = UUID.randomUUID().toString();
        var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        final var serverMembers = new ConcurrentSkipListMap<Digest, Member>();
        var member = new ControlledIdentifierMember(stereotomy.newIdentifier().get());
        var context = Context.<Member>newBuilder().setCardinality(1).build();
        context.activate(member);

        // Gorgoneion service comms
        var gorgonRouter = new LocalRouter(prefix, serverMembers, ServerConnectionCache.newBuilder().setTarget(2),
                                           ForkJoinPool.commonPool(), null);
        gorgonRouter.setMember(member);
        gorgonRouter.start();

        // The async abstraction of the underlying attestation verification service
        final Function<SignedAttestation, CompletableFuture<Boolean>> verifier = sa -> {
            var fs = new CompletableFuture<Boolean>();
            fs.complete(true);
            return fs;
        };
        Parameters params = Parameters.newBuilder().setVerifier(verifier).setEntropy(entropy).setExec(exec).build();

        // This is the identifier used to validate the client's KERL
        var validating = member.getIdentifier()
                               .newIdentifier(IdentifierSpecification.<SelfAddressingIdentifier>newBuilder())
                               .get();

        var gorgon = new Gorgoneion(member, validating, context, gorgonRouter, kerl, gorgonRouter, params, null);
        gorgon.start(Duration.ofMillis(5), scheduler);

        // The registering client
        var client = new ControlledIdentifierMember(stereotomy.newIdentifier().get());

        // Registering client comms
        var clientRouter = new LocalRouter(prefix, serverMembers, ServerConnectionCache.newBuilder().setTarget(2), exec,
                                           null);
        Admission admissions = mock(Admission.class);
        var clientComminications = clientRouter.create(member, context.getId(), admissions, "admissions",
                                                       r -> new AdmissionServer(r,
                                                                                clientRouter.getClientIdentityProvider(),
                                                                                params.exec(), null),
                                                       AdmissionClient.getCreate(context.getId(), null),
                                                       AdmissionClient.getLocalLoopback(admissions, member));
        clientRouter.setMember(client);
        clientRouter.start();

        // Admin client link
        var admin = clientComminications.apply(member, client);

        assertNotNull(admin);

        // Apply for registration of the client's KERL, receiving the signed nonce from
        // the server
        var signedNonce = admin.apply(Registration.newBuilder()
                                                  .setContext(context.getId().toDigeste())
                                                  .setIdentity(client.getIdentifier().getIdentifier().toIdent())
                                                  .setKerl(client.kerl().get())
                                                  .build())
                               .get();
        assertNotNull(signedNonce);
        assertNotNull(signedNonce.getNonce());
        assertEquals(client.getIdentifier().getIdentifier().toIdent(), signedNonce.getNonce().getMember());

        // Create attestation
        final var now = Instant.now();
        // Attestation document from fundamental identity service (AWS, PAL, GCM, etc)
        final var attestationDocument = Any.getDefaultInstance();
        final var attestation = Attestation.newBuilder()
                                           .setMember(client.getIdentifier().getIdentifier().toIdent())
                                           .setTimestamp(Timestamp.newBuilder()
                                                                  .setSeconds(now.getEpochSecond())
                                                                  .setNanos(now.getNano()))
                                           .setNonce(client.sign(signedNonce.toByteString()).toSig())
                                           .setAttestation(attestationDocument)
                                           .build();

        // Register with gorgoneion, receive validations of client's KERL
        Validations validation = admin.register(SignedAttestation.newBuilder()
                                                                 .setContext(context.getId().toDigeste())
                                                                 .setAttestation(attestation)
                                                                 .setSignature(client.sign(attestation.toByteString())
                                                                                     .toSig())
                                                                 .build())
                                      .get(1, TimeUnit.SECONDS);
        assertNotNull(validation);
        assertNotEquals(Validations.getDefaultInstance(), validation);
        assertEquals(1, validation.getValidationsCount());
        assertEquals(client.getIdentifier().getCoordinates().toEventCoords(), validation.getCoordinates());
    }
}
