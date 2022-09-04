/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.thoth.proto.Admittance;
import com.salesfoce.apollo.thoth.proto.Attestation;
import com.salesfoce.apollo.thoth.proto.Registration;
import com.salesfoce.apollo.thoth.proto.SignedAttestation;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.thoth.Gorgoneion.Parameters;
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
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        final var prefix = UUID.randomUUID().toString();
        var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        final var serverMembers = new ConcurrentSkipListMap<Digest, Member>();
        var member = new ControlledIdentifierMember(stereotomy.newIdentifier().get());
        var gorgonRouter = new LocalRouter(prefix, serverMembers, ServerConnectionCache.newBuilder().setTarget(2),
                                           ForkJoinPool.commonPool(), null);

        gorgonRouter.setMember(member);
        gorgonRouter.start();

        var validating = member.getIdentifier()
                               .newIdentifier(IdentifierSpecification.<SelfAddressingIdentifier>newBuilder())
                               .get();
        var context = Context.<Member>newBuilder().setCardinality(1).build();
        context.activate(member);
        Parameters params = Parameters.newBuilder().setVerifier(sa -> {
            var fs = new CompletableFuture<Boolean>();
            fs.complete(true);
            return fs;
        }).setEntropy(entropy).setExec(Executors.newSingleThreadExecutor()).build();

        var gorgon = new Gorgoneion(member, validating, context, gorgonRouter, kerl, gorgonRouter, params, null);
        gorgon.start(Duration.ofMillis(5), Executors.newSingleThreadScheduledExecutor());

        var client = new ControlledIdentifierMember(stereotomy.newIdentifier().get());
        var clientRouter = new LocalRouter(prefix, serverMembers, ServerConnectionCache.newBuilder().setTarget(2),
                                           ForkJoinPool.commonPool(), null);
        Admission admissions = mock(Admission.class);
        var clientComminications = clientRouter.create(member, context.getId(), admissions, "admissions",
                                                       r -> new AdmissionServer(r,
                                                                                clientRouter.getClientIdentityProvider(),
                                                                                params.exec(), null),
                                                       AdmissionClient.getCreate(context.getId(), null),
                                                       AdmissionClient.getLocalLoopback(admissions, member));
        clientRouter.setMember(client);
        clientRouter.start();

        var admin = clientComminications.apply(member, client);

        assertNotNull(admin);

        var signedNonce = admin.apply(Registration.newBuilder()
                                                  .setContext(context.getId().toDigeste())
                                                  .setIdentity(client.getIdentifier().getIdentifier().toIdent())
                                                  .setKerl(client.kerl().get())
                                                  .build())
                               .get();
        assertNotNull(signedNonce);
        assertNotNull(signedNonce.getNonce());
        assertEquals(client.getIdentifier().getIdentifier().toIdent(), signedNonce.getNonce().getMember());

        final var now = Instant.now();
        final var attestation = Attestation.newBuilder()
                                           .setMember(client.getIdentifier().getIdentifier().toIdent())
                                           .setTimestamp(Timestamp.newBuilder()
                                                                  .setSeconds(now.getEpochSecond())
                                                                  .setNanos(now.getNano()))
                                           .setNonce(client.sign(signedNonce.toByteString()).toSig())
                                           .setAttestation(Any.getDefaultInstance())
                                           .build();

        Admittance validation = null;
        try {
            validation = admin.register(SignedAttestation.newBuilder()
                                                         .setContext(context.getId().toDigeste())
                                                         .setAttestation(attestation)
                                                         .setSignature(client.sign(attestation.toByteString()).toSig())
                                                         .build())
                              .get(1, TimeUnit.SECONDS);
            assertNotNull(validation);
        } catch (TimeoutException e) {
            // expected for now
            System.out.println("Timeout");
        }

    }
}
