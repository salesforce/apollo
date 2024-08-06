/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.context.DynamicContext;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsServer;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsService;
import com.salesforce.apollo.gorgoneion.proto.Attestation;
import com.salesforce.apollo.gorgoneion.proto.Credentials;
import com.salesforce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.event.proto.KERL_;
import com.salesforce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;
import com.salesforce.apollo.test.proto.ByteMessage;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * @author hal.hildebrand
 */
public class GorgoneionTest {

    @Test
    public void smokin() throws Exception {
        final var testMessage = ByteMessage.newBuilder().setContents(ByteString.copyFromUtf8("hello world")).build();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        final var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        var stereotomy = new StereotomyImpl(new MemKeyStore(), kerl, entropy);
        final var prefix = UUID.randomUUID().toString();
        var member = new ControlledIdentifierMember(stereotomy.newIdentifier());
        var b = DynamicContext.newBuilder();
        b.setCardinality(10);
        var context = b.build();
        context.activate(member);

        // Gorgoneion service comms
        var gorgonRouter = new LocalServer(prefix, member).router(ServerConnectionCache.newBuilder().setTarget(2));
        gorgonRouter.start();

        // The kerl observer to publish admitted client KERLs to
        var observer = mock(ProtoEventObserver.class);
        @SuppressWarnings("unused")
        var gorgon = new Gorgoneion(true, t -> true, (c, v) -> Any.pack(testMessage),
                                    Parameters.newBuilder().setKerl(kerl).build(), member, context, observer,
                                    gorgonRouter, null);

        // The registering client
        var client = new ControlledIdentifierMember(stereotomy.newIdentifier());

        // Registering client comms
        var clientRouter = new LocalServer(prefix, client).router(ServerConnectionCache.newBuilder().setTarget(2));
        AdmissionsService admissions = mock(AdmissionsService.class);
        var clientCommunications = clientRouter.create(client, context.getId(), admissions, ":admissions",
                                                       r -> new AdmissionsServer(
                                                       clientRouter.getClientIdentityProvider(), r, null),
                                                       AdmissionsClient.getCreate(),
                                                       Admissions.getLocalLoopback(client));
        clientRouter.start();

        // Admin client link
        var admin = clientCommunications.connect(member);

        assertNotNull(admin);

        // Apply for registration of the client's KERL, receiving the signed nonce from
        // the server
        final KERL_ cKerl = client.kerl();
        var fs = admin.apply(cKerl, Duration.ofSeconds(1));
        assertNotNull(fs);
        assertNotNull(fs.getNonce());
        assertEquals(client.getIdentifier().getIdentifier().toIdent(), fs.getNonce().getMember());

        // Create attestation
        final var now = Instant.now();
        // Attestation document from fundamental identity service (AWS, PAL, GCM, etc.)
        final var attestationDocument = Any.getDefaultInstance();
        final var attestation = Attestation.newBuilder()
                                           .setTimestamp(Timestamp.newBuilder()
                                                                  .setSeconds(now.getEpochSecond())
                                                                  .setNanos(now.getNano()))
                                           .setNonce(client.sign(fs.toByteString()).toSig())
                                           .setKerl(client.kerl())
                                           .setAttestation(attestationDocument)
                                           .build();

        var establishment = admin.register(Credentials.newBuilder()
                                                      .setAttestation(SignedAttestation.newBuilder()
                                                                                       .setAttestation(attestation)
                                                                                       .setSignature(client.sign(
                                                                                                           attestation.toByteString())
                                                                                                           .toSig())
                                                                                       .build())
                                                      .setNonce(fs)
                                                      .build(), Duration.ofSeconds(1));
        gorgonRouter.close(Duration.ofSeconds(0));
        clientRouter.close(Duration.ofSeconds(0));
        assertNotNull(establishment);
        assertNotEquals(Validations.getDefaultInstance(), establishment);
        assertEquals(1, establishment.getValidations().getValidationsCount());
        assertEquals(testMessage.getContents(),
                     establishment.getProvisioning().unpack(ByteMessage.class).getContents());
        // Verify client KERL published

        // Because this is a minimal test, the notarization is not published
        //        verify(observer, times(3)).publish(cKerl, Collections.singletonList(establishment));
    }
}
