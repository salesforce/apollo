/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import org.junit.jupiter.api.Test;

/**
 * @author hal.hildebrand
 *
 */
public class GorgoneionTest {

    @Test
    public void smokin() throws Exception {
//        final var scheduler = Executors.newSingleThreadScheduledExecutor();
//        final var exec = Executors.newSingleThreadExecutor();
//        var entropy = SecureRandom.getInstance("SHA1PRNG");
//        entropy.setSeed(new byte[] { 6, 6, 6 });
//        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
//        final var prefix = UUID.randomUUID().toString();
//        final var serverMembers = new ConcurrentSkipListMap<Digest, Member>();
//        var member = new ControlledIdentifierMember(stereotomy.newIdentifier().get());
//        var context = Context.<Member>newBuilder().setCardinality(1).build();
//        context.activate(member);
//
//        // Gorgoneion service comms
//        var gorgonRouter = new LocalRouter(prefix, serverMembers, ServerConnectionCache.newBuilder().setTarget(2),
//                                           ForkJoinPool.commonPool(), null);
//        gorgonRouter.setMember(member);
//        gorgonRouter.start();
//
//        // The async abstraction of the underlying attestation verification service
//        final Function<SignedAttestation, CompletableFuture<Boolean>> verifier = sa -> {
//            var fs = new CompletableFuture<Boolean>();
//            fs.complete(true);
//            return fs;
//        };
//        Parameters params = Parameters.newBuilder().setVerifier(verifier).build();
//
//        // This is the identifier used to validate the client's KERL
//        var validating = member.getIdentifier()
//                               .newIdentifier(IdentifierSpecification.<SelfAddressingIdentifier>newBuilder())
//                               .get();
//
//        // The kerl observer to publish admitted client KERLs to
//        var observer = mock(ProtoEventObserver.class);
////        var gorgon = new Gorgoneion(member, validating, context, gorgonRouter, observer, gorgonRouter, params, null);
////        gorgon.start(Duration.ofMillis(5), scheduler);
//
//        // The registering client
//        var client = new ControlledIdentifierMember(stereotomy.newIdentifier().get());
//
//        // Registering client comms
//        var clientRouter = new LocalRouter(prefix, serverMembers, ServerConnectionCache.newBuilder().setTarget(2), exec,
//                                           null);
//        AdmissionsService admissions = mock(AdmissionsService.class);
//        Function<RoutableService<AdmissionsService>, AdmissionsServer> factory = r -> new AdmissionsServer(clientRouter.getClientIdentityProvider(),
//                                                                                                           r, exec,
//                                                                                                           null);
//        var clientComminications = clientRouter.create(member, context.getId(), admissions, "admissions", factory,
//                                                       AdmissionsClient.getCreate(null),
//                                                       Admissions.getLocalLoopback(member));
//        clientRouter.setMember(client);
//        clientRouter.start();
//
//        // Admin client link
//        var admin = clientComminications.apply(member, client);
//
//        assertNotNull(admin);
//
//        // Apply for registration of the client's KERL, receiving the signed nonce from
//        // the server
//        ListenableFuture<SignedNonce> fs = admin.apply(Registration.newBuilder()
//                                                                   .setContext(context.getId().toDigeste())
//                                                                   .setKerl(client.kerl().get())
//                                                                   .build())
//                                                .get();
//        assertNotNull(fs);
//        var signedNonce = fs.get();
//        assertNotNull(signedNonce.getNonce());
//        assertEquals(client.getIdentifier().getIdentifier().toIdent(), signedNonce.getNonce().getMember());
//
//        // Create attestation
//        final var now = Instant.now();
//        // Attestation document from fundamental identity service (AWS, PAL, GCM, etc)
//        final var attestationDocument = Any.getDefaultInstance();
//        final var attestation = Attestation.newBuilder()
//                                           .setTimestamp(Timestamp.newBuilder()
//                                                                  .setSeconds(now.getEpochSecond())
//                                                                  .setNanos(now.getNano()))
//                                           .setNonce(client.sign(signedNonce.toByteString()).toSig())
//                                           .setAttestation(attestationDocument)
//                                           .build();
//
//        // Register with gorgoneion, receive validations of client's KERL
//        Validations validation = admin.register(Credentials.newBuilder()
//                                                           .setContext(context.getId().toDigeste())
//                                                           .setAttestation(SignedAttestation.newBuilder()
//                                                                                            .setAttestation(attestation)
//                                                                                            .build())
//                                                           .setKerl(member.kerl().get())
//                                                           .build())
//                                      .get(1, TimeUnit.SECONDS);
//        assertNotNull(validation);
//        assertNotEquals(Validations.getDefaultInstance(), validation);
//        assertEquals(1, validation.getValidationsCount());
//        assertEquals(client.getIdentifier().getCoordinates().toEventCoords(), validation.getCoordinates());
//
//        // Verify client KERL published
//        verify(observer).publish(client.kerl().get(), Collections.singletonList(validation));
    }
}
