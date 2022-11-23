/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.gorgoneion.Gorgoneion;
import com.salesforce.apollo.gorgoneion.GorgoneionClient;
import com.salesforce.apollo.gorgoneion.Parameters;
import com.salesforce.apollo.gorgoneion.comm.admissions.Admissions;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsClient;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsServer;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsService;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;
import com.salesforce.apollo.thoth.KerlDHT.CompletionException;

/**
 * @author hal.hildebrand
 *
 */
public class BootstrappingTest extends AbstractDhtTest {

    @Test
    public void smokin() throws Exception {

        // The kerl observer to publish admitted client KERLs to
        var observer = new DirectPublisher(new ProtoKERLAdapter(kerl));

        final var parameters = Parameters.newBuilder().setKerl(kerl).build();
        final var exec = Executors.newVirtualThreadPerTaskExecutor();
        @SuppressWarnings("unused")
        final var gorgons = dhts.keySet().stream().map(m -> {
            final var router = new LocalServer(prefix, m, exec).router(ServerConnectionCache.newBuilder().setTarget(2),
                                                                       exec);
            router.start();
            return router;
        })
                                .map(r -> new Gorgoneion(parameters, (ControlledIdentifierMember) r.getFrom(), context,
                                                         observer, r,
                                                         Executors.newScheduledThreadPool(2,
                                                                                          Thread.ofVirtual().factory()),
                                                         null, exec))
                                .toList();

        var entropy = new SecureRandom();
        var clientKerl = new MemKERL(DigestAlgorithm.DEFAULT);
        var clientStereotomy = new StereotomyImpl(new MemKeyStore(), clientKerl, entropy);

        // The registering client
        var client = new ControlledIdentifierMember(clientStereotomy.newIdentifier().get());

        // Registering client comms
        var clientRouter = new LocalServer(prefix, client, exec).router(ServerConnectionCache.newBuilder().setTarget(2),
                                                                        exec);
        AdmissionsService admissions = mock(AdmissionsService.class);
        var clientComminications = clientRouter.create(client, context.getId(), admissions, ":admissions-client",
                                                       r -> new AdmissionsServer(clientRouter.getClientIdentityProvider(),
                                                                                 r, null),
                                                       AdmissionsClient.getCreate(null),
                                                       Admissions.getLocalLoopback(client));
        clientRouter.start();

        // Admin client link
        var admin = clientComminications.connect(dhts.keySet().stream().findFirst().get());

        assertNotNull(admin);
        Function<SignedNonce, CompletableFuture<Any>> attester = sn -> {
            var fs = new CompletableFuture<Any>();
            fs.complete(Any.getDefaultInstance());
            return fs;
        };

        // Verify client KERL not published
        assertNull(kerl.getKeyEvent(client.getEvent().getCoordinates()).get());

        // Verify we can't publish without correct validation
        try {
            dhts.values().stream().findFirst().get().append(client.getEvent().toKeyEvent_()).get();
        } catch (ExecutionException e) {
            assertEquals(CompletionException.class, e.getCause().getClass());
        }

        var gorgoneionClient = new GorgoneionClient(client, attester, parameters.clock(), admin);

        final var apply = gorgoneionClient.apply(Duration.ofSeconds(60));
        var invitation = apply.get(300, TimeUnit.SECONDS);
        assertNotNull(invitation);
        assertNotEquals(Validations.getDefaultInstance(), invitation);
        assertTrue(invitation.getValidationsCount() >= context.majority());

        // Verify client KERL published
        var ks = kerl.getKeyEvent(client.getEvent().getCoordinates()).get();
        assertNotNull(ks);
    }

    @Override
    protected Function<KERL, KERL> wrap() {
        return k -> new Maat(context, k);
    }
}
