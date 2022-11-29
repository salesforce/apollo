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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;

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

    private AtomicBoolean gate;

    @BeforeEach
    public void beforeIt() {
        gate = new AtomicBoolean(false);
    }

//    @Test
    public void smokin() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(getCardinality(),
                                                                              Thread.ofVirtual().factory());
        routers.values().forEach(r -> r.start());
        dhts.values().forEach(dht -> dht.start(scheduler, Duration.ofSeconds(1)));

        identities.entrySet().forEach(e -> {
            try {
                dhts.get(e.getKey()).asKERL().append(e.getValue().getLastEstablishingEvent().get()).get();
            } catch (InterruptedException | ExecutionException e1) {
                fail(e1.toString());
            }
        });

        gate.set(true);
        final var exec = Executors.newVirtualThreadPerTaskExecutor();
        @SuppressWarnings("unused")
        final var gorgons = routers.values().stream().map(r -> {
            var k = dhts.get(r.getFrom()).asKERL();
            return new Gorgoneion(Parameters.newBuilder().setKerl(k).build(), (ControlledIdentifierMember) r.getFrom(),
                                  context, new DirectPublisher(new ProtoKERLAdapter(k)), r,
                                  Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory()), null, exec);
        }).toList();

        final KERL testKerl = dhts.values().stream().findFirst().get().asKERL();
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
        try {
            testKerl.getKeyEvent(client.getEvent().getCoordinates()).get();
        } catch (ExecutionException e) {
            assertEquals(CompletionException.class, e.getCause().getClass());
        }

        // Verify we can't publish without correct validation
        try {
            testKerl.append(client.getEvent()).get();
        } catch (ExecutionException e) {
            assertEquals(CompletionException.class, e.getCause().getClass());
        }

        var gorgoneionClient = new GorgoneionClient(client, attester, Clock.systemUTC(), admin);

        final var apply = gorgoneionClient.apply(Duration.ofSeconds(60));
        var invitation = apply.get(3000, TimeUnit.SECONDS);
        assertNotNull(invitation);
        assertNotEquals(Validations.getDefaultInstance(), invitation);
        assertTrue(invitation.getValidationsCount() >= context.majority());

        // Verify client KERL published
        var ks = testKerl.getKeyEvent(client.getEvent().getCoordinates()).get();
        assertNotNull(ks);
    }

    @Override
    protected BiFunction<KerlDHT, KERL, KERL> wrap() {
        return (t, k) -> gate.get() ? new Maat(context, k, t.asKERL()) : k;
    }
}
