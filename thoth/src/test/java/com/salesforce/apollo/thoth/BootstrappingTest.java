/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import com.google.protobuf.Any;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.gorgoneion.Gorgoneion;
import com.salesforce.apollo.gorgoneion.Parameters;
import com.salesforce.apollo.gorgoneion.client.GorgoneionClient;
import com.salesforce.apollo.gorgoneion.client.client.comm.Admissions;
import com.salesforce.apollo.gorgoneion.client.client.comm.AdmissionsClient;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsServer;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsService;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * @author hal.hildebrand
 */
public class BootstrappingTest extends AbstractDhtTest {

    private AtomicBoolean gate;

    @BeforeEach
    public void beforeIt() {
        gate = new AtomicBoolean(false);
    }

    @Test
    public void smokin() throws Exception {
        routers.values().forEach(r -> r.start());
        dhts.values()
            .forEach(dht -> dht.start(LARGE_TESTS ? Duration.ofSeconds(100) : Duration.ofMillis(10)));

        identities.entrySet()
                  .forEach(e -> dhts.get(e.getKey()).asKERL().append(e.getValue().getLastEstablishingEvent()));

        gate.set(true);
        final var exec = Executors.newVirtualThreadPerTaskExecutor();
        @SuppressWarnings("unused")
        final var gorgons = routers.values().stream().map(r -> {
            var k = dhts.get(r.getFrom()).asKERL();
            return new Gorgoneion(Parameters.newBuilder().setKerl(k).build(), (ControlledIdentifierMember) r.getFrom(),
                                  context, new DirectPublisher(new ProtoKERLAdapter(k)), r,
                                  Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory()), null, exec);
        }).toList();

        final var dht = (KerlDHT) dhts.values().stream().findFirst().get();
        final KERL testKerl = dht.asKERL();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 7, 7, 7 });
        var clientKerl = new MemKERL(DigestAlgorithm.DEFAULT);
        var clientStereotomy = new StereotomyImpl(new MemKeyStore(), clientKerl, entropy);

        // The registering client
        var client = new ControlledIdentifierMember(clientStereotomy.newIdentifier());

        // Registering client comms
        var clientRouter = new LocalServer(prefix, client, exec).router(ServerConnectionCache.newBuilder().setTarget(2),
                                                                        exec);
        AdmissionsService admissions = mock(AdmissionsService.class);
        var clientComminications = clientRouter.create(client, context.getId(), admissions, ":admissions-client",
                                                       r -> new AdmissionsServer(
                                                       clientRouter.getClientIdentityProvider(), r, null),
                                                       AdmissionsClient.getCreate(null),
                                                       Admissions.getLocalLoopback(client));
        clientRouter.start();

        // Admin client link
        var admin = clientComminications.connect(dhts.keySet().stream().findFirst().get());

        assertNotNull(admin);
        Function<SignedNonce, Any> attester = sn -> {
            return Any.getDefaultInstance();
        };

        // Verify client KERL not published
        testKerl.getKeyEvent(client.getEvent().getCoordinates());

        // Verify we can't publish without correct validation
        KeyState ks = testKerl.append(client.getEvent());
        assertNull(ks);
        dht.clearCache();

        var gorgoneionClient = new GorgoneionClient(client, attester, Clock.systemUTC(), admin);

        final var invitation = gorgoneionClient.apply(Duration.ofSeconds(60));
        assertNotNull(invitation);
        assertNotEquals(Validations.getDefaultInstance(), invitation);
        assertTrue(invitation.getValidationsCount() >= context.majority());

        Thread.sleep(3000);
        // Verify client KERL published
        var keyS = testKerl.getKeyEvent(client.getEvent().getCoordinates());
        assertNotNull(keyS);
        admin.close();
    }

    @Override
    protected BiFunction<KerlDHT, KERL, KERL> wrap() {
        return (t, k) -> gate.get() ? new Maat(context, k, t.asKERL()) : k;
    }
}
