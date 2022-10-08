/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.SecureRandom;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.StereotomyKeyStore;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal.CoordinatesSeal;
import com.salesforce.apollo.stereotomy.event.Seal.DigestSeal;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.DelegatedKERL;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLClient;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLServer;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLService;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;

/**
 * @author hal.hildebrand
 *
 */
public class TestKerlService {
    KERL                     kel;
    final StereotomyKeyStore ks = new MemKeyStore();
    SecureRandom             secureRandom;
    private LocalRouter      clientRouter;
    private LocalRouter      serverRouter;

    @AfterEach
    public void after() {
        if (serverRouter != null) {
            serverRouter.close();
            serverRouter = null;
        }
        if (clientRouter != null) {
            clientRouter.close();
            clientRouter = null;
        }
    }

    @BeforeEach
    public void before() throws Exception {
        secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(new byte[] { 0 });
        kel = new MemKERL(DigestAlgorithm.DEFAULT);
    }

    @Test
    public void kerl() throws Exception {
        var context = DigestAlgorithm.DEFAULT.getLast().prefix("foo");
        var client = setup(context);

        var service = new DelegatedKERL(client, DigestAlgorithm.DEFAULT);
        Stereotomy controller = new StereotomyImpl(ks, service, secureRandom);

        var i = controller.newIdentifier().get();

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kel.getKeyEvent(i.getLastEstablishmentEvent()).get());
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest),
                            CoordinatesSeal.construct(event));

        i.rotate().get();
        i.seal(InteractionSpecification.newBuilder()).get();
        i.rotate(RotationSpecification.newBuilder().addAllSeals(seals)).get();
        i.seal(InteractionSpecification.newBuilder().addAllSeals(seals)).get();
        i.rotate().get();
        i.rotate().get();

        var iKerl = service.kerl(i.getIdentifier()).get();
        assertNotNull(iKerl);
        assertEquals(7, iKerl.size());
        assertEquals(KeyEvent.INCEPTION_TYPE, iKerl.get(0).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(1).event().getIlk());
        assertEquals(KeyEvent.INTERACTION_TYPE, iKerl.get(2).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(3).event().getIlk());
        assertEquals(KeyEvent.INTERACTION_TYPE, iKerl.get(4).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(5).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(6).event().getIlk());

        var keyState = service.getKeyState(i.getIdentifier()).get();
        assertNotNull(keyState);
        assertEquals(kel.getKeyState(i.getIdentifier()).get(), keyState);

        keyState = service.getKeyState(i.getCoordinates()).get();
        assertNotNull(keyState);
        assertEquals(kel.getKeyState(i.getIdentifier()).get(), keyState);
    }

    private KERLService setup(Digest context) throws Exception {
        var prefix = UUID.randomUUID().toString();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        var serverMember = new ControlledIdentifierMember(stereotomy.newIdentifier().get());
        var clientMember = new ControlledIdentifierMember(stereotomy.newIdentifier().get());

        var builder = ServerConnectionCache.newBuilder();
        final var exec = Executors.newFixedThreadPool(3);
        ConcurrentSkipListMap<Digest, Member> serverMembers = new ConcurrentSkipListMap<>();
        serverRouter = new LocalRouter(serverMember, prefix, serverMembers, builder, exec, null);
        clientRouter = new LocalRouter(clientMember, prefix, serverMembers, builder, exec, null);

        serverRouter.start();
        clientRouter.start();

        ProtoKERLService protoService = new ProtoKERLAdapter(kel);

        serverRouter.create(serverMember, context, protoService, protoService.getClass().getCanonicalName(),
                            r -> new KERLServer(r, exec, null), null, null);

        var clientComms = clientRouter.create(clientMember, context, protoService,
                                              protoService.getClass().getCanonicalName(),
                                              r -> new KERLServer(r, exec, null), KERLClient.getCreate(context, null),
                                              null);

        var client = clientComms.apply(serverMember, clientMember);
        return client;
    }
}
