/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.StereotomyKeyStore;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal.CoordinatesSeal;
import com.salesforce.apollo.stereotomy.event.Seal.DigestSeal;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.stereotomy.services.impl.ProtoKERLService;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class TestResolver {
    KERL                     kel;
    final StereotomyKeyStore ks = new MemKeyStore();
    SecureRandom             secureRandom;

    private LocalRouter clientRouter;

    private LocalRouter serverRouter;

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
    public void provider() throws Exception {
        var context = DigestAlgorithm.DEFAULT.getLast().prefix("foo");
        var client = setup(context);
        client.resolve(new SelfAddressingIdentifier(context));

        Stereotomy controller = new StereotomyImpl(ks, kel, secureRandom);

        var i = controller.newIdentifier().get();

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kel.getKeyEvent(i.getLastEstablishmentEvent()).get());
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest),
                            CoordinatesSeal.construct(event));

        i.rotate();
        i.seal(InteractionSpecification.newBuilder());
        i.rotate(RotationSpecification.newBuilder().addAllSeals(seals));
        i.seal(InteractionSpecification.newBuilder().addAllSeals(seals));
        i.rotate();
        i.rotate();

        var opti = client.kerl(i.getIdentifier());
        assertNotNull(opti);
        assertFalse(opti.isEmpty());
        var iKerl = opti.get();
        assertEquals(7, iKerl.size());
        assertEquals(KeyEvent.INCEPTION_TYPE, iKerl.get(0).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(1).event().getIlk());
        assertEquals(KeyEvent.INTERACTION_TYPE, iKerl.get(2).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(3).event().getIlk());
        assertEquals(KeyEvent.INTERACTION_TYPE, iKerl.get(4).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(5).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(6).event().getIlk());

        Optional<KeyState> keyState = client.resolve(i.getIdentifier());
        assertNotNull(keyState);
        assertFalse(keyState.isEmpty());
        assertEquals(kel.getKeyState(i.getIdentifier()).get(), keyState.get());

        keyState = client.resolve(i.getCoordinates());
        assertNotNull(keyState);
        assertFalse(keyState.isEmpty());
        assertEquals(kel.getKeyState(i.getIdentifier()).get(), keyState.get());
    }

    private KERLClient setup(Digest context) {
        var prefix = UUID.randomUUID().toString();

        var serverMember = new SigningMemberImpl(Utils.getMember(0));
        var clientMember = new SigningMemberImpl(Utils.getMember(1));

        var builder = ServerConnectionCache.newBuilder();
        serverRouter = new LocalRouter(prefix, serverMember, builder, ForkJoinPool.commonPool());
        clientRouter = new LocalRouter(prefix, clientMember, builder, ForkJoinPool.commonPool());

        serverRouter.start();
        clientRouter.start();

        ProtoKERLService protoService = new ProtoKERLService(kel);

        serverRouter.create(serverMember, context, protoService, r -> new KERLServer(null, r), null, null);

        var clientComms = clientRouter.create(clientMember, context, protoService, r -> new KERLServer(null, r),
                                              KERLClient.getCreate(context, null), null);

        var client = clientComms.apply(serverMember, clientMember);
        return client;
    }
}
