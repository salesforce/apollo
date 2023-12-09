/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.*;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.Seal.DigestSeal;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Utils;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 */
public class DhtRebalanceTest {
    public static final int                                                                             CARDINALITY = 23;
    private final       TreeMap<SigningMember, Router>                                                  routers     = new TreeMap<>();
    private final       TreeMap<SigningMember, KerlDHT>                                                 dhts        = new TreeMap<>();
    private final       TreeMap<SigningMember, Context<Member>>                                         contexts    = new TreeMap<>();
    private             String                                                                          prefix;
    private             SecureRandom                                                                    entropy;
    private             StereotomyImpl                                                                  stereotomy;
    private             MemKERL                                                                         kerl;
    private             Map<ControlledIdentifierMember, ControlledIdentifier<SelfAddressingIdentifier>> identities;

    @AfterEach
    public void afterIt() throws Exception {
        routers.values().forEach(r -> r.close(Duration.ofSeconds(1)));
        routers.clear();
        dhts.clear();
        contexts.clear();
        if (identities != null) {
            identities.clear();
        }
    }

    @BeforeEach
    public void beforeIt() throws Exception {
        entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        prefix = UUID.randomUUID().toString();
        kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        stereotomy = new StereotomyImpl(new MemKeyStore(), kerl, entropy);
        identities = IntStream.range(0, CARDINALITY)
                              .mapToObj(i -> stereotomy.newIdentifier())
                              .collect(Collectors.toMap(controlled -> new ControlledIdentifierMember(controlled),
                                                        controlled -> controlled));
        identities.keySet().forEach(member -> instantiate(member));
    }

    //    @Test
    public void lifecycle() throws Exception {
        routers.values().forEach(r -> r.start());
        var members = new TreeSet<SigningMember>();
        var order = dhts.navigableKeySet().stream().toList();
        System.out.println("Order: " + order);
        members.add(order.getFirst());
        KERL fristKerl = dhts.get(order.getFirst()).asKERL();
        dhts.get(order.getFirst()).start(Duration.ofMillis(10));

        Stereotomy controller = new StereotomyImpl(new MemKeyStore(), fristKerl, entropy);

        var identifier = controller.newIdentifier();
        List<KERL.EventWithAttachments> identifierKerl = fristKerl.kerl(identifier.getIdentifier());
        assertEquals(1, identifierKerl.size());
        assertEquals(KeyEvent.INCEPTION_TYPE, identifierKerl.get(0).event().getIlk());

        var remaining = order.subList(1, order.size());
        members.add(remaining.getFirst());
        var test = dhts.get(remaining.getFirst());
        test.start(Duration.ofMillis(10));
        var testKerl = test.asKERL();
        members.forEach(m -> {
            contexts.values().forEach(c -> c.activate(m));
        });

        assertTrue(Utils.waitForCondition(20_000, 1000, () -> testKerl.kerl(identifier.getIdentifier()).size() == 1));
        var mKerl = testKerl.kerl(identifier.getIdentifier());
        assertEquals(1, mKerl.size());
        assertEquals(KeyEvent.INCEPTION_TYPE, mKerl.get(0).event().getIlk());

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(testKerl.getKeyEvent(identifier.getLastEstablishmentEvent()));
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest), Seal.construct(event));

        identifier.rotate();
        identifier.seal(InteractionSpecification.newBuilder());
        identifier.rotate(RotationSpecification.newBuilder().addAllSeals(seals));
        identifier.seal(InteractionSpecification.newBuilder().addAllSeals(seals));
        identifier.rotate();
        identifier.rotate();

        identifierKerl = testKerl.kerl(identifier.getIdentifier());
        assertEquals(7, identifierKerl.size());
        assertEquals(KeyEvent.INCEPTION_TYPE, identifierKerl.get(0).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, identifierKerl.get(1).event().getIlk());
        assertEquals(KeyEvent.INTERACTION_TYPE, identifierKerl.get(2).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, identifierKerl.get(3).event().getIlk());
        assertEquals(KeyEvent.INTERACTION_TYPE, identifierKerl.get(4).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, identifierKerl.get(5).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, identifierKerl.get(6).event().getIlk());
    }

    protected void instantiate(SigningMember member) {
        var context = Context.<Member>newBuilder().build();
        contexts.put(member, context);
        context.activate(member);
        final var url = String.format("jdbc:h2:mem:%s-%s;DB_CLOSE_ON_EXIT=FALSE", member.getId(), prefix);
        context.activate(member);
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create(url, "", "");
        connectionPool.setMaxConnections(10);
        var exec = Executors.newVirtualThreadPerTaskExecutor();
        var router = new LocalServer(prefix, member).router(ServerConnectionCache.newBuilder().setTarget(2));
        routers.put(member, router);
        dhts.put(member, new KerlDHT(Duration.ofMillis(3), context, member, (t, k) -> k, connectionPool,
                                     DigestAlgorithm.DEFAULT, router, Duration.ofSeconds(1), 0.0125, null));
    }
}
