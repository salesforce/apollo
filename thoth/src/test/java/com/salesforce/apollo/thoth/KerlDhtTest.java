/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import static com.salesforce.apollo.crypto.SigningThreshold.unweighted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class KerlDhtTest {
    private static final double                                                PBYZ        = 0.33;
    private static Map<Digest, ControlledIdentifier<SelfAddressingIdentifier>> identities;
    private static final int                                                   CARDINALITY = 100;

    @BeforeAll
    public static void beforeClass() {
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT),
                                            new SecureRandom());
        identities = IntStream.range(0, CARDINALITY)
                              .parallel()
                              .mapToObj(i -> stereotomy.newIdentifier().get())
                              .map(ci -> {
                                  @SuppressWarnings("unchecked")
                                  var casted = (ControlledIdentifier<SelfAddressingIdentifier>) ci;
                                  return casted;
                              })
                              .collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(),
                                                        controlled -> controlled));
    }

    private final Map<Digest, KerlDHT>     dhts    = new HashMap<>();
    private final Map<Digest, LocalRouter> routers = new HashMap<>();
    private int                            majority;

    @BeforeEach
    public void before() {
        String prefix = UUID.randomUUID().toString();
        Executor exec = Executors.newFixedThreadPool(CARDINALITY);
        Context<Member> context = Context.<Member>newBuilder().setpByz(PBYZ).setCardinality(CARDINALITY).build();
        majority = context.majority();
        identities.values().forEach(ident -> instantiate(ident, context, exec, prefix));
    }

    @AfterEach
    public void after() {
        routers.values().forEach(r -> r.close());
        routers.clear();
        dhts.values().forEach(t -> t.stop());
        dhts.clear();
    }

    private void instantiate(ControlledIdentifier<SelfAddressingIdentifier> identifier, Context<Member> context,
                             Executor executor, String prefix) {
        SigningMember member = new ControlledIdentifierMember(identifier);
        context.activate(member);
        final var url = String.format("jdbc:h2:mem:%s-%s;DB_CLOSE_DELAY=-1", prefix, member.getId());
        context.activate(member);
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create(url, "", "");
        LocalRouter router = new LocalRouter(prefix, ServerConnectionCache.newBuilder().setTarget(2), executor, null);
        router.setMember(member);
        routers.put(member.getId(), router);
        dhts.put(member.getId(), new KerlDHT(context, member, connectionPool, DigestAlgorithm.DEFAULT, router, executor,
                                             Duration.ofMillis(300), 0.125, null));
    }

    @Test
    public void smokin() throws Exception {
        System.out.println();
        System.out.println();
        System.out.println(String.format("Cardinality: %s, Prob Byz: %s, Majority: %s", CARDINALITY, PBYZ, majority));
        System.out.println();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(CARDINALITY);
        routers.values().forEach(r -> r.start());
        dhts.values().forEach(dht -> dht.start(scheduler, Duration.ofMillis(10)));
        var specification = IdentifierSpecification.newBuilder();

        var factory = new ProtobufEventFactory();

        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair();
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair();
        var inception = inception(specification, initialKeyPair, factory, nextKeyPair);

        var dht = dhts.values().stream().findFirst().get();

        dht.append(Collections.singletonList(inception.toKeyEvent_())).get();
        var lookup = dht.getKeyEvent(inception.getCoordinates().toEventCoords()).get();
        assertNotNull(lookup);
        assertEquals(inception.toKeyEvent_(), lookup);

        // rotate
        var prevNext = nextKeyPair;
        nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair();
        var digest = inception.hash(DigestAlgorithm.DEFAULT);

        RotationEvent rotation = rotation(prevNext, digest, inception, nextKeyPair, factory);

        dht.append(Collections.singletonList(rotation.toKeyEvent_())).get();
        lookup = dht.getKeyEvent(rotation.getCoordinates().toEventCoords()).get();
        assertNotNull(lookup);
        assertEquals(rotation.toKeyEvent_(), lookup);

    }

    private InceptionEvent inception(Builder<?> specification, KeyPair initialKeyPair, ProtobufEventFactory factory,
                                     KeyPair nextKeyPair) {

        specification.addKey(initialKeyPair.getPublic())
                     .setSigningThreshold(unweighted(1))
                     .setNextKeys(List.of(nextKeyPair.getPublic()))
                     .setWitnesses(Collections.emptyList())
                     .setSigner(new SignerImpl(initialKeyPair.getPrivate()));
        var identifier = Identifier.NONE;
        InceptionEvent event = factory.inception(identifier, specification.build());
        return event;
    }

    private RotationEvent rotation(KeyPair prevNext, final Digest prevDigest, EstablishmentEvent prev,
                                   KeyPair nextKeyPair, ProtobufEventFactory factory) {
        var rotSpec = RotationSpecification.newBuilder();
        rotSpec.setIdentifier(prev.getIdentifier())
               .setCurrentCoords(prev.getCoordinates())
               .setCurrentDigest(prevDigest)
               .setKey(prevNext.getPublic())
               .setSigningThreshold(unweighted(1))
               .setNextKeys(List.of(nextKeyPair.getPublic()))
               .setSigner(new SignerImpl(prevNext.getPrivate()));

        RotationEvent rotation = factory.rotation(rotSpec.build(), false);
        return rotation;
    }
}
