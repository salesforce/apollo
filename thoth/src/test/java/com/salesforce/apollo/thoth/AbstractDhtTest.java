/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static com.salesforce.apollo.crypto.SigningThreshold.unweighted;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

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
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class AbstractDhtTest {
    protected static final ProtobufEventFactory factory = new ProtobufEventFactory();

    protected static final double                                                PBYZ    = 0.2;
    protected final Map<SigningMember, KerlDHT>                                  dhts    = new HashMap<>();
    protected Map<SigningMember, ControlledIdentifier<SelfAddressingIdentifier>> identities;
    protected int                                                                majority;
    protected final Map<SigningMember, LocalRouter>                              routers = new HashMap<>();
    protected Stereotomy                                                         stereotomy;

    public AbstractDhtTest() {
        super();
    }

    @AfterEach
    public void after() {
        routers.values().forEach(r -> r.close());
        routers.clear();
        dhts.values().forEach(t -> t.stop());
        dhts.clear();
    }

    @BeforeEach
    public void before() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        identities = IntStream.range(0, getCardinality()).mapToObj(i -> {
            try {
                return stereotomy.newIdentifier().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        })
                              .collect(Collectors.toMap(controlled -> new ControlledIdentifierMember(controlled),
                                                        controlled -> controlled));
        String prefix = UUID.randomUUID().toString();
        Context<Member> context = Context.<Member>newBuilder()
                                         .setpByz(PBYZ)
                                         .setCardinality(getCardinality())
                                         .setBias(3)
                                         .build();
        majority = context.majority();
        identities.keySet().forEach(member -> instantiate(member, context, prefix));

        System.out.println();
        System.out.println();
        System.out.println(String.format("Cardinality: %s, Prob Byz: %s, Majority: %s", getCardinality(), PBYZ,
                                         majority));
        System.out.println();
    }

    protected int getCardinality() {
        return Boolean.getBoolean("large_tests") ? 100 : 5;
    }

    protected InceptionEvent inception(Builder<?> specification, KeyPair initialKeyPair, ProtobufEventFactory factory,
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

    protected void instantiate(SigningMember member, Context<Member> context, String prefix) {
        context.activate(member);
        final var url = String.format("jdbc:h2:mem:%s-%s;DB_CLOSE_DELAY=-1", member.getId(), prefix);
//        System.out.println("URL: " + url);
        context.activate(member);
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create(url, "", "");
        LocalRouter router = new LocalRouter(prefix, ServerConnectionCache.newBuilder().setTarget(2),
                                             ForkJoinPool.commonPool(), null);
        router.setMember(member);
        routers.put(member, router);
        final var scheduler = Executors.newScheduledThreadPool(2);
        dhts.put(member, new KerlDHT(Duration.ofMillis(5), context, member, connectionPool, DigestAlgorithm.DEFAULT,
                                     router, ForkJoinPool.commonPool(), Duration.ofSeconds(1), scheduler, 0.125, null));
    }

    protected RotationEvent rotation(KeyPair prevNext, final Digest prevDigest, EstablishmentEvent prev,
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
