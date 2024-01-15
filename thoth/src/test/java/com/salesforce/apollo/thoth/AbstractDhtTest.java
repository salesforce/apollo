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
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.Signer.SignerImpl;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.EventFactory;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import org.h2.jdbcx.JdbcConnectionPool;
import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.salesforce.apollo.cryptography.SigningThreshold.unweighted;

/**
 * @author hal.hildebrand
 */
public class AbstractDhtTest {
    protected static final ProtobufEventFactory factory     = new ProtobufEventFactory();
    protected static final boolean              LARGE_TESTS = Boolean.getBoolean("large_tests");
    protected static final double               PBYZ        = 0.25;

    protected final TreeMap<SigningMember, KerlDHT>                                    dhts    = new TreeMap<>();
    protected final Map<SigningMember, Router>                                         routers = new HashMap<>();
    protected final AtomicBoolean                                                      gate    = new AtomicBoolean(
    false);
    protected       Context<Member>                                                    context;
    protected       Map<SigningMember, ControlledIdentifier<SelfAddressingIdentifier>> identities;
    protected       MemKERL                                                            kerl;
    protected       String                                                             prefix;
    protected       Stereotomy                                                         stereotomy;

    public AbstractDhtTest() {
        super();
    }

    public static InceptionEvent inception(Builder<?> specification, KeyPair initialKeyPair, EventFactory factory,
                                           KeyPair nextKeyPair) {

        specification.addKey(initialKeyPair.getPublic())
                     .setSigningThreshold(unweighted(1))
                     .setNextKeys(List.of(nextKeyPair.getPublic()))
                     .setWitnesses(Collections.emptyList())
                     .setSigner(new SignerImpl(initialKeyPair.getPrivate(), ULong.MIN));
        var identifier = Identifier.NONE;
        InceptionEvent event = factory.inception(identifier, specification.build());
        return event;
    }

    public static RotationEvent rotation(KeyPair prevNext, final Digest prevDigest, EstablishmentEvent prev,
                                         KeyPair nextKeyPair, ProtobufEventFactory factory) {
        var rotSpec = RotationSpecification.newBuilder();
        rotSpec.setIdentifier(prev.getIdentifier())
               .setCurrentCoords(prev.getCoordinates())
               .setCurrentDigest(prevDigest)
               .setKey(prevNext.getPublic())
               .setSigningThreshold(unweighted(1))
               .setNextKeys(List.of(nextKeyPair.getPublic()))
               .setSigner(new SignerImpl(prevNext.getPrivate(), prev.getCoordinates().getSequenceNumber().add(1)));

        RotationEvent rotation = factory.rotation(rotSpec.build(), false);
        return rotation;
    }

    @AfterEach
    public void after() {
        routers.values().forEach(r -> r.close(Duration.ofSeconds(2)));
        routers.clear();
        dhts.values().forEach(t -> t.stop());
        dhts.clear();
    }

    @BeforeEach
    public void before() throws Exception {
        prefix = UUID.randomUUID().toString();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        stereotomy = new StereotomyImpl(new MemKeyStore(), kerl, entropy);
        identities = IntStream.range(0, getCardinality())
                              .mapToObj(i -> stereotomy.newIdentifier())
                              .collect(Collectors.toMap(controlled -> new ControlledIdentifierMember(controlled),
                                                        controlled -> controlled));
        context = Context.newBuilder().setpByz(PBYZ).setCardinality(getCardinality()).build();
        ConcurrentSkipListMap<Digest, Member> serverMembers = new ConcurrentSkipListMap<>();
        identities.keySet().forEach(member -> instantiate(member, context, serverMembers));

        System.out.println();
        System.out.println();
        System.out.printf("Cardinality: %s, Prob Byz: %s, Rings: %s Majority: %s%n", getCardinality(), PBYZ,
                          context.getRingCount(), context.majority(true));
        System.out.println();
    }

    protected int getCardinality() {
        return LARGE_TESTS ? 10 : 5;
    }

    protected void instantiate(SigningMember member, Context<Member> context,
                               ConcurrentSkipListMap<Digest, Member> serverMembers) {
        context.activate(member);
        final var url = String.format("jdbc:h2:mem:%s-%s;DB_CLOSE_ON_EXIT=FALSE", member.getId(), prefix);
        context.activate(member);
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create(url, "", "");
        connectionPool.setMaxConnections(10);
        var router = new LocalServer(prefix, member).router(ServerConnectionCache.newBuilder().setTarget(2));
        routers.put(member, router);
        dhts.put(member,
                 new KerlDHT(Duration.ofMillis(5), context, member, wrap(), connectionPool, DigestAlgorithm.DEFAULT,
                             router, Duration.ofSeconds(10), 0.0125, null));
    }

    protected BiFunction<KerlDHT, KERL.AppendKERL, KERL.AppendKERL> wrap() {
        return (t, k) -> k;
    }
}
