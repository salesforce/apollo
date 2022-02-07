/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.MemberImpl;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class FireFliesTest {
    private static final int    CARDINALITY     = 5;
    private static final Digest GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());

    private final List<Node>             nodes   = new ArrayList<>();
    private final Map<Node, LocalRouter> routers = new HashMap<>();
    private final Map<Node, View>        views   = new HashMap<>();

    @AfterEach
    public void after() {
        nodes.forEach(n -> n.stop());
        nodes.clear();
        routers.values().forEach(r -> r.close());
        routers.clear();
        views.values().forEach(v -> v.getService().stop());
        views.clear();
    }

    @BeforeEach
    public void before() throws SQLException {
        final var prefix = UUID.randomUUID().toString();
        Path checkpointDirBase = Path.of("target", "ct-chkpoints-" + Utils.bitStreamEntropy().nextLong());
        Utils.clean(checkpointDirBase.toFile());
        var context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin(), 0.2, CARDINALITY, 3);
        var params = params();
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(params.getDigestAlgorithm()),
                                            new SecureRandom());

        var members = new HashMap<SigningMember, ControlledIdentifier<SelfAddressingIdentifier>>();
        for (int i = 0; i < CARDINALITY; i++) {
            @SuppressWarnings("unchecked")
            ControlledIdentifier<SelfAddressingIdentifier> id = (ControlledIdentifier<SelfAddressingIdentifier>) stereotomy.newIdentifier()
                                                                                                                           .get();
            var cert = id.provision(null, InetSocketAddress.createUnresolved("localhost", 0), Instant.now(),
                                    Duration.ofHours(1), SignatureAlgorithm.DEFAULT);
            var member = new SigningMemberImpl(id.getIdentifier().getDigest(), cert.get().getX509Certificate(),
                                               cert.get().getPrivateKey(), id.getSigner().get(), id.getKeys().get(0));
            members.put(member, id);

        }

        members.keySet().forEach(m -> context.activate(m));
        var scheduler = Executors.newScheduledThreadPool(CARDINALITY * 5);

        members.forEach((member, id) -> {
            AtomicInteger execC = new AtomicInteger();

            var localRouter = new LocalRouter(prefix, member, ServerConnectionCache.newBuilder().setTarget(30),
                                              Executors.newFixedThreadPool(2, r -> {
                                                  Thread thread = new Thread(r, "Router exec" + member.getId() + "["
                                                  + execC.getAndIncrement() + "]");
                                                  thread.setDaemon(true);
                                                  return thread;
                                              }));
            params.getProducer().ethereal().setSigner(member);
            var exec = Router.createFjPool();
            var node = new Node(id, params,
                                RuntimeParameters.newBuilder()
                                                 .setScheduler(scheduler)
                                                 .setMember(member)
                                                 .setContext(context)
                                                 .setExec(exec)
                                                 .setCommunications(localRouter));
            nodes.add(node);
            routers.put(node, localRouter);
            localRouter.start();
        });

        var ffParams = FirefliesParameters.newBuilder()
                                          .setCertificateValidator(CertificateValidator.NONE)
                                          .setCardinality(CARDINALITY)
                                          .build();
        var certToMember = new View.CertToMember() {

            @Override
            public Member from(X509Certificate cert) {
                var decoded = Stereotomy.decode(cert).get();
                return new MemberImpl(((SelfAddressingIdentifier) decoded.identifier()).getDigest(), cert,
                                      decoded.keyEvent().getKeys().get(0));
            }

            @Override
            public Digest idOf(X509Certificate cert) {
                var decoded = Stereotomy.decode(cert).get();
                return ((SelfAddressingIdentifier) decoded.identifier()).getDigest();
            }
        };
        nodes.forEach(m -> {
            var cert = m.provision(null, new InetSocketAddress(Utils.allocatePort()), Duration.ofDays(1),
                                   SignatureAlgorithm.DEFAULT)
                        .get();
            var node = new com.salesforce.apollo.fireflies.Node(m.getMember(), cert, ffParams);
            views.put(m, new View(DigestAlgorithm.DEFAULT.getOrigin(), node, certToMember, routers.get(m), null));
        });
    }

    @Test
    public void smokin() throws Exception {
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        nodes.forEach(n -> n.start());
        views.values()
             .forEach(v -> v.getService()
                            .start(Duration.ofMillis(10),
                                   nodes.stream().map(n -> n.getMember().getCertificate()).toList(), scheduler));
        Thread.sleep(5000);
    }

    private Builder params() {
        var params = Parameters.newBuilder()
                               .setSynchronizationCycles(1)
                               .setSynchronizeTimeout(Duration.ofSeconds(1))
                               .setGenesisViewId(GENESIS_VIEW_ID)
                               .setGossipDuration(Duration.ofMillis(10))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(20))
                                                              .setBatchInterval(Duration.ofMillis(100))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(3000)
                                                              .build())
                               .setTxnPermits(5000)
                               .setCheckpointBlockSize(200);
        params.getClientBackoff()
              .setBase(10)
              .setCap(250)
              .setInfiniteAttempts()
              .setJitter()
              .setExceptionHandler(t -> System.out.println(t.getClass().getSimpleName()));

        params.getProducer().ethereal().setNumberOfEpochs(4);
        return params;
    }
}
