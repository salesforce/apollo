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
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class NodeTest {
    private static final int    CARDINALITY     = 5;
    private static final Digest GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());

    @BeforeEach
    public void before() throws SQLException {
        final var prefix = UUID.randomUUID().toString();
        Path checkpointDirBase = Path.of("target", "ct-chkpoints-" + Utils.bitStreamEntropy().nextLong());
        Utils.clean(checkpointDirBase.toFile());
        var context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin(), 0.2, CARDINALITY, 3);
        var params = params(context);
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(params.getDigestAlgorithm()),
                                            new SecureRandom());

        record memId(SigningMember m, ControlledIdentifier<SelfAddressingIdentifier> id) {}
        var members = IntStream.range(0, CARDINALITY).mapToObj(i -> {
            @SuppressWarnings("unchecked")
            ControlledIdentifier<SelfAddressingIdentifier> id = (ControlledIdentifier<SelfAddressingIdentifier>) stereotomy.newIdentifier()
                                                                                                                           .get();
            // TODO for now
            var cert = id.provision(InetSocketAddress.createUnresolved("localhost", 0), Instant.now(),
                                    Duration.ofHours(1), SignatureAlgorithm.DEFAULT);

            return new memId(new SigningMemberImpl(id.getDigest(), cert.get().getX509Certificate(),
                                                   cert.get().getPrivateKey(), id.getSigner().get(),
                                                   id.getKeys().get(0)),
                             id);
        }).collect(Collectors.toMap(m -> m.m, m -> m.id));

        members.keySet().forEach(m -> context.activate(m));

        var nodes = new ArrayList<Node>();
        members.forEach((member, id) -> {
            AtomicInteger exec = new AtomicInteger();

            params.setMember(member);
            var localRouter = new LocalRouter(prefix, member,
                                              ServerConnectionCache.newBuilder()
                                                                   .setTarget(30)
                                                                   .setMetrics(params.getMetrics()),
                                              Executors.newFixedThreadPool(2, r -> {
                                                  Thread thread = new Thread(r, "Router exec" + member.getId() + "["
                                                  + exec.getAndIncrement() + "]");
                                                  thread.setDaemon(true);
                                                  return thread;
                                              }));
            params.setMember(member);
            params.getProducer().ethereal().setSigner(member);
            params.setCommunications(localRouter);
            nodes.add(new Node(id, params));
        });
    }

    @Test
    public void smoke() {
    }

    private Builder params(Context<Member> context) {
        var scheduler = Executors.newScheduledThreadPool(CARDINALITY * 5);
        var params = Parameters.newBuilder()
                               .setContext(context)
                               .setSynchronizationCycles(1)
                               .setSynchronizeTimeout(Duration.ofSeconds(1))
                               .setExec(Router.createFjPool())
                               .setGenesisViewId(GENESIS_VIEW_ID)
                               .setGossipDuration(Duration.ofMillis(10))
                               .setScheduler(scheduler)
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
