/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.crypto.QualifiedBase64.bs;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.joou.ULong;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.choam.proto.Assemble;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.choam.CHOAM.BlockProducer;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.comm.TerminalClient;
import com.salesforce.apollo.choam.comm.TerminalServer;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class GenesisAssemblyTest {
    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LoggerFactory.getLogger(GenesisAssemblyTest.class).error("Error on thread: {}", t.getName(), e);
        });
    }

    @Test
    public void genesis() throws Exception {
        Digest viewId = DigestAlgorithm.DEFAULT.getOrigin().prefix(2);
        int cardinality = 5;
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        List<Member> members = IntStream.range(0, cardinality).mapToObj(i -> {
            try {
                return stereotomy.newIdentifier().get();
            } catch (InterruptedException | ExecutionException e1) {
                throw new IllegalStateException(e1);
            }
        }).map(cpk -> new ControlledIdentifierMember(cpk)).map(e -> (Member) e).toList();
        Context<Member> base = new ContextImpl<>(viewId, members.size(), 0.2, 3);
        base.activate(members);
        Context<Member> committee = Committee.viewFor(viewId, base);

        Parameters.Builder params = Parameters.newBuilder()
                                              .setProducer(ProducerParameters.newBuilder()
                                                                             .setGossipDuration(Duration.ofMillis(100))
                                                                             .build())
                                              .setGossipDuration(Duration.ofMillis(10));

        Map<Member, GenesisAssembly> genii = new HashMap<>();

        Map<Member, Concierge> servers = members.stream().collect(Collectors.toMap(m -> m, m -> mock(Concierge.class)));

        servers.forEach((m, s) -> {
            when(s.join(any(Digest.class), any(Digest.class))).then(new Answer<ViewMember>() {
                @Override
                public ViewMember answer(InvocationOnMock invocation) throws Throwable {
                    KeyPair keyPair = params.getViewSigAlgorithm().generateKeyPair();
                    final PubKey consensus = bs(keyPair.getPublic());
                    return ViewMember.newBuilder()
                                     .setId(m.getId().toDigeste())
                                     .setConsensusKey(consensus)
                                     .setSignature(((Signer) m).sign(consensus.toByteString()).toSig())
                                     .build();

                }
            });
        });

        final var prefix = UUID.randomUUID().toString();
        Map<Member, Router> communications = members.stream().collect(Collectors.toMap(m -> m, m -> {
            var comm = new LocalServer(prefix, m, Executors.newSingleThreadExecutor()).router( ServerConnectionCache.newBuilder(),
                                       Executors.newSingleThreadExecutor());
            return comm;
        }));
        CountDownLatch complete = new CountDownLatch(committee.activeCount());
        var comms = members.stream()
                           .collect(Collectors.toMap(m -> m,
                                                     m -> communications.get(m)
                                                                        .create(m, base.getId(), servers.get(m),
                                                                                servers.get(m)
                                                                                       .getClass()
                                                                                       .getCanonicalName(),
                                                                                r -> new TerminalServer(communications.get(m)
                                                                                                                      .getClientIdentityProvider(),
                                                                                                        null, r),
                                                                                TerminalClient.getCreate(null),
                                                                                Terminal.getLocalLoopback((SigningMember) m,
                                                                                                          servers.get(m)))));
        committee.active().forEach(m -> {
            SigningMember sm = (SigningMember) m;
            Router router = communications.get(m);
            params.getProducer().ethereal().setSigner(sm);
            var built = params.build(RuntimeParameters.newBuilder()
                                                      .setExec(Executors.newFixedThreadPool(2))
                                                      .setScheduler(Executors.newSingleThreadScheduledExecutor())
                                                      .setContext(base)
                                                      .setMember(sm)
                                                      .setCommunications(router)
                                                      .build());
            BlockProducer reconfigure = new BlockProducer() {

                @Override
                public Block checkpoint() {
                    return null;
                }

                @Override
                public Block genesis(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous) {
                    return CHOAM.genesis(viewId, joining, previous, committee, previous, built, previous,
                                         Collections.emptyList());
                }

                @Override
                public Block produce(ULong height, Digest prev, Assemble assemble, HashedBlock checkpoint) {
                    return null;
                }

                @Override
                public Block produce(ULong height, Digest prev, Executions executions, HashedBlock checkpoint) {
                    return null;
                }

                @Override
                public void publish(CertifiedBlock cb) {
                    complete.countDown();
                }

                @Override
                public Block reconfigure(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous,
                                         HashedBlock checkpoint) {
                    return null;
                }
            };
            var view = new GenesisContext(committee, built, sm, reconfigure);

            KeyPair keyPair = params.getViewSigAlgorithm().generateKeyPair();
            final PubKey consensus = bs(keyPair.getPublic());
            var vm = ViewMember.newBuilder()
                               .setId(m.getId().toDigeste())
                               .setConsensusKey(consensus)
                               .setSignature(((Signer) m).sign(consensus.toByteString()).toSig())
                               .build();
            genii.put(m, new GenesisAssembly(view, comms.get(m), vm, Ethereal.consumer(m.getId().toString())));
        });

        try {
            communications.values().forEach(r -> r.start());
            genii.values().forEach(r -> r.start());
            complete.await(15, TimeUnit.SECONDS);
        } finally {
            communications.values().forEach(r -> r.close(Duration.ofSeconds(1)));
            genii.values().forEach(r -> r.stop());
        }
    }
}
