/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.google.protobuf.Empty;
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
import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.context.StaticContext;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.Signer;
import com.salesforce.apollo.cryptography.proto.PubKey;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import org.joou.ULong;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.salesforce.apollo.cryptography.QualifiedBase64.bs;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author hal.hildebrand
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

        List<Member> members = IntStream.range(0, cardinality)
                                        .mapToObj(i -> stereotomy.newIdentifier())
                                        .map(ControlledIdentifierMember::new)
                                        .map(e -> (Member) e)
                                        .toList();
        var base = new StaticContext<>(viewId, 0.2, members, 3);
        var committee = Committee.viewFor(viewId, base);

        Parameters.Builder params = Parameters.newBuilder()
                                              .setGenesisViewId(DigestAlgorithm.DEFAULT.getLast())
                                              .setGenerateGenesis(true)
                                              .setProducer(ProducerParameters.newBuilder()
                                                                             .setGossipDuration(Duration.ofMillis(100))
                                                                             .build())
                                              .setGossipDuration(Duration.ofMillis(10));

        Map<Member, GenesisAssembly> genii = new HashMap<>();

        Map<Member, Concierge> servers = members.stream().collect(Collectors.toMap(m -> m, m -> mock(Concierge.class)));

        servers.forEach((m, s) -> {
            when(s.join(any(SignedViewMember.class), any(Digest.class))).then((Answer<Empty>) invocation -> {
                KeyPair keyPair = params.getViewSigAlgorithm().generateKeyPair();
                final PubKey consensus = bs(keyPair.getPublic());
                return Empty.getDefaultInstance();
            });
        });

        final var prefix = UUID.randomUUID().toString();
        Map<Member, Router> communications = members.stream()
                                                    .collect(Collectors.toMap(m -> m,
                                                                              m -> new LocalServer(prefix, m).router(
                                                                              ServerConnectionCache.newBuilder())));
        CountDownLatch complete = new CountDownLatch(committee.memberCount());
        var comms = members.stream()
                           .collect(Collectors.toMap(m -> m, m -> communications.get(m)
                                                                                .create(m, base.getId(), servers.get(m),
                                                                                        servers.get(m)
                                                                                               .getClass()
                                                                                               .getCanonicalName(),
                                                                                        r -> new TerminalServer(
                                                                                        communications.get(m)
                                                                                                      .getClientIdentityProvider(),
                                                                                        null, r),
                                                                                        TerminalClient.getCreate(null),
                                                                                        Terminal.getLocalLoopback(
                                                                                        (SigningMember) m,
                                                                                        servers.get(m)))));
        committee.getAllMembers().forEach(m -> {
            SigningMember sm = (SigningMember) m;
            Router router = communications.get(m);
            params.getProducer().ethereal().setSigner(sm);
            var built = params.build(
            RuntimeParameters.newBuilder().setContext(base).setMember(sm).setCommunications(router).build());
            BlockProducer reconfigure = new BlockProducer() {

                @Override
                public Block checkpoint() {
                    return null;
                }

                @Override
                public Block genesis(Map<Digest, Join> joining, Digest nextViewId, HashedBlock previous) {
                    return CHOAM.genesis(viewId, joining, previous, previous, built, previous, Collections.emptyList());
                }

                @Override
                public void onFailure() {
                    // do nothing
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
                public void publish(Digest hash, CertifiedBlock cb, boolean beacon) {
                    complete.countDown();
                }

                @Override
                public Block reconfigure(Map<Digest, Join> joining, Digest nextViewId, HashedBlock previous,
                                         HashedBlock checkpoint) {
                    return null;
                }
            };
            var pending = new CHOAM.PendingViews();
            pending.add(base.getId(), base);
            var view = new GenesisContext(committee, () -> pending, built, sm, reconfigure);

            KeyPair keyPair = params.getViewSigAlgorithm().generateKeyPair();
            final PubKey consensus = bs(keyPair.getPublic());
            var vm = ViewMember.newBuilder()
                               .setId(m.getId().toDigeste())
                               .setView(params.getGenesisViewId().toDigeste())
                               .setConsensusKey(consensus)
                               .setSignature(((Signer) m).sign(consensus.toByteString()).toSig())
                               .build();
            var svm = SignedViewMember.newBuilder()
                                      .setVm(vm)
                                      .setSignature(((SigningMember) m).sign(vm.toByteString()).toSig())
                                      .build();
            genii.put(m, new GenesisAssembly(view, comms.get(m), svm, m.getId().toString()));
        });

        try {
            communications.values().forEach(Router::start);
            genii.values().forEach(GenesisAssembly::start);
            complete.await(15, TimeUnit.SECONDS);
        } finally {
            communications.values().forEach(r -> r.close(Duration.ofSeconds(0)));
            genii.values().forEach(GenesisAssembly::stop);
        }
    }
}
