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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.comm.TerminalClient;
import com.salesforce.apollo.choam.comm.TerminalServer;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Verifier;
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
public class ViewAssemblyTest {
    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LoggerFactory.getLogger(ViewAssemblyTest.class).error("Error on thread: {}", t.getName(), e);
        });
    }

    @Test
    public void assembly() throws Exception {
        Digest viewId = DigestAlgorithm.DEFAULT.getOrigin().prefix(2);
        Digest nextViewId = viewId.prefix(0x666);
        int cardinality = 5;
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        List<Member> members = IntStream.range(0, cardinality)
                                        .mapToObj(i -> stereotomy.newIdentifier().get())
                                        .map(cpk -> new ControlledIdentifierMember(cpk))
                                        .map(e -> (Member) e)
                                        .toList();
        Context<Member> base = new ContextImpl<>(viewId, members.size(), 0.2, 3);
        base.activate(members);
        Context<Member> committee = Committee.viewFor(viewId, base);

        Parameters.Builder params = Parameters.newBuilder()
                                              .setProducer(ProducerParameters.newBuilder()
                                                                             .setGossipDuration(Duration.ofMillis(10))
                                                                             .build())
                                              .setGossipDuration(Duration.ofMillis(10));

        Map<Member, ViewAssembly> recons = new HashMap<>();
        Map<Member, Concierge> servers = members.stream().collect(Collectors.toMap(m -> m, m -> mock(Concierge.class)));
        Map<Member, KeyPair> consensusPairs = new HashMap<>();
        servers.forEach((m, s) -> {
            KeyPair keyPair = params.getViewSigAlgorithm().generateKeyPair();
            consensusPairs.put(m, keyPair);
            final PubKey consensus = bs(keyPair.getPublic());
            when(s.join(any(JoinRequest.class), any(Digest.class))).then(new Answer<ViewMember>() {
                @Override
                public ViewMember answer(InvocationOnMock invocation) throws Throwable {
                    return ViewMember.newBuilder()
                                     .setId(m.getId().toDigeste())
                                     .setConsensusKey(consensus)
                                     .setSignature(((Signer) m).sign(consensus.toByteString()).toSig())
                                     .build();

                }
            });
        });
        CountDownLatch complete = new CountDownLatch(committee.size());
        final var prefix = UUID.randomUUID().toString();
        Map<Member, Router> communications = members.stream().collect(Collectors.toMap(m -> m, m -> {
            var localRouter = new LocalRouter(prefix, ServerConnectionCache.newBuilder(),
                                              Executors.newFixedThreadPool(2), null);
            localRouter.setMember(m);
            return localRouter;
        }));
        var comms = members.stream()
                           .collect(Collectors.toMap(m -> m,
                                                     m -> communications.get(m)
                                                                        .create(m, base.getId(), servers.get(m),
                                                                                r -> new TerminalServer(communications.get(m)
                                                                                                                      .getClientIdentityProvider(),
                                                                                                        null, r,
                                                                                                        Executors.newSingleThreadExecutor()),
                                                                                TerminalClient.getCreate(null),
                                                                                Terminal.getLocalLoopback((SigningMember) m,
                                                                                                          servers.get(m)))));

        Map<Member, Verifier> validators = consensusPairs.entrySet()
                                                         .stream()
                                                         .collect(Collectors.toMap(e -> e.getKey(),
                                                                                   e -> new Verifier.DefaultVerifier(e.getValue()
                                                                                                                      .getPublic())));
        committee.active().forEach(m -> {
            SigningMember sm = (SigningMember) m;
            Router router = communications.get(m);
            params.getProducer().ethereal().setSigner(sm);
            ViewContext view = new ViewContext(committee,
                                               params.build(RuntimeParameters.newBuilder()
                                                                             .setExec(Executors.newFixedThreadPool(2))
                                                                             .setScheduler(Executors.newSingleThreadScheduledExecutor())
                                                                             .setContext(base)
                                                                             .setMember(sm)
                                                                             .setCommunications(router)
                                                                             .build()),
                                               new Signer.SignerImpl(consensusPairs.get(m).getPrivate()), validators,
                                               null);
            recons.put(m, new ViewAssembly(nextViewId, view, comms.get(m)) {

                @Override
                public void complete() {
                    super.complete();
                    complete.countDown();
                }

                @Override
                public void failed() {
                    super.failed();
                }
            });
        });

        try {
            communications.values().forEach(r -> r.start());
            recons.values().forEach(r -> r.start());

            recons.values().forEach(r -> r.assembled());

            complete.await(15, TimeUnit.SECONDS);
        } finally {
            recons.values().forEach(r -> r.stop());
            communications.values().forEach(r -> r.close());
        }
    }
}
