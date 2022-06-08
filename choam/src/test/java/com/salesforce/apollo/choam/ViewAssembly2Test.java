/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.crypto.QualifiedBase64.bs;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.Reassemble2;
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
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.Ethereal;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
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
public class ViewAssembly2Test {

    private static class VDataSource implements DataSource {

        private BlockingQueue<Reassemble2> outbound = new ArrayBlockingQueue<>(100);

        @Override
        public ByteString getData() {
            Reassemble2.Builder result;
            try {
                result = Reassemble2.newBuilder(outbound.poll(100, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return ByteString.EMPTY;
            }
            while (outbound.peek() != null) {
                var current = outbound.peek();
                result.addAllMembers(current.getMembersList()).addAllValidations(current.getValidationsList());
            }
            return result.build().toByteString();
        }

        public void publish(Reassemble2 r) {
        }

    }

    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LoggerFactory.getLogger(ViewAssembly2Test.class).error("Error on thread: {}", t.getName(), e);
        });
    }

    private Map<Member, Router>      communications = new HashMap<>();
    private Map<Member, Ethereal>    ethereals      = new HashMap<>();
    private Map<Member, ChRbcGossip> gossips        = new HashMap<>();

//    @Test
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

        Map<Member, ViewAssembly2> assemblies = new HashMap<>();
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
        communications = members.stream().collect(Collectors.toMap(m -> m, m -> {
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
        Map<Member, VDataSource> dataSources = members.stream()
                                                      .collect(Collectors.toMap(m -> m, m -> new VDataSource()));
        Map<Member, ViewContext> views = new HashMap<>();
        committee.active().forEach(m -> {
            SigningMember sm = (SigningMember) m;
            Router router = communications.get(m);
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
            views.put(m, view);
            assemblies.put(m, new ViewAssembly2(nextViewId, view, r -> dataSources.get(m).publish(r), comms.get(m)) {
                @Override
                public void complete() {
                    super.complete();
                    complete.countDown();
                }
            });
        });

        initEthereals(params, committee, assemblies, dataSources, views);
        try {
            communications.values().forEach(r -> r.start());
            assemblies.values().forEach(r -> r.assembled());
            gossips.values().forEach(e -> e.start(Duration.ofMillis(10), Executors.newSingleThreadScheduledExecutor()));
            ethereals.values().forEach(e -> e.start());

            assertTrue(complete.await(15, TimeUnit.SECONDS), "Failed to reconfigure");
        } finally {
            communications.values().forEach(r -> r.close());
        }
    }

    private void initEthereals(Parameters.Builder params, Context<Member> context,
                               Map<Member, ViewAssembly2> assemblies, Map<Member, VDataSource> dataSources,
                               Map<Member, ViewContext> views) {
        context.activeMembers().stream().forEach(m -> {
            Config.Builder config = params.getProducer().ethereal().clone();

            config.setPid(views.get(m).roster().get(m.getId())).setnProc((short) context.activeCount());
            config.setEpochLength(7).setNumberOfEpochs(3);
            config.setLabel("View reconfig sim" + context.getId() + " on: " + m.getId());
            var assembly = assemblies.get(m);
            var controller = new Ethereal(config.build(), params.getProducer().maxBatchByteSize(), dataSources.get(m),
                                          (preblock, last) -> assembly.inbound().accept(process(preblock, last)),
                                          epoch -> {
                                          });
            ethereals.put(m, controller);
            gossips.put(m, new ChRbcGossip(context, (SigningMember) m, controller.processor(), communications.get(m),
                                           Executors.newFixedThreadPool(2), null));
        });
    }

    private List<Reassemble2> process(PreBlock preblock, Boolean last) {
        return preblock.data().stream().map(bs -> {
            try {
                return Reassemble2.parseFrom(bs);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
        }).toList();
    }
}
