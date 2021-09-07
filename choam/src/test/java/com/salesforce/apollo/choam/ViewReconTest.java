/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static com.salesforce.apollo.crypto.QualifiedBase64.bs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.salesfoce.apollo.choam.proto.Assemble;
import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesfoce.apollo.utils.proto.PubKey;
import com.salesforce.apollo.choam.CHOAM.BlockProducer;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.comm.TerminalClient;
import com.salesforce.apollo.choam.comm.TerminalServer;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class ViewReconTest {

    @Test
    public void func() throws Exception {
        Digest viewId = DigestAlgorithm.DEFAULT.getOrigin().prefix(2);
        Digest nextViewId = viewId.prefix(0x666);
        int cardinality = 5;

        List<Member> members = IntStream.range(0, cardinality).mapToObj(i -> Utils.getMember(i))
                                        .map(cpk -> new SigningMemberImpl(cpk)).map(e -> (Member) e).toList();
        Context<Member> base = new Context<>(viewId, 0.33, members.size());
        base.activate(members);
        Context<Member> committee = Committee.viewFor(viewId, base);

        Map<Member, Verifier> validators = committee.activeMembers().stream().collect(Collectors.toMap(m -> m, m -> m));

        Parameters.Builder params = Parameters.newBuilder().setScheduler(Executors.newScheduledThreadPool(cardinality))
                                              .setProducer(ProducerParameters.newBuilder()
                                                                             .setGossipDuration(Duration.ofMillis(100))
                                                                             .build())
                                              .setGossipDuration(Duration.ofMillis(100)).setContext(base);
        List<HashedCertifiedBlock> published = new CopyOnWriteArrayList<>();

        Map<Member, ViewReconfiguration> recons = new HashMap<>();

        HashedCertifiedBlock previous = new HashedCertifiedBlock(DigestAlgorithm.DEFAULT,
                                                                 CertifiedBlock.getDefaultInstance());
        BlockProducer reconfigure = new BlockProducer() {

            @Override
            public Block checkpoint() {
                return null;
            }

            @Override
            public Block genesis(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous) {
                return null;
            }

            @Override
            public Block produce(Long height, Digest prev, Assemble assemble) {
                return null;
            }

            @Override
            public Block produce(Long height, Digest prev, Executions executions) {
                return null;
            }

            @Override
            public void publish(CertifiedBlock cb) {
                published.add(new HashedCertifiedBlock(DigestAlgorithm.DEFAULT, cb));
            }

            @Override
            public Block reconfigure(Map<Member, Join> joining, Digest nextViewId, HashedBlock previous) {
                return CHOAM.reconfigure(nextViewId, joining, previous, committee, previous, params.build(), previous);
            }
        };
        Map<Member, Concierge> servers = members.stream().collect(Collectors.toMap(m -> m, m -> mock(Concierge.class)));

        servers.forEach((m, s) -> {
            final var mbr = m;
            final SigningMember sm = (SigningMember) mbr;
            when(s.join(any(JoinRequest.class), any(Digest.class))).then(new Answer<ViewMember>() {
                @Override
                public ViewMember answer(InvocationOnMock invocation) throws Throwable {
                    final PubKey consensus = bs(mbr.getPublicKey());
                    return ViewMember.newBuilder().setId(mbr.getId().toDigeste()).setConsensusKey(consensus)
                                     .setSignature(sm.sign(consensus.toByteString()).toSig()).build();

                }
            });
        });

        Map<Member, Router> communications = members.stream()
                                                    .collect(Collectors.toMap(m -> m,
                                                                              m -> new LocalRouter(m,
                                                                                                   ServerConnectionCache.newBuilder(),
                                                                                                   ForkJoinPool.commonPool())));
        var comms = members.stream()
                           .collect(Collectors.toMap(m -> m,
                                                     m -> communications.get(m)
                                                                        .create(m, base.getId(), servers.get(m),
                                                                                r -> new TerminalServer(communications.get(m)
                                                                                                                      .getClientIdentityProvider(),
                                                                                                        null, r, 3_000),
                                                                                TerminalClient.getCreate(null),
                                                                                Terminal.getLocalLoopback((SigningMember) m,
                                                                                                          servers.get(m)))));
        committee.activeMembers().forEach(m -> {
            SigningMember sm = (SigningMember) m;
            Router router = communications.get(m);
            ViewContext view = new ViewContext(committee, params.setMember(sm).setCommunications(router).build(), sm,
                                               validators, reconfigure);
            recons.put(m, new ViewReconfiguration(nextViewId, view, previous, comms.get(m), reconfigure, false));
        });

        try {
            communications.values().forEach(r -> r.start());
            recons.values().forEach(r -> r.start());

            Utils.waitForCondition(20_000, () -> published.size() == committee.activeMembers().size());
            assertEquals(published.size(), committee.activeMembers().size());
        } finally {
            communications.values().forEach(r -> r.close());
        }
    }
}
