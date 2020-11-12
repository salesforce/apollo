/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getCa;
import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
import com.salesforce.apollo.consortium.fsm.GenesisFsm;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.membership.CertWithKey;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;
import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 *
 */
public class TestConsortium {

    private static final RootCertificate                   ca              = getCa();
    private static Map<HashKey, CertificateWithPrivateKey> certs;
    private static final FirefliesParameters               parameters      = new FirefliesParameters(
            ca.getX509Certificate());
    private static int                                     testCardinality = 100;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, testCardinality + 1)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Utils.getMemberId(cert.getX509Certificate()), cert -> cert));
    }

    private File                          baseDir;
    private Builder                       builder        = ServerConnectionCache.newBuilder().setTarget(30);
    private Map<HashKey, Router>          communications = new HashMap<>();
    private final Map<Member, Consortium> consortium     = new HashMap<>();
    @SuppressWarnings("unused")
    private SecureRandom                  entropy;
    private List<Node>                    members;

    @AfterEach
    public void after() {
        consortium.values().forEach(e -> e.stop());
        consortium.clear();
        communications.values().forEach(e -> e.close());
        communications.clear();
    }

    @BeforeEach
    public void before() {

        baseDir = new File(System.getProperty("user.dir"), "target/cluster");
        Utils.clean(baseDir);
        baseDir.mkdirs();
        entropy = new SecureRandom();

        assertTrue(certs.size() >= testCardinality);

        members = new ArrayList<>();
        for (CertificateWithPrivateKey cert : certs.values()) {
            if (members.size() < testCardinality) {
                members.add(new Node(new CertWithKey(cert.getX509Certificate(), cert.getPrivateKey()), parameters));
            } else {
                break;
            }
        }

        assertEquals(testCardinality, members.size());

        members.forEach(node -> communications.put(node.getId(), (Router) new LocalRouter(node.getId(), builder)));

    }

    @Test
    public void smoke() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(testCardinality);

        Context<Member> view = new Context<Member>(HashKey.ORIGIN.prefix(1), parameters.rings);
        Duration gossipDuration = Duration.ofMillis(100);
        Messenger.Parameters msgParameters = Messenger.Parameters.newBuilder()
                                                                 .setBufferSize(100)
                                                                 .setEntropy(new SecureRandom())
                                                                 .build();
        CountDownLatch processed = new CountDownLatch(testCardinality);
        AtomicBoolean published = new AtomicBoolean();
        Function<CertifiedBlock, HashKey> consensus = c -> {
            published.set(true);
            ForkJoinPool.commonPool()
                        .execute(() -> consortium.values()
                                                 .parallelStream()
                                                 .peek(m -> ForkJoinPool.commonPool().execute(() -> m.process(c)))
                                                 .forEach(m -> processed.countDown()));
            return HashKey.ORIGIN;
        };
        members.stream()
               .map(m -> new Consortium(Parameters.newBuilder()
                                                  .setConsensus(consensus)
                                                  .setExecutor(t -> Collections.emptyList())
                                                  .setMember(m)
                                                  .setSignature(() -> m.forSigning())
                                                  .setContext(view)
                                                  .setMsgParameters(msgParameters)
                                                  .setCommunications(communications.get(m.getId()))
                                                  .setGossipDuration(gossipDuration)
                                                  .setScheduler(scheduler)
                                                  .build()))
               .peek(c -> view.activate(c.getMember()))
               .forEach(e -> consortium.put(e.getMember(), e));

        Set<Consortium> blueRibbon = new HashSet<>();
        Consortium.viewFor(Consortium.GENESIS_VIEW_ID, view).allMembers().forEach(e -> {
            blueRibbon.add(consortium.get(e));
        });

        communications.values().forEach(r -> r.start());

        System.out.println("starting consortium");
        consortium.values().forEach(e -> e.start());

        System.out.println("generate genesis");
        assertTrue(Utils.waitForCondition(20_000, () -> published.get()), "Did not publish Genesis block");

        System.out.println("genesis published, awaiting processing");

        assertTrue(processed.await(20_000, TimeUnit.SECONDS), "Did not converge, end state of true clients gone bad: "
                + consortium.values()
                            .stream()
                            .filter(c -> !blueRibbon.contains(c))
                            .map(c -> c.getTransitions().fsm().getCurrentState())
                            .filter(b -> b != CollaboratorFsm.CLIENT)
                            .collect(Collectors.toSet())
                + " : "
                + consortium.values()
                            .stream()
                            .filter(c -> !blueRibbon.contains(c))
                            .filter(c -> c.getTransitions().fsm().getCurrentState() != CollaboratorFsm.CLIENT)
                            .map(c -> c.getMember())
                            .collect(Collectors.toList()));

        System.out.println("processing complete, validating state");

        long clientsInWrongState = consortium.values()
                                             .stream()
                                             .filter(c -> !blueRibbon.contains(c))
                                             .map(c -> c.getTransitions().fsm().getCurrentState())
                                             .filter(b -> b != CollaboratorFsm.CLIENT)
                                             .count();
        Set<Consortium> failedMembers = consortium.values()
                                                  .stream()
                                                  .filter(c -> !blueRibbon.contains(c))
                                                  .filter(c -> c.getTransitions()
                                                                .fsm()
                                                                .getCurrentState() != CollaboratorFsm.CLIENT)
                                                  .collect(Collectors.toSet());
        assertEquals(0, clientsInWrongState, "True clients gone bad: " + failedMembers);
        assertEquals(9,
                     blueRibbon.stream()
                               .map(c -> c.getTransitions().fsm().getCurrentState())
                               .filter(b -> b == GenesisFsm.ORDERED)
                               .count(),
                     "True member gone bad");

//        Consortium client = consortium.get(blueRibbon.size() + 1);
//        try {
//            client.submit(h -> {
//            }, "Hello world".getBytes());
//        } catch (TimeoutException e) {
//            fail();
//        }
//
//        boolean submitted = Utils.waitForCondition(10_000, 1_000,
//                                                   () -> blueRibbon.stream()
//                                                                   .map(collaborator -> collaborator.getState()
//                                                                                                    .getPending()
//                                                                                                    .isEmpty())
//                                                                   .filter(b -> b)
//                                                                   .count() == 0);
//
//        assertTrue(submitted,
//                   "Transaction not submitted to consortium, missing: "
//                           + blueRibbon.stream()
//                                       .map(collaborator -> collaborator.getState().getPending().isEmpty())
//                                       .filter(b -> b)
//                                       .count());

    }

}
