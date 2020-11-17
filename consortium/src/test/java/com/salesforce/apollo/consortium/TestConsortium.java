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
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.consortium.proto.ByteTransaction;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
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
    private static final Duration                          gossipDuration  = Duration.ofMillis(10);
    private static final FirefliesParameters               parameters      = new FirefliesParameters(
            ca.getX509Certificate());
    private final static int                               testCardinality = 25;

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
        Messenger.Parameters msgParameters = Messenger.Parameters.newBuilder()
                                                                 .setBufferSize(100)
                                                                 .setEntropy(new SecureRandom())
                                                                 .build();
        AtomicReference<CountDownLatch> processed = new AtomicReference<>(new CountDownLatch(testCardinality));
        Function<CertifiedBlock, HashKey> consensus = c -> {
            ForkJoinPool.commonPool()
                        .execute(() -> consortium.values()
                                                 .stream()
                                                 .forEach(m -> ForkJoinPool.commonPool().execute(() -> {
                                                     if (m.process(c)) {
                                                         processed.get().countDown();
                                                     }
                                                 })));
            return HashKey.ORIGIN;
        };
        gatherConsortium(view, consensus, gossipDuration, scheduler, msgParameters);

        Set<Consortium> blueRibbon = new HashSet<>();
        Consortium.viewFor(Consortium.GENESIS_VIEW_ID, view).allMembers().forEach(e -> {
            blueRibbon.add(consortium.get(e));
        });

        communications.values().forEach(r -> r.start());

        System.out.println("starting consortium");
        consortium.values().forEach(e -> e.start());

        System.out.println("awaiting genesis processing");

        assertTrue(processed.get().await(10, TimeUnit.SECONDS),
                   "Did not converge, end state of true clients gone bad: "
                           + consortium.values()
                                       .stream()
                                       .filter(c -> !blueRibbon.contains(c))
                                       .map(c -> c.fsm().getCurrentState())
                                       .filter(b -> b != CollaboratorFsm.CLIENT)
                                       .collect(Collectors.toSet())
                           + " : "
                           + consortium.values()
                                       .stream()
                                       .filter(c -> !blueRibbon.contains(c))
                                       .filter(c -> c.fsm().getCurrentState() != CollaboratorFsm.CLIENT)
                                       .map(c -> c.getMember())
                                       .collect(Collectors.toList()));

        System.out.println("processing complete, validating state");

        validateState(view, blueRibbon);

        processed.set(new CountDownLatch(testCardinality));
        Consortium client = consortium.values().stream().filter(c -> !blueRibbon.contains(c)).findFirst().get();
        AtomicBoolean txnProcessed = new AtomicBoolean();

        System.out.println("Submitting transaction");
        HashKey hash;
        try {
            hash = client.submit(h -> txnProcessed.set(true),
                                 Any.pack(ByteTransaction.newBuilder()
                                                         .setContent(ByteString.copyFromUtf8("Hello world"))
                                                         .build()));
        } catch (TimeoutException e) {
            fail();
            return;
        }

        System.out.println("Submitted transaction: " + hash + ", awaiting processing of next block");
        assertTrue(processed.get().await(5, TimeUnit.SECONDS), "Did not process transaction block");

        System.out.println("block processed, waiting for transaction completion: " + hash);
        assertTrue(Utils.waitForCondition(5_000, () -> txnProcessed.get()), "Transaction not completed");
        System.out.println("transaction completed: " + hash);
        System.out.println();

        int bunchCount = 10;
        System.out.println("Submitting bunch: " + bunchCount);
        CountDownLatch submittedBunch = new CountDownLatch(bunchCount);
        for (int i = 0; i < bunchCount; i++) {
            try {
                HashKey pending = client.submit(h -> {
                    System.out.println("Completing: " + h);
                    submittedBunch.countDown();
                }, Any.pack(ByteTransaction.newBuilder().setContent(ByteString.copyFromUtf8("Hello world")).build()));
                System.out.println("Submitted transaction:  " + pending);
            } catch (TimeoutException e) {
                fail();
                return;
            }
        }

        System.out.println("Awaiting " + bunchCount + " transactions");
        boolean completed = submittedBunch.await(5, TimeUnit.SECONDS);
        assertTrue(completed, "Did not process transaction bunch: " + submittedBunch.getCount());
        System.out.println("Completed additional " + bunchCount + " transactions");
    }

    private void gatherConsortium(Context<Member> view, Function<CertifiedBlock, HashKey> consensus,
                                  Duration gossipDuration, ScheduledExecutorService scheduler,
                                  Messenger.Parameters msgParameters) {
        members.stream()
               .map(m -> new Consortium(
                       Parameters.newBuilder()
                                 .setConsensus(consensus)
                                 .setValidator(txn -> ByteString.copyFromUtf8("Give Me Food Or Give Me Slack Or Kill Me"))
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
    }

    private void validateState(Context<Member> view, Set<Consortium> blueRibbon) {
        long clientsInWrongState = consortium.values()
                                             .stream()
                                             .filter(c -> !blueRibbon.contains(c))
                                             .map(c -> c.fsm().getCurrentState())
                                             .filter(b -> b != CollaboratorFsm.CLIENT)
                                             .count();
        Set<Consortium> failedMembers = consortium.values()
                                                  .stream()
                                                  .filter(c -> !blueRibbon.contains(c))
                                                  .filter(c -> c.fsm().getCurrentState() != CollaboratorFsm.CLIENT)
                                                  .collect(Collectors.toSet());
        assertEquals(0, clientsInWrongState, "True clients gone bad: " + failedMembers);
        assertEquals(view.getRingCount() - 1,
                     blueRibbon.stream()
                               .map(c -> c.fsm().getCurrentState())
                               .filter(b -> b == CollaboratorFsm.FOLLOWER)
                               .count(),
                     "True follower gone bad: "
                             + blueRibbon.stream().map(c -> c.fsm().getCurrentState()).collect(Collectors.toSet()));
        assertEquals(1,
                     blueRibbon.stream()
                               .map(c -> c.fsm().getCurrentState())
                               .filter(b -> b == CollaboratorFsm.LEADER)
                               .count(),
                     "True leader gone bad: "
                             + blueRibbon.stream().map(c -> c.fsm().getCurrentState()).collect(Collectors.toSet()));
    }

}
