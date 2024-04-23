/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Parameters.BootstrapParameters;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.context.DynamicContext;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Utils;
import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 */
public class MembershipTests {
    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LoggerFactory.getLogger(MembershipTests.class).error("Error on thread: {}", t.getName(), e);
        });
        //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Session.class)).setLevel(Level.TRACE);
        //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CHOAM.class)).setLevel(Level.TRACE);
        //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(GenesisAssembly.class)).setLevel(Level.TRACE);
        //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ViewAssembly.class)).setLevel(Level.TRACE);
        //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Producer.class)).setLevel(Level.TRACE);
        //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Committee.class)).setLevel(Level.TRACE);
        //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Fsm.class)).setLevel(Level.TRACE);
    }

    private Map<Digest, CHOAM>     choams;
    private List<Member>           members;
    private Map<Digest, Router>    routers;
    private DynamicContext<Member> context;

    @AfterEach
    public void after() throws Exception {
        shutdown();
        members = null;
        context = null;
    }

    @Test
    public void genesisBootstrap() throws Exception {
        SigningMember testSubject = initialize(2000, 5);
        System.out.println(
        "Test subject: " + testSubject.getId() + " membership: " + members.stream().map(Member::getId).toList());
        routers.entrySet()
               .stream()
               .filter(e -> !e.getKey().equals(testSubject.getId()))
               .forEach(r -> r.getValue().start());
        choams.entrySet()
              .stream()
              .filter(e -> !e.getKey().equals(testSubject.getId()))
              .forEach(ch -> ch.getValue().start());

        final Duration timeout = Duration.ofSeconds(3);
        var txneer = choams.get(members.getLast().getId());

        System.out.println("Transactioneer: " + txneer.getId());

        boolean active = Utils.waitForCondition(12_000, 1_000, () -> choams.entrySet()
                                                                           .stream()
                                                                           .filter(
                                                                           e -> !testSubject.getId().equals(e.getKey()))
                                                                           .map(Map.Entry::getValue)
                                                                           .allMatch(CHOAM::active));
        assertTrue(active,
                   "Group did not become active, test subject: " + testSubject.getId() + " txneer: " + txneer.getId()
                   + " inactive: " + choams.entrySet()
                                           .stream()
                                           .filter(e -> !testSubject.getId().equals(e.getKey()))
                                           .map(Map.Entry::getValue)
                                           .filter(c -> !c.active())
                                           .map(CHOAM::logState)
                                           .toList());

        final var countdown = new CountDownLatch(1);
        var transactioneer = new Transactioneer(txneer.getSession(), timeout, 1, countdown);

        transactioneer.start();
        assertTrue(countdown.await(30, TimeUnit.SECONDS), "Could not submit transaction");

        var target = choams.values()
                           .stream()
                           .map(CHOAM::currentHeight)
                           .filter(Objects::nonNull)
                           .mapToInt(ULong::intValue)
                           .max()
                           .getAsInt();

        routers.get(testSubject.getId()).start();
        choams.get(testSubject.getId()).start();
        context.activate(testSubject);
        final var targetMet = Utils.waitForCondition(120_000, 1_000, () -> {
            final var currentHeight = choams.get(testSubject.getId()).currentHeight();
            return currentHeight != null && currentHeight.intValue() >= target;
        });
        assertTrue(targetMet,
                   "Expecting: " + target + " completed: " + choams.get(testSubject.getId()).currentHeight());

    }

    public SigningMember initialize(int checkpointBlockSize, int cardinality) throws Exception {

        var params = Parameters.newBuilder()
                               .setGenerateGenesis(true)
                               .setBootstrap(
                               BootstrapParameters.newBuilder().setGossipDuration(Duration.ofMillis(20)).build())
                               .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin())
                               .setGossipDuration(Duration.ofMillis(10))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(20))
                                                              .setBatchInterval(Duration.ofMillis(10))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(10_000)
                                                              .setEthereal(Config.newBuilder()
                                                                                 .setEpochLength(7)
                                                                                 .setNumberOfEpochs(3))
                                                              .build())
                               .setGenerateGenesis(true)
                               .setCheckpointBlockDelta(checkpointBlockSize);
        params.getDrainPolicy().setInitialBackoff(Duration.ofMillis(1)).setMaxBackoff(Duration.ofMillis(1));
        params.getProducer().ethereal().setNumberOfEpochs(2).setEpochLength(20);

        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        members = IntStream.range(0, cardinality)
                           .mapToObj(i -> stereotomy.newIdentifier())
                           .map(ControlledIdentifierMember::new)
                           .map(e -> (Member) e)
                           .toList();
        context = DynamicContext.newBuilder().setpByz(0.2).setBias(3).setCardinality(cardinality).build();
        context.activate(members);

        SigningMember testSubject = new ControlledIdentifierMember(stereotomy.newIdentifier());

        final var prefix = UUID.randomUUID().toString();
        routers = members.stream()
                         .collect(Collectors.toMap(Member::getId, m -> new LocalServer(prefix, m).router(
                         ServerConnectionCache.newBuilder().setTarget(cardinality))));
        routers.put(testSubject.getId(), new LocalServer(prefix, testSubject).router(
        ServerConnectionCache.newBuilder().setTarget(cardinality)));
        choams = new HashMap<>();
        for (Member m : members) {
            choams.put(m.getId(), constructCHOAM((SigningMember) m, params, false));
        }
        choams.put(testSubject.getId(), constructCHOAM(testSubject, params, true));
        return testSubject;
    }

    private CHOAM constructCHOAM(SigningMember m, Parameters.Builder params, boolean testSubject) {
        final TransactionExecutor processor = (index, hash, t, f, executor) -> {
            if (f != null) {
                f.completeAsync(Object::new, executor);
            }
        };
        params.getProducer().ethereal().setSigner(m);
        if (testSubject) {
            params.setSynchronizationCycles(1);
        }
        return new CHOAM(params.build(RuntimeParameters.newBuilder()
                                                       .setMember(m)
                                                       .setCommunications(routers.get(m.getId()))
                                                       .setProcessor(processor)
                                                       .setContext(context)
                                                       .build()));
    }

    private void shutdown() {
        if (choams != null) {
            choams.values().forEach(CHOAM::stop);
            choams = null;
        }
        if (routers != null) {
            routers.values().forEach(e -> e.close(Duration.ofSeconds(1)));
            routers = null;
        }
    }
}
