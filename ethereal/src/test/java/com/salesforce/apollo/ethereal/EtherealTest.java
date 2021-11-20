/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.crypto.Verifier.DefaultVerifier;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;
import com.salesforce.apollo.ethereal.creator.CreatorTest;
import com.salesforce.apollo.ethereal.memberships.ContextGossiper;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class EtherealTest {

    private static class SimpleDataSource implements DataSource {
        final Deque<ByteString> dataStack = new ArrayDeque<>();

        @Override
        public ByteString getData() {
            try {
                Thread.sleep(Utils.bitStreamEntropy().nextLong(100));
            } catch (InterruptedException e) {
            }
            return dataStack.pollFirst();
        }

    }

    static PreUnit newPreUnit(long id, Crown crown, ByteString data, byte[] rsData, DigestAlgorithm algo) {
        var t = PreUnit.decode(id);
        if (t.height() != crown.heights()[t.creator()] + 1) {
            throw new IllegalStateException("Inconsistent height information in preUnit id and crown");
        }
        JohnHancock signature = PreUnit.sign(CreatorTest.DEFAULT_SIGNER, id, crown, data, rsData);
        return new preUnit(t.creator(), t.epoch(), t.height(), signature.toDigest(algo), crown, data, rsData,
                           signature);
    }

    @Test
    public void context() throws Exception {

        short nProc = 31;
        CountDownLatch finished = new CountDownLatch(nProc);

        List<Ethereal> ethereals = new ArrayList<>();
        List<DataSource> dataSources = new ArrayList<>();
        List<Controller> controllers = new ArrayList<>();
        List<ContextGossiper> gossipers = new ArrayList<>();
        List<Router> comms = new ArrayList<>();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(nProc * 10);

        List<SigningMember> members = IntStream.range(0, nProc)
                                               .mapToObj(i -> (SigningMember) new SigningMemberImpl(Utils.getMember(i)))
                                               .toList();

        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin(), 0.1, members.size(), 3);
        for (Member m : members) {
            context.activate(m);
        }
        var builder = Config.deterministic().setnProc(nProc)
                            .setVerifiers(members.toArray(new Verifier[members.size()]));
        var executor = Executors.newCachedThreadPool();

        List<List<PreBlock>> produced = new ArrayList<>();
        for (int i = 0; i < nProc; i++) {
            produced.add(new CopyOnWriteArrayList<>());
        }

        var level = new AtomicInteger();
        for (short i = 0; i < nProc; i++) {
            var e = new Ethereal();
            var ds = new SimpleDataSource();
            final short pid = i;
            List<PreBlock> output = produced.get(pid);
            var controller = e.deterministic(builder.setSigner(members.get(i)).setPid(pid).build(), ds, (pb, last) -> {
                if (pid == 0) {
                    System.out.println("block: " + level.incrementAndGet());
                }
                output.add(pb);
                if (last) {
                    finished.countDown();
                }
            }, ep -> {
                if (pid == 0) {
                    System.out.println("new epoch: " + ep);
                }
            });
            ethereals.add(e);
            dataSources.add(ds);
            controllers.add(controller);
            for (int d = 0; d < 5000; d++) {
                ds.dataStack.add(ByteMessage.newBuilder()
                                            .setContents(ByteString.copyFromUtf8("pid: " + pid + " data: " + d)).build()
                                            .toByteString());
            }
            Router com = new LocalRouter(members.get(i), ServerConnectionCache.newBuilder(), executor);
            comms.add(com);
            gossipers.add(new ContextGossiper(controller, context, members.get(i), com, null));
        }
        try {
            controllers.forEach(e -> e.start());
            comms.forEach(e -> e.start());
            gossipers.forEach(e -> e.start(Duration.ofMillis(5), scheduler));
            finished.await(60, TimeUnit.SECONDS);
        } finally {
            comms.forEach(e -> e.close());
            gossipers.forEach(e -> e.stop());
            controllers.forEach(e -> e.stop());
        }
        final var first = produced.stream().filter(l -> l.size() == 87).findFirst();
        assertFalse(first.isEmpty(), "No process produced 87 blocks");
        List<PreBlock> preblocks = first.get();
        List<String> outputOrder = new ArrayList<>();
        int success = 0;
        for (int i = 0; i < nProc; i++) {
            final List<PreBlock> output = produced.get(i);
            if (output.size() != 87) {
                System.out.println("Did not get all expected blocks on: " + i);
                break;
            }
            for (int j = 0; j < preblocks.size(); j++) {
                var a = preblocks.get(j);
                var b = output.get(j);
                if (a.data().size() != b.data().size()) {
                    System.out.println("Mismatch at block: " + j + " process: " + i);
                    break;
                }
                boolean s = true;
                for (int k = 0; k < a.data().size(); k++) {
                    if (!a.data().get(k).equals(b.data().get(k))) {
                        s = false;
                        System.out.println("Mismatch at block: " + j + " unit: " + k + " process: " + i + " expected: "
                        + a.data().get(k) + " received: " + b.data().get(k));
                        break;
                    }
                    outputOrder.add(new String(ByteMessage.parseFrom(a.data().get(k)).getContents().toByteArray()));
                }
                if (a.randomBytes() != b.randomBytes()) {
                    System.out.println("Mismatch random bytea at block: " + j + " process: " + i);
                    break;
                }
                if (s) {
                    success++;
                }
            }
            var minQuorum = Dag.minimalQuorum(nProc, builder.getBias());
            assertTrue(success > minQuorum,
                       "Did not have a majority of processes aggree: " + success + " need: " + minQuorum);
        }
    }

    @Test
    public void fourWay() throws Exception {

        short nProc = 4;
        CountDownLatch finished = new CountDownLatch(nProc);

        List<Ethereal> ethereals = new ArrayList<>();
        List<DataSource> dataSources = new ArrayList<>();
        List<Controller> controllers = new ArrayList<>();
        final var cpks = IntStream.range(0, nProc).mapToObj(i -> Utils.getMember(i)).toList();
        final var verifiers = cpks.stream()
                                  .map(c -> (Verifier) new DefaultVerifier(c.getX509Certificate().getPublicKey()))
                                  .toList();
        var builder = Config.deterministic().setnProc(nProc)
                            .setVerifiers(verifiers.toArray(new Verifier[verifiers.size()]));

        List<List<PreBlock>> produced = new ArrayList<>();
        for (int i = 0; i < nProc; i++) {
            produced.add(new CopyOnWriteArrayList<>());
        }

        for (short i = 0; i < nProc; i++) {
            var e = new Ethereal();
            var ds = new SimpleDataSource();
            final short pid = i;
            final AtomicInteger round = new AtomicInteger();
            List<PreBlock> output = produced.get(pid);
            builder.setSigner(new SignerImpl(0, cpks.get(i).getPrivateKey()));
            var controller = e.deterministic(builder.setSigner(new SignerImpl(0,
                                                                              SignatureAlgorithm.DEFAULT.generateKeyPair()
                                                                                                        .getPrivate()))
                                                    .setPid(pid).build(),
                                             ds, (pb, last) -> {
                                                 if (pid == 0) {
                                                     System.out.println("Output: " + round.incrementAndGet());
                                                 }
                                                 output.add(pb);
                                                 if (last) {
                                                     finished.countDown();
                                                 }
                                             }, ep -> {
                                                 if (pid == 0) {
                                                     System.out.println("new epoch: " + ep);
                                                 }
                                             });
            ethereals.add(e);
            dataSources.add(ds);
            controllers.add(controller);
            for (int d = 0; d < 500; d++) {
                ds.dataStack.add(ByteMessage.newBuilder()
                                            .setContents(ByteString.copyFromUtf8("pid: " + pid + " data: " + d)).build()
                                            .toByteString());
            }
        }
        TestGossiper gossiper = new TestGossiper(controllers.stream().map(c -> c.orderer()).toList());

        try {
            controllers.forEach(e -> e.start());
            gossiper.start(Duration.ofMillis(5));
            finished.await(300, TimeUnit.SECONDS);
        } finally {
            gossiper.close();
            controllers.forEach(e -> e.stop());
        }
        List<PreBlock> preblocks = produced.get(0);
        List<String> outputOrder = new ArrayList<>();

        int success = 0;
        for (int i = 0; i < nProc; i++) {
            final List<PreBlock> output = produced.get(i);
            if (output.size() != 87) {
                System.out.println("Did not get all expected blocks on: " + i);
                break;
            }
            for (int j = 0; j < preblocks.size(); j++) {
                var a = preblocks.get(j);
                var b = output.get(j);
                if (a.data().size() != b.data().size()) {
                    System.out.println("Mismatch at block: " + j + " process: " + i);
                    break;
                }
                boolean s = true;
                for (int k = 0; k < a.data().size(); k++) {
                    if (!a.data().get(k).equals(b.data().get(k))) {
                        s = false;
                        System.out.println("Mismatch at block: " + j + " unit: " + k + " process: " + i + " expected: "
                        + a.data().get(k) + " received: " + b.data().get(k));
                        break;
                    }
                    outputOrder.add(new String(ByteMessage.parseFrom(a.data().get(k)).getContents().toByteArray()));
                }
                if (a.randomBytes() != b.randomBytes()) {
                    System.out.println("Mismatch random bytea at block: " + j + " process: " + i);
                    break;
                }
                if (s) {
                    success++;
                }
            }
            var minQuorum = Dag.minimalQuorum(nProc, builder.getBias());
            assertTrue(success > minQuorum,
                       "Did not have a majority of processes aggree: " + success + " need: " + minQuorum);
        }
    }

    @Test
    public void large() throws Exception {

        short nProc = 31;
        CountDownLatch finished = new CountDownLatch(nProc);

        List<Ethereal> ethereals = new ArrayList<>();
        List<DataSource> dataSources = new ArrayList<>();
        List<Controller> controllers = new ArrayList<>();
        final var cpks = IntStream.range(0, nProc).mapToObj(i -> Utils.getMember(i)).toList();
        final var verifiers = cpks.stream()
                                  .map(c -> (Verifier) new DefaultVerifier(c.getX509Certificate().getPublicKey()))
                                  .toList();
        var builder = Config.deterministic().setnProc(nProc)
                            .setVerifiers(verifiers.toArray(new Verifier[verifiers.size()]));

        List<List<PreBlock>> produced = new ArrayList<>();
        for (int i = 0; i < nProc; i++) {
            produced.add(new CopyOnWriteArrayList<>());
        }

        var level = new AtomicInteger();
        for (short i = 0; i < nProc; i++) {
            var e = new Ethereal();
            var ds = new SimpleDataSource();
            final short pid = i;
            List<PreBlock> output = produced.get(pid);
            builder.setSigner(new SignerImpl(0, cpks.get(i).getPrivateKey()));
            var controller = e.deterministic(builder.setSigner(new SignerImpl(0, cpks.get(pid).getPrivateKey()))
                                                    .setPid(pid).build(),
                                             ds, (pb, last) -> {
                                                 if (pid == 0) {
                                                     System.out.println("Preblock: " + level.incrementAndGet());
                                                 }
                                                 output.add(pb);
                                                 if (last) {
                                                     finished.countDown();
                                                 }
                                             }, ep -> {
                                                 if (pid == 0) {
                                                     System.out.println("new epoch: " + ep);
                                                 }
                                             });
            ethereals.add(e);
            dataSources.add(ds);
            controllers.add(controller);
            for (int d = 0; d < 500; d++) {
                ds.dataStack.add(ByteMessage.newBuilder()
                                            .setContents(ByteString.copyFromUtf8("pid: " + pid + " data: " + d)).build()
                                            .toByteString());
            }
        }
        TestGossiper gossiper = new TestGossiper(controllers.stream().map(c -> c.orderer()).toList());
        try {
            controllers.forEach(e -> e.start());
            gossiper.start(Duration.ofMillis(5));
            finished.await(1360, TimeUnit.SECONDS);
        } finally {
            gossiper.close();
            controllers.forEach(e -> e.stop());
        }
        final var first = produced.stream().filter(l -> l.size() == 87).findFirst();
        assertFalse(first.isEmpty(), "No process produced 87 blocks");
        List<PreBlock> preblocks = first.get();
        List<String> outputOrder = new ArrayList<>();
        int success = 0;
        for (int i = 0; i < nProc; i++) {
            final List<PreBlock> output = produced.get(i);
            if (output.size() != 87) {
                System.out.println("Did not get all expected blocks on: " + i);
                break;
            }
            for (int j = 0; j < preblocks.size(); j++) {
                var a = preblocks.get(j);
                var b = output.get(j);
                if (a.data().size() != b.data().size()) {
                    System.out.println("Mismatch at block: " + j + " process: " + i);
                    break;
                }
                boolean s = true;
                for (int k = 0; k < a.data().size(); k++) {
                    if (!a.data().get(k).equals(b.data().get(k))) {
                        s = false;
                        System.out.println("Mismatch at block: " + j + " unit: " + k + " process: " + i + " expected: "
                        + a.data().get(k) + " received: " + b.data().get(k));
                        break;
                    }
                    outputOrder.add(new String(ByteMessage.parseFrom(a.data().get(k)).getContents().toByteArray()));
                }
                if (a.randomBytes() != b.randomBytes()) {
                    System.out.println("Mismatch random bytea at block: " + j + " process: " + i);
                    break;
                }
                if (s) {
                    success++;
                }
            }
            var minQuorum = Dag.minimalQuorum(nProc, builder.getBias());
            assertTrue(success > minQuorum,
                       "Did not have a majority of processes aggree: " + success + " need: " + minQuorum);
        }
    }
}
