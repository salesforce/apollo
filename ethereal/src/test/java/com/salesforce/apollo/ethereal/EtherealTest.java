/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.RouterMetrics;
import com.salesforce.apollo.comm.RouterMetricsImpl;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.PreUnit.preUnit;
import com.salesforce.apollo.ethereal.creator.CreatorTest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Parameters;
import com.salesforce.apollo.utils.ChannelConsumer;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class EtherealTest {

    static PreUnit newPreUnit(long id, Crown crown, ByteString data, byte[] rsData, DigestAlgorithm algo) {
        var t = PreUnit.decode(id);
        if (t.height() != crown.heights()[t.creator()] + 1) {
            throw new IllegalStateException("Inconsistent height information in preUnit id and crown");
        }
        JohnHancock signature = PreUnit.sign(CreatorTest.DEFAULT_SIGNER, id, crown, data, rsData);
        return new preUnit(t.creator(), t.epoch(), t.height(), signature.toDigest(algo), crown, data, rsData,
                           signature);
    }

    private static class SimpleDataSource implements DataSource {
        final Deque<ByteString> dataStack = new ArrayDeque<>();

        @Override
        public ByteString getData() {
            return dataStack.pollFirst();
        }

    }

    @Test
    public void fourWay() throws Exception {

        short nProc = 4;
        CountDownLatch finished = new CountDownLatch(nProc);
        ChannelConsumer<PreUnit> synchronizer = new ChannelConsumer<>(new LinkedBlockingDeque<>());

        List<Ethereal> ethereals = new ArrayList<>();
        List<DataSource> dataSources = new ArrayList<>();
        List<Controller> controllers = new ArrayList<>();
        var builder = Config.deterministic().setExecutor(ForkJoinPool.commonPool()).setnProc(nProc);

        List<List<PreBlock>> produced = new ArrayList<>();
        for (int i = 0; i < nProc; i++) {
            produced.add(new CopyOnWriteArrayList<>());
        }

        for (short i = 0; i < nProc; i++) {
            var e = new Ethereal();
            var ds = new SimpleDataSource();
            final short pid = i;
            List<PreBlock> output = produced.get(pid);
            var controller = e.deterministic(builder.setSigner(new SignerImpl(0,
                                                                              SignatureAlgorithm.DEFAULT.generateKeyPair()
                                                                                                        .getPrivate()))
                                                    .setPid(pid).build(),
                                             ds, (pb, last) -> {
                                                 output.add(pb);
                                                 if (last) {
                                                     finished.countDown();
                                                 }
                                             }, pu -> synchronizer.getChannel().offer(pu));
            ethereals.add(e);
            dataSources.add(ds);
            controllers.add(controller);
            for (int d = 0; d < 500; d++) {
                ds.dataStack.add(ByteMessage.newBuilder()
                                            .setContents(ByteString.copyFromUtf8("pid: " + pid + " data: " + d)).build()
                                            .toByteString());
            }
        }
        List<Short> ordering = IntStream.range(0, nProc).mapToObj(i -> (short) i).collect(Collectors.toList());
        synchronizer.consume(msgs -> {
            Collections.shuffle(ordering);
            var pus = PreUnit.topologicalSort(msgs);
            ordering.stream().forEach(pid -> {
                var controller = controllers.get(pid);
                controller.input().accept(pus.stream().filter(u -> u.creator() != pid).toList());
            });
        });
        try {
            controllers.forEach(e -> e.start());
            finished.await(10, TimeUnit.SECONDS);
        } finally {
            controllers.forEach(e -> e.stop());
        }
        List<PreBlock> preblocks = produced.get(0);
        List<String> outputOrder = new ArrayList<>();

        for (int i = 0; i < nProc; i++) {
            final List<PreBlock> output = produced.get(i);
            assertEquals(87, output.size(), "Did not get all expected blocks on: " + i);
            for (int j = 0; j < preblocks.size(); j++) {
                if (output.size() <= j) {
                    System.out.println(String.format("Agreement with: %s up to: %s", i, j));
                    break;
                }
                var a = preblocks.get(j);
                var b = output.get(j);
                assertEquals(a.data().size(), b.data().size(), "Mismatch at block: " + j + " process: " + i);
                for (int k = 0; k < a.data().size(); k++) {
                    assertEquals(a.data().get(k), b.data().get(k),
                                 "Mismatch at block: " + j + " unit: " + k + " process: " + i);
                    outputOrder.add(new String(ByteMessage.parseFrom(a.data().get(k)).getContents().toByteArray()));
                }
                assertEquals(a.randomBytes(), b.randomBytes());
            }
        }
    }

    @Test
    public void rbc() throws Exception {
        MetricRegistry registry = new MetricRegistry();
        RouterMetrics metrics = new RouterMetricsImpl(registry);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
        short nProc = 61;
        CountDownLatch finished = new CountDownLatch(nProc);
        SigningMember[] members = new SigningMember[nProc];
        Map<Digest, Short> ordering = new HashMap<>();
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 0.33, nProc);
        Map<SigningMember, ReliableBroadcaster> casting = new HashMap<>();
        List<LocalRouter> comms = new ArrayList<>();
        Parameters.Builder params = Parameters.newBuilder().setExecutor(Executors.newCachedThreadPool())
                                              .setMetrics(metrics).setBufferSize(500).setContext(context);
        for (short i = 0; i < nProc; i++) {
            SigningMember member = new SigningMemberImpl(Utils.getMember(i));
            context.activate(member);
            members[i] = member;
            ordering.put(member.getId(), i);
        }

        for (int i = 0; i < nProc; i++) {
            var member = members[i];
            LocalRouter router = new LocalRouter(member, ServerConnectionCache.newBuilder().setMetrics(metrics),
                                                 ForkJoinPool.commonPool());
            comms.add(router);
            casting.put(member, new ReliableBroadcaster(params.setMember(member).build(), router));
            router.start();
        }

        List<Ethereal> ethereals = new ArrayList<>();
        List<DataSource> dataSources = new ArrayList<>();
        List<Controller> controllers = new ArrayList<>();
        var builder = Config.deterministic().setExecutor(Executors.newCachedThreadPool()).setnProc(nProc);

        List<List<PreBlock>> produced = new ArrayList<>();
        for (int i = 0; i < nProc; i++) {
            produced.add(new ArrayList<>());
        }

        for (short i = 0; i < nProc; i++) {
            var e = new Ethereal();
            var ds = new SimpleDataSource();
            final short pid = i;
            List<PreBlock> output = produced.get(pid);
            ReliableBroadcaster caster = casting.get(members[pid]);
            AtomicInteger round = new AtomicInteger();
            var controller = e.deterministic(builder.setSigner(members[pid]).setPid(pid).build(), ds, (pb, last) -> {
                if (pid == 0) {
                    System.out.println("Preblock: " + round.getAndIncrement());
                }
                output.add(pb);
                if (last) {
                    finished.countDown();
                }
            }, pu -> caster.publish(pu.toPreUnit_s()));
            ethereals.add(e);
            dataSources.add(ds);
            controllers.add(controller);
            for (int d = 0; d < 2500; d++) {
                ds.dataStack.add(ByteMessage.newBuilder()
                                            .setContents(ByteString.copyFromUtf8("pid: " + pid + " data: " + d)).build()
                                            .toByteString());
            }
        }
        try {
            for (short i = 0; i < nProc; i++) {
                final short pid = i;
                var controller = controllers.get(pid);
                var caster = casting.get(members[pid]);
                caster.registerHandler((ctx, msgs) -> msgs.forEach(m -> {
                    var pus = PreUnit.topologicalSort(msgs.stream().map(msg -> {
                        try {
                            return PreUnit.from(PreUnit_s.parseFrom(msg.content()), DigestAlgorithm.DEFAULT);
                        } catch (Throwable t) {
                            t.printStackTrace();
                            return null;
                        }
                    }).collect(Collectors.toList()));
                    controller.input().accept(pus);
//                    System.out.println("Input: "+ pu + " on: " + caster.getMember());
                }));
                caster.start(Duration.ofMillis(100), scheduler);
            }
            Thread.sleep(2);
            controllers.forEach(e -> e.start());

            finished.await(300, TimeUnit.SECONDS);
        } finally {
            casting.values().forEach(e -> e.stop());
            controllers.forEach(e -> e.stop());
        }
        List<PreBlock> preblocks = produced.get(0);
        List<String> outputOrder = new ArrayList<>();

        for (int i = 1; i < nProc; i++) {
            final List<PreBlock> output = produced.get(i);
            assertEquals(87, output.size(), "Did not get all expected blocks on: " + i);
            for (int j = 0; j < preblocks.size(); j++) {
                if (output.size() <= j) {
                    System.out.println(String.format("Agreement with: %s up to: %s", i, j));
                    break;
                }
                var a = preblocks.get(j);
                var b = output.get(j);
                assertEquals(a.data().size(), b.data().size(), "Mismatch at block: " + j + " process: " + i);
                for (int k = 0; k < a.data().size(); k++) {
                    assertEquals(a.data().get(k), b.data().get(k),
                                 "Mismatch at block: " + j + " unit: " + k + " process: " + i);
                    outputOrder.add(new String(ByteMessage.parseFrom(a.data().get(k)).getContents().toByteArray()));
                }
                assertEquals(a.randomBytes(), b.randomBytes());
            }
        }
        System.out.println();

        ConsoleReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build().report();
    }
}
