/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.ethereal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
import com.salesforce.apollo.ethereal.memberships.comm.EtherealMetricsImpl;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * 
 * @author hal.hildebrand
 *
 */
public class EtherealTest {

//    @Test
    public void lots() throws Exception {
        for (int i = 0; i < 100; i++) {
            System.out.println("Iteration: " + i);
            context();
            System.out.println();
        }
    }

    @SuppressWarnings("preview")
    @Test
    public void context() throws Exception {
        var executor = Executors.newVirtualThreadPerTaskExecutor();

        final var gossipPeriod = Duration.ofMillis(5);

        var registry = new MetricRegistry();

        short nProc = 4;
        CountDownLatch finished = new CountDownLatch(nProc);

        List<Ethereal> controllers = new ArrayList<>();
        List<DataSource> dataSources = new ArrayList<>();
        List<ChRbcGossip> gossipers = new ArrayList<>();
        List<Router> comms = new ArrayList<>(); 
        var scheduler = Executors.newScheduledThreadPool(nProc, Thread.ofVirtual().factory());

        List<SigningMember> members = IntStream.range(0, nProc)
                                               .mapToObj(i -> (SigningMember) new SigningMemberImpl(Utils.getMember(i)))
                                               .toList();

        Context<Member> context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), members.size(), 0.1, 3);
        var metrics = new EtherealMetricsImpl(context.getId(), "test", registry);
        for (Member m : members) {
            context.activate(m);
        }
        var builder = Config.deterministic()
                            .setFpr(0.000125)
                            .setnProc(nProc)
                            .setVerifiers(members.toArray(new Verifier[members.size()]));

        List<List<PreBlock>> produced = new ArrayList<>();
        for (int i = 0; i < nProc; i++) {
            produced.add(new CopyOnWriteArrayList<>());
        }

        var level = new AtomicInteger();
        final var prefix = UUID.randomUUID().toString();
        int maxSize = 1024 * 1024;
        for (short i = 0; i < nProc; i++) {
            var ds = new SimpleDataSource();
            final short pid = i;
            List<PreBlock> output = produced.get(pid);
            var com = new LocalRouter(prefix, ServerConnectionCache.newBuilder(), executor, metrics.limitsMetrics());
            comms.add(com);
            final var member = members.get(i);
            var controller = new Ethereal(builder.setSigner(members.get(i)).setPid(pid).build(), maxSize, ds,
                                          (pb, last) -> {
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

            var gossiper = new ChRbcGossip(context, member, controller.processor(), com, executor, metrics);
            gossipers.add(gossiper);
            dataSources.add(ds);
            controllers.add(controller);
            for (int d = 0; d < 5000; d++) {
                ds.dataStack.add(ByteMessage.newBuilder()
                                            .setContents(ByteString.copyFromUtf8("pid: " + pid + " data: " + d))
                                            .build()
                                            .toByteString());
            }
            com.setMember(members.get(i));
        }
        try {
            controllers.forEach(e -> e.start());
            comms.forEach(e -> e.start());
            gossipers.forEach(e -> e.start(gossipPeriod, scheduler));
            finished.await(60, TimeUnit.SECONDS);
        } finally {
            controllers.forEach(e -> e.stop());
            gossipers.forEach(e -> e.stop());
            comms.forEach(e -> e.close());
        }
        final var first = produced.stream().filter(l -> l.size() == 87).findFirst();
        assertFalse(first.isEmpty(), "No process produced 87 blocks: " + produced.stream().map(l -> l.size()).toList());
        List<PreBlock> preblocks = first.get();
        List<String> outputOrder = new ArrayList<>();
        for (int i = 0; i < nProc; i++) {
            final List<PreBlock> output = produced.get(i);
            if (output.size() != 87) {
                fail("Did not get all expected blocks on: " + i + " blocks received: " + output.size());
            }
            for (int j = 0; j < preblocks.size(); j++) {
                var a = preblocks.get(j);
                var b = output.get(j);
                if (a.data().size() != b.data().size()) {
                    fail("Mismatch at block: " + j + " process: " + i);
                }
                for (int k = 0; k < a.data().size(); k++) {
                    if (!a.data().get(k).equals(b.data().get(k))) {
                        fail("Mismatch at block: " + j + " unit: " + k + " process: " + i + " expected: "
                        + a.data().get(k) + " received: " + b.data().get(k));
                    }
                    outputOrder.add(new String(ByteMessage.parseFrom(a.data().get(k)).getContents().toByteArray()));
                }
                if (a.randomBytes() != b.randomBytes()) {
                    fail("Mismatch random bytea at block: " + j + " process: " + i);
                }
            }
        }
//        System.out.println();
//
//        ConsoleReporter.forRegistry(registry)
//                       .convertRatesTo(TimeUnit.SECONDS)
//                       .convertDurationsTo(TimeUnit.MILLISECONDS)
//                       .build()
//                       .report();
    }
}
