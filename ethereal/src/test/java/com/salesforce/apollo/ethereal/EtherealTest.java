/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.ethereal;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
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
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;

/**
 * 
 * @author hal.hildebrand
 *
 */
public class EtherealTest {

    @Test
    public void context() throws Exception {

        final var gossipPeriod = Duration.ofMillis(5);

        var registry = new MetricRegistry();

        short nProc = 4;
        CountDownLatch finished = new CountDownLatch(nProc);

        List<Ethereal> controllers = new ArrayList<>();
        List<DataSource> dataSources = new ArrayList<>();
        List<ChRbcGossip> gossipers = new ArrayList<>();
        List<Router> comms = new ArrayList<>();

        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        List<SigningMember> members = IntStream.range(0, nProc)
                                               .mapToObj(i -> stereotomy.newIdentifier().get())
                                               .map(cpk -> new ControlledIdentifierMember(cpk))
                                               .map(e -> (SigningMember) e)
                                               .toList();

        Context<Member> context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), members.size(), 0.1, 3);
        var metrics = new EtherealMetricsImpl(context.getId(), "test", registry);
        for (Member m : members) {
            context.activate(m);
        }
        var builder = Config.newBuilder()
                            .setFpr(0.0125)
                            .setnProc(nProc)
                            .setVerifiers(members.toArray(new Verifier[members.size()]));

        List<List<PreBlock>> produced = new ArrayList<>();
        for (int i = 0; i < nProc; i++) {
            produced.add(new CopyOnWriteArrayList<>());
        }

        List<ExecutorService> executors = new ArrayList<>();
        var level = new AtomicInteger();
        final var prefix = UUID.randomUUID().toString();
        int maxSize = 1024 * 1024;
        for (short i = 0; i < nProc; i++) {
            var ds = new SimpleDataSource();
            final short pid = i;
            List<PreBlock> output = produced.get(pid);
            final var exec = Executors.newFixedThreadPool(2);
            executors.add(exec);
            var com = new LocalRouter(prefix, ServerConnectionCache.newBuilder(), exec, metrics.limitsMetrics());
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

            var e = Executors.newFixedThreadPool(3);
            executors.add(e);
            var gossiper = new ChRbcGossip(context, member, controller.processor(), com, e, metrics);
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
            gossipers.forEach(e -> {
                final var sched = Executors.newSingleThreadScheduledExecutor();
                executors.add(sched);
                e.start(gossipPeriod, sched);
            });
            finished.await(60, TimeUnit.SECONDS);
        } finally {
            controllers.forEach(e -> e.stop());
            gossipers.forEach(e -> e.stop());
            comms.forEach(e -> e.close());
            executors.forEach(executor -> {
                executor.shutdown();
                try {
                    executor.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e1) {
                }
            });
        }
        final var first = produced.stream().filter(l -> l.size() == 87).findFirst();
        assertFalse(first.isEmpty(), "No process produced 87 blocks: " + produced.stream().map(l -> l.size()).toList());
        List<PreBlock> preblocks = first.get();
        List<String> outputOrder = new ArrayList<>();
        boolean failed = false;
        for (int i = 0; i < nProc; i++) {
            final List<PreBlock> output = produced.get(i);
            if (output.size() != 87) {
                failed = true;
                System.out.println("Did not get all expected blocks on: " + i + " blocks received: " + output.size());
            } else {
                for (int j = 0; j < preblocks.size(); j++) {
                    var a = preblocks.get(j);
                    var b = output.get(j);
                    if (a.data().size() != b.data().size()) {
                        failed = true;
                        System.out.println("Mismatch at block: " + j + " process: " + i + " data size: "
                        + a.data().size() + " != " + b.data().size());
                    } else {
                        for (int k = 0; k < a.data().size(); k++) {
                            if (!a.data().get(k).equals(b.data().get(k))) {
                                failed = true;
                                System.out.println("Mismatch at block: " + j + " unit: " + k + " process: " + i
                                + " expected: " + a.data().get(k) + " received: " + b.data().get(k));
                            }
                            outputOrder.add(new String(ByteMessage.parseFrom(a.data().get(k))
                                                                  .getContents()
                                                                  .toByteArray()));
                        }
                    }
                }
            }
        }
        assertFalse(failed, "Failed");
//        System.out.println();
//
//        ConsoleReporter.forRegistry(registry)
//                       .convertRatesTo(TimeUnit.SECONDS)
//                       .convertDurationsTo(TimeUnit.MILLISECONDS)
//                       .build()
//                       .report();
    }

    @Test
    public void lots() throws Exception {
        if (!Boolean.getBoolean("large_tests")) {
            return;
        }
        for (int i = 0; i < 100; i++) {
            System.out.println("Iteration: " + i);
            context();
            System.out.println();
        }
    }
}
