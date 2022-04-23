/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.rbc;

/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag.DagImpl;
import com.salesforce.apollo.ethereal.DagFactory;
import com.salesforce.apollo.ethereal.DagReader;
import com.salesforce.apollo.ethereal.DagTest;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class ChRbcTest {

    @Test
    public void multiple() throws Exception {
        HashMap<Short, Map<Integer, List<Unit>>> units;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            var dag = DagReader.readDag(fis, new DagFactory.TestDagFactory()).dag();
            units = DagTest.collectUnits(dag);
        }

        String prefix = UUID.randomUUID().toString();
        Executor exec = Executors.newFixedThreadPool(2);
        var context = Context.newBuilder().setCardinality(10).build();
        List<SigningMember> members = IntStream.range(0, 4)
                                               .mapToObj(i -> (SigningMember) new SigningMemberImpl(Utils.getMember(i)))
                                               .toList();
        List<LocalRouter> comms = new ArrayList<>();
        members.forEach(m -> context.activate(m));
        var builder = Config.deterministic()
                            .setnProc((short) members.size())
                            .setVerifiers(members.toArray(new Verifier[members.size()]));
        List<ChRbcGossiper> gossipers = new ArrayList<>();
        List<ChRbcAdder> adders = new ArrayList<>();
        int maxSize = 1024 * 1024;

        for (var i = 0; i < members.size(); i++) {
            var comm = new LocalRouter(prefix, ServerConnectionCache.newBuilder(), exec, null);
            comms.add(comm);
            comm.setMember(members.get(i));
            final var config = builder.setSigner(members.get(i)).setPid((short) i).build();
            ChRbcAdder adder = new ChRbcAdder(0, new DagImpl(config, 0), maxSize, config, context.toleranceLevel(),
                                              new ConcurrentSkipListSet<>());
            adders.add(adder);
            gossipers.add(new ChRbcGossiper(context, members.get(i), new Processor() {

                @Override
                public void updateFrom(Update update) {
                    adder.updateFrom(update.getMissings(0));
                }

                @Override
                public Update update(Update update) {
                    var builder = Update.newBuilder();
                    final var missing = update.getMissings(0);
                    adder.updateFrom(missing);
                    builder.addMissings(adder.updateFor(missing.getHaves()));
                    return builder.build();
                }

                @Override
                public Update gossip(Gossip gossip) {
                    return Update.newBuilder().addMissings(adder.updateFor(gossip.getHavesList().get(0))).build();
                }

                @Override
                public Gossip gossip(Digest context, int ring) {
                    return Gossip.newBuilder()
                                 .setContext(context.toDigeste())
                                 .setRing(ring)
                                 .addHaves(adder.have())
                                 .build();
                }
            }, comm, exec, null));
        }

        comms.forEach(lr -> lr.start());
        Duration duration = Duration.ofMillis(10);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        gossipers.forEach(e -> e.start(duration, scheduler));

        var maxLevel = units.get((short) 0).size();
        System.out.println("Dealing");
        for (var pid : units.keySet()) {
            adders.get(pid).produce(units.get(pid).get(0).get(0));
        }
        try {
            for (var level = 1; level < maxLevel; level++) {
                System.out.println("Level: " + level);
                for (var pid : units.keySet()) {
                    adders.get(pid).produce(units.get(pid).get(level).get(0));
                }
                for (var pid : units.keySet()) {
                    System.out.println("   pid: " + pid);
                    Unit u = units.get(pid).get(level - 1).get(0);
                    assertTrue(Utils.waitForCondition(5_000,
                                                      () -> adders.stream()
                                                                  .map(a -> a.getDag())
                                                                  .map(d -> d.contains(u.hash()))
                                                                  .allMatch(b -> b)),
                               "Failed at level: " + level);
                }
            }
        } finally {
            gossipers.forEach(g -> g.stop());
            comms.forEach(lr -> lr.close());
            System.out.println("=== DUMP ===");
            adders.stream().map(a -> a.dump()).forEach(dump -> {
                System.out.println(dump);
            });
        }
    }
}
