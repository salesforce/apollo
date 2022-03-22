/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.joou.ULong;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class CheckpointBootstrapTest extends AbstractLifecycleTest {

    @Test
    public void checkpointBootstrap() throws Exception {
        final SigningMember testSubject = members.get(CARDINALITY - 1);
        final Duration timeout = Duration.ofSeconds(6);
        var transactioneers = new ArrayList<Transactioneer>();
        final int clientCount = 1;
        final int max = 1;
        final CountDownLatch countdown = new CountDownLatch((choams.size() - 1) * clientCount);

        routers.entrySet()
               .stream()
               .filter(e -> !e.getKey().equals(testSubject.getId()))
               .map(e -> e.getValue())
               .forEach(r -> r.start());
        choams.entrySet()
              .stream()
              .filter(e -> !e.getKey().equals(testSubject.getId()))
              .map(e -> e.getValue())
              .forEach(ch -> ch.start());

        assertTrue(Utils.waitForCondition(30_000,
                                          () -> choams.entrySet()
                                                      .stream()
                                                      .filter(e -> !e.getKey().equals(testSubject.getId()))
                                                      .map(e -> e.getValue())
                                                      .filter(c -> !c.active())
                                                      .count() == 0),
                   "System did not become active");

        for (int i = 0; i < 1; i++) {
            updaters.entrySet().stream().filter(e -> !e.getKey().equals(testSubject)).map(e -> {
                var mutator = e.getValue().getMutator(choams.get(e.getKey().getId()).getSession());
                Supplier<Txn> update = () -> update(entropy, mutator);
                return new Transactioneer(update, mutator, timeout, max, txExecutor, countdown, txScheduler);
            }).forEach(e -> transactioneers.add(e));
        }

        System.out.println("# of clients: " + (choams.size() - 1) * clientCount);
        System.out.println("Starting txns");

        transactioneers.stream().forEach(e -> e.start());
        checkpointOccurred.whenComplete((s, t) -> {
            txExecutor.execute(() -> {
                System.out.println("Starting late joining node");
                var choam = choams.get(testSubject.getId());
                choam.context().activate(Collections.singletonList(testSubject));
                choam.start();
                routers.get(testSubject.getId()).start();
            });
        });

        assertTrue(countdown.await(120, TimeUnit.SECONDS), "Did not complete transactions");
        assertTrue(checkpointOccurred.get(120, TimeUnit.SECONDS), "Checkpoint did not occur");

        System.out.println("State: " + updaters.values().stream().map(ssm -> ssm.getCurrentBlock()).toList());
        final ULong target = updaters.values()
                                     .stream()
                                     .map(ssm -> ssm.getCurrentBlock())
                                     .filter(cb -> cb != null)
                                     .map(cb -> cb.height())
                                     .max((a, b) -> a.compareTo(b))
                                     .get();

        assertTrue(Utils.waitForCondition(10_000, 1000, () -> {
            var mT = members.stream()
                            .map(m -> updaters.get(m))
                            .map(ssm -> ssm.getCurrentBlock())
                            .filter(cb -> cb != null)
                            .map(cb -> cb.height())
                            .max((a, b) -> a.compareTo(b))
                            .get();
            return members.stream()
                          .map(m -> updaters.get(m))
                          .map(ssm -> ssm.getCurrentBlock())
                          .filter(cb -> cb != null)
                          .map(cb -> cb.height())
                          .filter(l -> l.compareTo(target) >= 0)
                          .filter(l -> l.compareTo(mT) == 0)
                          .count() == members.size();
        }), "state: " + members.stream()
                               .map(m -> updaters.get(m))
                               .map(ssm -> ssm.getCurrentBlock())
                               .filter(cb -> cb != null)
                               .map(cb -> cb.height())
                               .toList());

        System.out.println("target: " + target + " results: "
        + members.stream()
                 .map(m -> updaters.get(m))
                 .map(ssm -> ssm.getCurrentBlock())
                 .filter(cb -> cb != null)
                 .map(cb -> cb.height())
                 .toList());

        System.out.println();

        record row(float price, int quantity) {}

        System.out.println("Checking replica consistency");

        Map<Member, Map<Integer, row>> manifested = new HashMap<>();

        for (Member m : members) {
            Connection connection = updaters.get(m).newConnection();
            Statement statement = connection.createStatement();
            try {
                ResultSet results = statement.executeQuery("select ID, PRICE, QTY from books");
                while (results.next()) {
                    manifested.computeIfAbsent(m, k -> new HashMap<>())
                              .put(results.getInt("ID"), new row(results.getFloat("PRICE"), results.getInt("QTY")));
                }
            } catch (Throwable e) {
                // ignore for now;
            }
            connection.close();
        }

        Map<Integer, row> standard = manifested.get(members.get(0));
        assertNotNull(standard, "No results for: " + members.get(0));
        for (Member m : members) {
            var candidate = manifested.get(m);
            assertNotNull(candidate, "No results for: " + m);
            for (var entry : standard.entrySet()) {
                assertTrue(candidate.containsKey(entry.getKey()));
                assertEquals(entry.getValue(), candidate.get(entry.getKey()));
            }
        }
    }

    @Override
    protected int checkpointBlockSize() {
        return 1;
    }
}
