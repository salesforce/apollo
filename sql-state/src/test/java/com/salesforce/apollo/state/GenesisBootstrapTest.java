/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class GenesisBootstrapTest extends AbstractLifecycleTest {

    @Test
    public void genesisBootstrap() throws Exception {
        final Duration timeout = Duration.ofSeconds(6);
        var transactioneers = new ArrayList<Transactioneer>();
        final CountDownLatch countdown = new CountDownLatch(1);

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

        var txneer = updaters.get(members.get(0));

        assertTrue(Utils.waitForCondition(30_000, 1_000, () -> choams.get(members.get(0).getId()).active()),
                   "txneer did not become active");

        var mutator = txneer.getMutator(choams.get(members.get(0).getId()).getSession());
        transactioneers.add(new Transactioneer(() -> update(entropy, mutator), mutator, timeout, 1, txExecutor,
                                               countdown, txScheduler));
        System.out.println("Transaction member: " + members.get(0).getId());
        System.out.println("Starting txns");
        transactioneers.stream().forEach(e -> e.start());
        var success = countdown.await(60, TimeUnit.SECONDS);
        assertTrue(success,
                   "Did not complete transactions: " + (transactioneers.stream().mapToInt(t -> t.completed()).sum()));

        System.out.println("Starting late joining node");
        var choam = choams.get(testSubject.getId());
        choam.context().activate(Collections.singletonList(testSubject));
        choam.start();
        routers.get(testSubject.getId()).start();

        assertTrue(Utils.waitForCondition(120_000, 1000, () -> {
            if (!(transactioneers.stream()
                                 .mapToInt(t -> t.inFlight())
                                 .filter(t -> t == 0)
                                 .count() == transactioneers.size())) {
                return false;
            }

            var max = members.stream()
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
                          .filter(l -> l.compareTo(max) == 0)
                          .count() == members.size();
        }), "state: " + members.stream()
                               .map(m -> updaters.get(m))
                               .map(ssm -> ssm.getCurrentBlock())
                               .filter(cb -> cb != null)
                               .map(cb -> cb.height())
                               .toList());

        choams.values().forEach(e -> e.stop());
        routers.values().forEach(e -> e.close());

        System.out.println("Final state: " + members.stream()
                                                    .map(m -> updaters.get(m))
                                                    .map(ssm -> ssm.getCurrentBlock())
                                                    .filter(cb -> cb != null)
                                                    .map(cb -> cb.height())
                                                    .toList());

        System.out.println();
        System.out.println();
        System.out.println();

        record row(float price, int quantity) {}

        System.out.println("Checking replica consistency");

        Map<Member, Map<Integer, row>> manifested = new HashMap<>();

        for (Member m : members) {
            Connection connection = updaters.get(m).newConnection();
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("select ID, PRICE, QTY from books");
            while (results.next()) {
                manifested.computeIfAbsent(m, k -> new HashMap<>())
                          .put(results.getInt("ID"), new row(results.getFloat("PRICE"), results.getInt("QTY")));
            }
            connection.close();
        }

        Map<Integer, row> standard = manifested.get(members.get(0));
        for (Member m : members) {
            var candidate = manifested.get(m);
            for (var entry : standard.entrySet()) {
                assertTrue(candidate.containsKey(entry.getKey()));
                assertEquals(entry.getValue(), candidate.get(entry.getKey()));
            }
        }
    }

    @Override
    protected int checkpointBlockSize() {
        return 10;
    }
}
