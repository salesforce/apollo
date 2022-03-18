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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.joou.ULong;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class GenesisBootstrapTest extends AbstractLifecycleTest {

    @Test
    public void genesisBootstrap() throws Exception {
        final SigningMember testSubject = members.get(CARDINALITY - 1);
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
        Thread.sleep(2_000);

        final var initial = choams.get(members.get(0).getId())
                                  .getSession()
                                  .submit(ForkJoinPool.commonPool(), initialInsert(), timeout, txScheduler);
        initial.get(10, TimeUnit.SECONDS);
        var txneer = updaters.entrySet().stream().filter(e -> !e.getKey().equals(testSubject)).findFirst().get();

        var mutator = txneer.getValue().getMutator(choams.get(txneer.getKey().getId()).getSession());
        transactioneers.add(new Transactioneer(() -> update(entropy, mutator), mutator, timeout, 1, txExecutor,
                                               countdown, txScheduler));
        System.out.println("Transaction member: " + txneer.getKey().getId());
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

        final ULong target = txneer.getValue().getCurrentBlock().height();

        assertTrue(Utils.waitForCondition(120_000, 100,
                                          () -> members.stream()
                                                       .map(m -> updaters.get(m))
                                                       .map(ssm -> ssm.getCurrentBlock())
                                                       .filter(cb -> cb != null)
                                                       .map(cb -> cb.height())
                                                       .filter(l -> l.compareTo(target) >= 0)
                                                       .count() == members.size()),
                   "state: " + members.stream()
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
