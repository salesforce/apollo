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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.joou.ULong;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class CheckpointBootstrapTest extends AbstractLifecycleTest {

    static {
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Session.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CHOAM.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(GenesisAssembly.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ViewAssembly.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Producer.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Committee.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Fsm.class)).setLevel(Level.TRACE);
    }

    @Test
    public void checkpointBootstrap() throws Exception {
        final Duration timeout = Duration.ofSeconds(6);
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

        final var activated = Utils.waitForCondition(30_000, 1_000,
                                                     () -> choams.entrySet()
                                                                 .stream()
                                                                 .filter(e -> !e.getKey().equals(testSubject.getId()))
                                                                 .map(e -> e.getValue())
                                                                 .filter(c -> !c.active())
                                                                 .count() == 0);
        assertTrue(activated,
                   "System did not become active: " + (choams.entrySet()
                                                             .stream()
                                                             .filter(e -> !e.getKey().equals(testSubject.getId()))
                                                             .map(e -> e.getValue())
                                                             .filter(c -> !c.active())
                                                             .map(c -> c.getId())
                                                             .toList()));

        final var submitter = updaters.entrySet()
                                      .stream()
                                      .filter(e -> !e.getKey().equals(testSubject))
                                      .findFirst()
                                      .get();

        var mutator = submitter.getValue().getMutator(choams.get(submitter.getKey().getId()).getSession());
        Supplier<Txn> update = () -> update(entropy, mutator);
        var transactioneer = new Transactioneer(update, mutator, timeout, max, txExecutor, countdown, txScheduler);

        System.out.println("# of clients: " + (choams.size() - 1) * clientCount);
        System.out.println("Starting txns");

        transactioneer.start();

        assertTrue(countdown.await(60, TimeUnit.SECONDS), "Did not complete transactions");
        assertTrue(checkpointOccurred.await(60, TimeUnit.SECONDS), "Checkpoints did not complete");

        ULong chkptHeight = checkpointHeight.get();
        System.out.println("Checkpoint at height: " + chkptHeight);

        assertTrue(Utils.waitForCondition(10_000, 1_000, () -> {
            return members.stream()
                          .filter(m -> !m.equals(testSubject))
                          .map(m -> updaters.get(m))
                          .map(ssm -> ssm.getCurrentBlock())
                          .filter(cb -> cb != null)
                          .map(cb -> cb.height())
                          .filter(l -> l.compareTo(chkptHeight) >= 0)
                          .count() == members.size() - 1;
        }), "All members did not process the checkpoint");

        System.out.println("Starting late joining node");
        var choam = choams.get(testSubject.getId());
        choam.context().activate(testSubject);
        choam.start();
        routers.get(testSubject.getId()).start();

        assertTrue(Utils.waitForCondition(30_000, 1000, () -> {
            if (transactioneer.inFlight() != 0) {
                return false;
            }
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
                          .filter(l -> l.compareTo(mT) == 0)
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
