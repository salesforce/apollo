/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
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
        final Duration timeout = Duration.ofSeconds(3);
        AtomicBoolean proceed = new AtomicBoolean(true);
        MetricRegistry reg = new MetricRegistry();
        Timer latency = reg.timer("Transaction latency");
        Counter timeouts = reg.counter("Transaction timeouts");
        AtomicInteger lineTotal = new AtomicInteger();
        var transactioneers = new ArrayList<Transactioneer>();
        final int waitFor = 23;
        final int clientCount = 10;
        final int max = 10;
        final CountDownLatch countdown = new CountDownLatch((choams.size() - 1) * clientCount);
        final ScheduledExecutorService txScheduler = Executors.newScheduledThreadPool(100);

        routers.entrySet().stream().filter(e -> !e.getKey().equals(testSubject.getId())).map(e -> e.getValue())
               .forEach(r -> r.start());
        choams.entrySet().stream().filter(e -> !e.getKey().equals(testSubject.getId())).map(e -> e.getValue())
              .forEach(ch -> ch.start());

        Utils.waitForCondition(15_000, 100, () -> blocks.values().stream().mapToInt(l -> l.size())
                                                          .filter(s -> s >= waitFor).count() == choams.size() - 1);
        assertEquals(choams.size() - 1,
                     blocks.values().stream().mapToInt(l -> l.size()).filter(s -> s >= waitFor).count(),
                     "Failed: " + blocks.get(members.get(0).getId()).size());

        final var initial = choams.get(members.get(0).getId()).getSession().submit(initialInsert(), timeout);
        initial.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

        for (int i = 0; i < clientCount; i++) {
            choams.entrySet().stream().filter(e -> !e.getKey().equals(testSubject.getId())).map(e -> e.getValue())
                  .map(c -> new Transactioneer(c, timeout, timeouts, latency, proceed, lineTotal, max, countdown,
                                               txScheduler))
                  .forEach(e -> transactioneers.add(e));
        }
        System.out.println("# of clients: " + (choams.size() - 1) * clientCount);
        System.out.println("Starting txns");
        transactioneers.stream().forEach(e -> e.start());
        Thread.sleep(5_000);
        System.out.println("Starting late joining node");
        choams.get(testSubject.getId()).start();
        routers.get(testSubject.getId()).start();

        try {
            countdown.await(120, TimeUnit.SECONDS);
        } finally {
            proceed.set(false);
        }

        System.out.println();
        System.out.println();
        System.out.println("Checking replica consistency");

        Connection connection = updaters.get(members.get(0)).newConnection();
        Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery("select ID, from books");
        ResultSetMetaData rsmd = results.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (results.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1)
                    System.out.print(",  ");
                Object columnValue = results.getObject(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
    }
}
