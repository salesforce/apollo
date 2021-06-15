/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.web;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hhildebrand
 *
 */
@Ignore
public class ClusterTesting { 

    // Load balancer EP for the cluster
    private static final String LOAD_BALANCER = "aff2c3e35066011eaac87028ff60873f-386560756.us-west-2.elb.amazonaws.com";

    private static final Logger log = LoggerFactory.getLogger(ClusterTesting.class);

//    @Test
//    public void createGenesis() throws Exception {
//        ClientConfig configuration = new ClientConfig();
//        configuration.property(ClientProperties.CONNECT_TIMEOUT, 1000);
//        configuration.property(ClientProperties.READ_TIMEOUT, 60000);
//        Client client = ClientBuilder.newClient(configuration);
//
//        final WebTarget endpoint = client.target(new URL("http", LOAD_BALANCER, 8080, "/").toURI());
//
//        // create Genesis
//        if (CREATE_GENESIS) {
//            createGenesis(endpoint);
//        }
//
//        smokeSyncApi(endpoint);
//        smokeAsync(endpoint);
//    }

    @Test
    public void loadTest() throws Exception {

        smokeLoad(20, Duration.ofSeconds(600), Duration.ofMillis(300), 400, Duration.ofSeconds(1), 100,
                  Duration.ofMillis(15));
    }

    private void smokeLoad(int clientCount, Duration duration, Duration initialDelay, int outstanding,
                           Duration queryInterval, int batchSize, Duration submitInterval) {
        MetricRegistry registry = new MetricRegistry();
        List<Transactioneer> txneers = new ArrayList<>();
        for (int i = 0; i < clientCount; i++) {
            txneers.add(new Transactioneer(registry));
        }
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(clientCount * 2);
        ConsoleReporter.forRegistry(registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .start(30, TimeUnit.SECONDS);

        log.info("Starting load test for {} simulated clients", clientCount);
        long then = System.currentTimeMillis();
        txneers.forEach(t -> {
            Client client = ClientBuilder.newClient();
            WebTarget endpoint;
            try {
                endpoint = client.target(new URL("http", LOAD_BALANCER, 8080, "/").toURI());
            } catch (MalformedURLException | URISyntaxException e) {
                throw new IllegalStateException(e);
            }

            final WebTarget submitEndpoint = endpoint.path("api/byteTransaction/submitAll");
            final WebTarget queryEndpoint = endpoint.path("api/dag/queryAllFinalized");

            t.start(scheduler, duration, scheduler, queryInterval, queryEndpoint, scheduler, submitInterval,
                    submitEndpoint, outstanding, initialDelay, batchSize);
        });

        log.info("Load test started");

        Utils.waitForCondition((int) (duration.toMillis() + 10_000), 1000,
                               () -> txneers.stream().map(t -> t.isFinished()).filter(e -> !e).count() == 0);

        final long testDuration = System.currentTimeMillis() - then;

        int tps = (int) (txneers.stream().mapToInt(t -> t.getFinalized().size()).sum() / (testDuration / 1000));

        log.info("Finalized {} in {} ms ({} TPS) ({} unfinalized)}",
                 txneers.stream().mapToInt(t -> t.getFinalized().size()).sum(), testDuration, tps,
                 txneers.stream().mapToInt(t -> t.getUnfinalized().size()).sum());

        System.out.println();
        System.out.print("Final Metrics");
        ConsoleReporter.forRegistry(registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
//        AtomicInteger count = new AtomicInteger();
//        t.getUnfinalized().forEach(txn -> {
//            System.out.print(txn);
//            final int current = count.incrementAndGet();
//            if (current % 8 == 0) {
//                System.out.println();
//            } else {
//                System.out.print(", ");
//            }
//        });
    }
}
