/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.time.Duration;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.protocols.Utils;
import com.salesforce.apollo.web.resources.ByteTransactionApi;
import com.salesforce.apollo.web.resources.ByteTransactionApi.TransactionResult;
import com.salesforce.apollo.web.resources.DagApi.QueryFinalizedResult;
import com.salesforce.apollo.web.resources.GenesisBlockApi.Result;

/**
 * @author hhildebrand
 *
 */

public class ClusterTesting {
    // Create the genesis block
    private static boolean CREATE_GENESIS = true;

    private static final Decoder DECODER = Base64.getUrlDecoder();

    // Load balancer EP for the cluster
    private static final String LOAD_BALANCER = "aff2c3e35066011eaac87028ff60873f-386560756.us-west-2.elb.amazonaws.com";

    private static final Logger log = LoggerFactory.getLogger(ClusterTesting.class);

    @Test
    public void loadTest() throws Exception {
        Client client = ClientBuilder.newClient();

        final WebTarget endpoint = client.target(new URL("http", LOAD_BALANCER, 8080, "/").toURI());

        smokeLoad(Duration.ofSeconds(200), endpoint, Duration.ofMillis(100), 200, Duration.ofSeconds(1), 10,
                  Duration.ofMillis(20));
    }

    @Test
    public void smoke() throws Exception {
        Client client = ClientBuilder.newClient();

        final WebTarget endpoint = client.target(new URL("http", LOAD_BALANCER, 8080, "/").toURI());

        // create Genesis
        if (CREATE_GENESIS) {
            createGenesis(endpoint);
        }

        smokeSyncApi(endpoint);
        smokeAsync(endpoint);
    }

    private void createGenesis(WebTarget endpoint) {
        Response response = endpoint.path("api/genesisBlock/create")
                                    .request(MediaType.APPLICATION_JSON)
                                    .post(Entity.json(new String(
                                            Base64.getUrlEncoder().withoutPadding().encode("Hello World".getBytes()))));

        assertEquals(200, response.getStatus());
        Result genesisResult = response.readEntity(Result.class);
        assertNotNull(genesisResult);
        assertFalse(genesisResult.errorMessage, genesisResult.error);
    }

    private void smokeAsync(WebTarget endpoint) {
        Response response = endpoint.path("api/byteTransaction/submitAsync")
                                    .request(MediaType.APPLICATION_JSON)
                                    .post(Entity.json(new ByteTransactionApi.ByteTransaction(40_000,
                                            "Hello World 2".getBytes())));

        assertEquals(200, response.getStatus());
        String asyncResult = response.readEntity(String.class);
        assertNotNull(asyncResult);

        final WebTarget queryFinalized = endpoint.path("api/dag/queryFinalized");
        assertTrue(Utils.waitForCondition(60_000, 1_000, () -> {
            Response r = queryFinalized.request().post(Entity.text(asyncResult));
            return r.getStatus() == 200 && r.readEntity(QueryFinalizedResult.class).isFinalized();
        }));
    }

    private void smokeLoad(Duration duration, WebTarget endpoint, Duration initialDelay, int outstanding,
                           Duration queryInterval, int maxDelta, Duration submitInterval) {
        MetricRegistry registry = new MetricRegistry();
        Transactioneer t = new Transactioneer(registry);
        final WebTarget submitEndpoint = endpoint.path("api/byteTransaction/submitAsync");
        final WebTarget queryEndpoint = endpoint.path("api/dag/fetch");
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
        ConsoleReporter.forRegistry(registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .start(30, TimeUnit.SECONDS);

        log.info("Starting load test");
        long then = System.currentTimeMillis();
        t.start(scheduler, duration, scheduler, queryInterval, queryEndpoint, scheduler, submitInterval, submitEndpoint,
                outstanding, initialDelay, maxDelta);

        log.info("Load test started");

        Utils.waitForCondition((int) (duration.toMillis() + 10_000), 1000, () -> t.isFinished());

        final long testDuration = System.currentTimeMillis() - then;

        int tps = (int) (t.getFinalized().size() / testDuration);

        log.info("Finalized {} in {} ms ({} TPS) ({} unfinalized)}", t.getFinalized().size(), testDuration, tps,
                 t.getUnfinalized().size());

        System.out.println();
        System.out.print("Final Metrics");
        ConsoleReporter.forRegistry(registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
        AtomicInteger count = new AtomicInteger();
        t.getUnfinalized().forEach(txn -> {
            System.out.print(txn);
            final int current = count.incrementAndGet();
            if (current % 8 == 0) {
                System.out.println();
            } else {
                System.out.print(", ");
            }
        });
    }

    private void smokeSyncApi(WebTarget endpoint) throws Exception {
        Response response = endpoint.path("api/byteTransaction/submit")
                                    .request(MediaType.APPLICATION_JSON)
                                    .post(Entity.json(new ByteTransactionApi.ByteTransaction(40_000,
                                            "Hello World".getBytes())));

        assertEquals(200, response.getStatus());
        TransactionResult result = response.readEntity(TransactionResult.class);
        assertNotNull(result);
        assertFalse(result.errorMessage, result.error);

        response = endpoint.path("api/dag/fetch").request().post(Entity.text(result.result));

        assertEquals(200, response.getStatus());
        String fetched = response.readEntity(String.class);
        assertNotNull(fetched);
        assertEquals("Hello World", new String(DECODER.decode(fetched)));
    }
}
