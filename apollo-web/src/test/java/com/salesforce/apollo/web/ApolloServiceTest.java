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

import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.salesforce.apollo.Apollo;
import com.salesforce.apollo.ApolloConfiguration;
import com.salesforce.apollo.PregenPopulation;
import com.salesforce.apollo.protocols.Utils;
import com.salesforce.apollo.web.resources.ByteTransactionApi;
import com.salesforce.apollo.web.resources.ByteTransactionApi.TransactionResult;
import com.salesforce.apollo.web.resources.DagApi.DagNode;
import com.salesforce.apollo.web.resources.DagApi.QueryFinalizedResult;
import com.salesforce.apollo.web.resources.GenesisBlockApi.Result;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;

/**
 * @author hhildebrand
 */
public class ApolloServiceTest {

    private static File baseDir;

    private static final Decoder DECODER = Base64.getUrlDecoder();

    static {
        ApolloConfiguration.SimCommunicationsFactory.reset();
    }

    @BeforeClass
    public static void before() {
        baseDir = new File(System.getProperty("user.dir"), "target/cluster");
        Utils.clean(baseDir);
        baseDir.mkdirs();
    }

    private static DropwizardAppExtension<ApolloServiceConfiguration> EXT = new DropwizardAppExtension<>(
            ApolloService.class, ResourceHelpers.resourceFilePath("server.yml"));

    @Test
    public void smoke() throws Exception {
        List<Apollo> oracles = new ArrayList<>();

        for (int i = 2; i < PregenPopulation.getCardinality() + 1; i++) {
            ApolloConfiguration config = new ApolloConfiguration();
            config.avalanche.core.alpha = 0.6;
            config.avalanche.core.k = 6;
            config.avalanche.core.beta1 = 3;
            config.avalanche.core.beta2 = 5;
            config.communications = new ApolloConfiguration.SimCommunicationsFactory();
            ApolloConfiguration.ResourceIdentitySource ks = new ApolloConfiguration.ResourceIdentitySource();
            ks.store = PregenPopulation.memberKeystoreResource(i);
            config.source = ks;
            oracles.add(new Apollo(config));
        }
        long then = System.currentTimeMillis();

        oracles.forEach(oracle -> {
            try {
                oracle.start();
            } catch (Exception e) {
                throw new IllegalStateException("unable to start oracle", e);
            }
        });

        Client client = EXT.client();

        boolean stabilized = Utils.waitForCondition(15_000, 1_000, () -> {
            return oracles.stream()
                          .map(o -> o.getView())
                          .map(view -> view.getLive().size() != oracles.size() + 1 ? view : null)
                          .filter(view -> view != null)
                          .count() == 0;
        });
        assertTrue("View did not stabilize", stabilized);
        System.out.println("View stabilized across " + (oracles.size() + 1) + " members in "
                + (System.currentTimeMillis() - then) + " millis");

        Response response = client.target(String.format("http://localhost:%d/api/genesisBlock/create",
                                                        EXT.getLocalPort()))
                                  .request(MediaType.APPLICATION_JSON)
                                  .post(Entity.json(new String(
                                          Base64.getUrlEncoder().withoutPadding().encode("Hello World".getBytes()))));
        assertEquals(200, response.getStatus());
        Result genesisResult = response.readEntity(Result.class);
        assertNotNull(genesisResult);
        assertFalse(genesisResult.errorMessage, genesisResult.error);

        response = client.target(String.format("http://localhost:%d/api/byteTransaction/submit", EXT.getLocalPort()))
                         .request(MediaType.APPLICATION_JSON)
                         .post(Entity.json(new ByteTransactionApi.ByteTransaction(40_000, "Hello World".getBytes())));

        assertEquals(200, response.getStatus());
        TransactionResult result = response.readEntity(TransactionResult.class);
        assertNotNull(result);
        assertFalse(result.errorMessage, result.error);

        response = client.target(String.format("http://localhost:%d/api/dag/fetch", EXT.getLocalPort()))
                         .request()
                         .post(Entity.text(result.result));

        assertEquals(200, response.getStatus());
        String fetched = response.readEntity(String.class);
        assertNotNull(fetched);
        assertEquals("Hello World", new String(DECODER.decode(fetched)));

        response = client.target(String.format("http://localhost:%d/api/dag/fetchDagNode", EXT.getLocalPort()))
                         .request()
                         .post(Entity.text(result.result));

        assertEquals(200, response.getStatus());
        DagNode dagNode = response.readEntity(DagNode.class);
        assertNotNull(dagNode);
        assertNotNull(dagNode.getData());
        assertEquals("Hello World", new String(DECODER.decode(dagNode.getData())));
        assertNotNull(dagNode.getLinks());
        assertFalse(dagNode.getLinks().isEmpty());

        // Asynchronous transaction case

        response = client.target(String.format("http://localhost:%d/api/byteTransaction/submitAsync",
                                               EXT.getLocalPort()))
                         .request(MediaType.APPLICATION_JSON)
                         .post(Entity.json(new ByteTransactionApi.ByteTransaction(40_000, "Hello World 2".getBytes())));

        assertEquals(200, response.getStatus());
        String asyncResult = response.readEntity(String.class);
        assertNotNull(asyncResult);
        assertTrue(Utils.waitForCondition(60_000, 1_000, () -> {
            Response r = client.target(String.format("http://localhost:%d/api/dag/queryFinalized", EXT.getLocalPort()))
                               .request()
                               .post(Entity.text(asyncResult));
            return r.getStatus() == 200 && r.readEntity(QueryFinalizedResult.class).isFinalized();
        }));
        response = client.target(String.format("http://localhost:%d/metrics?pretty=true", EXT.getAdminPort()))
                         .request()
                         .get();

        assertEquals(200, response.getStatus());
        System.out.println(response.readEntity(String.class));
    }

    @After
    public void stop() {
        try {
            ((ApolloService) EXT.getApplication()).stop();
        } catch (Throwable e) {
            // ignore
        }
    }
}
