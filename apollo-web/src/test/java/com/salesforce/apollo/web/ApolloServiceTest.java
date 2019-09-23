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

import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.junit.AfterClass;
import org.junit.ClassRule;
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
import io.dropwizard.testing.junit.DropwizardAppRule;

/**
 * @author hhildebrand
 */
public class ApolloServiceTest {

    @ClassRule
    public static final DropwizardAppRule<ApolloServiceConfiguration> RULE = new DropwizardAppRule<ApolloServiceConfiguration>(ApolloService.class,
                                                                                                                               ResourceHelpers.resourceFilePath("server.yml")) {

        @Override
        protected JerseyClientBuilder clientBuilder() {
            return super.clientBuilder().property(ClientProperties.CONNECT_TIMEOUT, 1000)
                                        .property(ClientProperties.READ_TIMEOUT, 60_000);
        }
    };

    private static final Decoder DECODER = Base64.getUrlDecoder();

    @AfterClass
    public static void stop() {
        try {
            ((ApolloService)RULE.getApplication()).stop();
        } catch (Throwable e) {
            // ignore
        }
    }

    @Test
    public void smoke() throws Exception {
        List<Apollo> oracles = new ArrayList<>();

        for (int i = 2; i < PregenPopulation.getCardinality(); i++) {
            ApolloConfiguration config = new ApolloConfiguration();
            config.avalanche.alpha = 0.6;
            config.avalanche.k = 6;
            config.avalanche.beta1 = 3;
            config.avalanche.beta2 = 5;
            config.avalanche.dbConnect = "jdbc:h2:mem:test-" + i + ";DB_CLOSE_ON_EXIT=FALSE";
            config.avalanche.limit = 20;
            config.avalanche.parentCount = 3;
            config.avalanche.epsilon = 9;
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

        Client client = RULE.client();

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

        Response response = client.target(
                                          String.format("http://localhost:%d/api/genesisBlock/create",
                                                        RULE.getLocalPort()))
                                  .request(MediaType.APPLICATION_JSON)
                                  .post(Entity.json(new String(Base64.getUrlEncoder()
                                                                     .withoutPadding()
                                                                     .encode("Hello World".getBytes()))));
        assertEquals(200, response.getStatus());
        Result genesisResult = response.readEntity(Result.class);
        assertNotNull(genesisResult);
        assertFalse(genesisResult.errorMessage, genesisResult.error);

        response = client.target(
                                 String.format("http://localhost:%d/api/byteTransaction/submit",
                                               RULE.getLocalPort()))
                         .request(MediaType.APPLICATION_JSON)
                         .post(Entity.json(new ByteTransactionApi.ByteTransaction(40_000,
                                                                                  "Hello World".getBytes())));

        assertEquals(200, response.getStatus());
        TransactionResult result = response.readEntity(TransactionResult.class);
        assertNotNull(result);
        assertFalse(result.errorMessage, result.error);

        response = client.target(
                                 String.format("http://localhost:%d/api/dag/fetch",
                                               RULE.getLocalPort()))
                         .request()
                         .post(Entity.text(result.result));

        assertEquals(200, response.getStatus());
        String fetched = response.readEntity(String.class);
        assertNotNull(fetched);
        assertEquals("Hello World", new String(DECODER.decode(fetched)));

        response = client.target(
                                 String.format("http://localhost:%d/api/dag/fetchDagNode",
                                               RULE.getLocalPort()))
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

        response = client.target(
                                 String.format("http://localhost:%d/api/byteTransaction/submitAsync",
                                               RULE.getLocalPort()))
                         .request(MediaType.APPLICATION_JSON)
                         .post(Entity.json(new ByteTransactionApi.ByteTransaction(40_000,
                                                                                  "Hello World 2".getBytes())));

        assertEquals(200, response.getStatus());
        String asyncResult = response.readEntity(String.class);
        assertNotNull(asyncResult);
        assertTrue(Utils.waitForCondition(60_000, 1_000, () -> {
            Response r = client.target(
                                       String.format("http://localhost:%d/api/dag/queryFinalized",
                                                     RULE.getLocalPort()))
                               .request()
                               .post(Entity.text(asyncResult));
            return r.getStatus() == 200 && r.readEntity(QueryFinalizedResult.class).isFinalized();
        }));
    }
}
