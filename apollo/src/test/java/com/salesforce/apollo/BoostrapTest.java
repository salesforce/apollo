/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */

package com.salesforce.apollo;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.junit.ClassRule;
import org.junit.Test;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.bootstrap.BootstrapCA;
import com.salesforce.apollo.bootstrap.BootstrapConfiguration;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;

/**
 * @author hhildebrand
 */
public class BoostrapTest {

    @ClassRule
    public static final DropwizardAppRule<BootstrapConfiguration> RULE = new DropwizardAppRule<BootstrapConfiguration>(BootstrapCA.class,
                                                                                                                       ResourceHelpers.resourceFilePath("bootstrap.yml")) {

        @Override
        protected JerseyClientBuilder clientBuilder() {
            return super.clientBuilder().property(ClientProperties.CONNECT_TIMEOUT, 1000)
                                        .property(ClientProperties.READ_TIMEOUT, 60_000);
        }
    };

    private Random entropy;

    @Test
    public void smoke() throws Exception {
        entropy = new Random(0x666);
        List<Apollo> oracles = new ArrayList<>();
        URL endpoint = new URL(String.format("http://localhost:%d/api/cnc/mint",
                                             RULE.getLocalPort()));

        for (int i = 1; i < PregenPopulation.getCardinality(); i++) {
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
            BootstrapIdSource ks = new BootstrapIdSource();
            ks.endpoint = endpoint;
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

        Utils.waitForCondition(15_000, 1_000, () -> {
            return oracles.stream()
                          .map(o -> o.getView())
                          .map(view -> view.getLive().size() != oracles.size() ? view : null)
                          .filter(view -> view != null)
                          .count() == 0;
        });

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
                + oracles.size() + " members");

        Map<HashKey, DagEntry> stored = new HashMap<>();

        DagEntry root = new DagEntry();
        root.setData(ByteBuffer.wrap("root node".getBytes()));
        stored.put(oracles.get(0).getGhost().putDagEntry(root), root);

        int rounds = 10;

        for (int i = 0; i < rounds; i++) {
            for (Apollo oracle : oracles) {
                DagEntry entry = new DagEntry();
                entry.setData(ByteBuffer.wrap(String.format("Member: %s round: %s", oracle.getGhost().getNode().getId(),
                                                            i)
                                                    .getBytes()));
                entry.setLinks(randomLinksTo(stored));
                stored.put(oracle.getGhost().putDagEntry(entry), entry);
            }
        }

        for (Entry<HashKey, DagEntry> entry : stored.entrySet()) {
            for (Apollo oracle : oracles) {
                DagEntry found = oracle.getGhost().getDagEntry(entry.getKey());
                assertNotNull(found);
                assertArrayEquals(entry.getValue().getData().array(), found.getData().array());
            }
        }
    }

    private List<HASH> randomLinksTo(Map<HashKey, DagEntry> stored) {
        List<HASH> links = new ArrayList<HASH>();
        Set<HashKey> keys = stored.keySet();
        for (int i = 0; i < entropy.nextInt(10); i++) {
            Iterator<HashKey> it = keys.iterator();
            for (int j = 0; j < entropy.nextInt(keys.size()); j++) {
                it.next();
            }
            links.add(it.next().toHash());
        }
        return links;
    }
}
