/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hal.hildebrand
 * @since 218
 */
@Ignore
public class MtlsTest {
    class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumeInputLine;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumeInputLine) {
            this.inputStream = inputStream;
            this.consumeInputLine = consumeInputLine;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumeInputLine);
        }
    }

    private Process process;

    @After
    public void after() {
        System.out.println("shutting down server");
        if (process != null) {
            process.destroyForcibly();
        }
    }

    @Before
    public void before() throws Exception {
        String java = System.getProperty("java.home") + "/bin/java";
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(java, "--illegal-access=permit", "-jar", "target/apollo-web.jar", "server", "target/test-classes/server.yml");
        process = builder.start();
        StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), string -> {
            try {
                System.out.println(string);
            } catch (Throwable e) {
                // ignore
            }
        });
        StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), string -> {
            try {
                System.out.println(string);
            } catch (Throwable e) {
                // ignore
            }
        });

        Thread thread = new Thread(outputGobbler);
        thread.setDaemon(true);
        thread.start();
        thread = new Thread(errorGobbler);
        thread.setDaemon(true);
        thread.start(); 
    }
    private Random entropy;

    @Test
    public void smoke() throws Exception {
        entropy = new Random(0x666);
        List<Apollo> oracles = new ArrayList<>();

        for (int i = 2; i < PregenPopulation.getCardinality(); i++) {
            ApolloConfiguration config = new ApolloConfiguration();
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
