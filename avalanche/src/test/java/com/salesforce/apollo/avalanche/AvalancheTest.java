/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.fireflies.PregenPopulation.getCa;
import static com.salesforce.apollo.fireflies.PregenPopulation.getMember;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.salesforce.apollo.avalanche.communications.AvalancheCommunications;
import com.salesforce.apollo.avalanche.communications.AvalancheLocalCommSim;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.communications.FfLocalCommSim;
import com.salesforce.apollo.fireflies.communications.FirefliesCommunications;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hal.hildebrand
 * @since 222
 */
public class AvalancheTest {

    @Test
    public void smoke() throws Exception {

        List<X509Certificate> seeds = Arrays.asList(getMember(1).getCertificate(),
                                                    getMember(2).getCertificate());
        FirefliesParameters ffParameters = new FirefliesParameters(getCa().getX509Certificate());
        FirefliesCommunications ffCommunications = new FfLocalCommSim();

        AvalancheCommunications communications = new AvalancheLocalCommSim();

        AvalancheParameters p1 = new AvalancheParameters();
        p1.alpha = 0.45;
        p1.k = 2;
        p1.beta1 = 5;
        p1.beta2 = 7;
        p1.dbConnect = "jdbc:h2:mem:dagCache-1";
        p1.epsilon = 2;
        Node node1 = new Node(getMember(1), ffParameters);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        View view1 = new View(node1, ffCommunications, seeds, scheduler);
        Avalanche avalanche1 = new Avalanche(view1, communications, p1);

        AvalancheParameters p2 = new AvalancheParameters();
        p2.alpha = 0.45;
        p2.k = 2;
        p2.beta1 = 5;
        p2.beta2 = 7;
        p2.dbConnect = "jdbc:h2:mem:dagCache-2";
        p2.epsilon = 2;
        Node node2 = new Node(getMember(2), ffParameters);
        View view2 = new View(node2, ffCommunications, seeds, scheduler);
        Avalanche avalanche2 = new Avalanche(view2, communications, p2);

        Duration period = Duration.ofMillis(100);
        view1.getService().start(period);
        view2.getService().start(period);
        avalanche1.start();
        avalanche2.start();

        Duration timeout = Duration.ofMillis(30_000);

        CompletableFuture<HashKey> genesis = avalanche1.createGenesis("Genesis".getBytes(), timeout, scheduler);
        HashKey genesisKey = genesis.get(30, TimeUnit.SECONDS);

        assertNotNull(genesisKey);
        HASH k = genesisKey.toHash();
        assertTrue("Failed to finalize genesis on: " + avalanche2.getNode().getId(),
                   Utils.waitForCondition(10_000, () -> avalanche2.getDagDao().isFinalized(k)));

        CompletableFuture<HashKey> fs1 = avalanche1.submitTransaction(WellKnownDescriptions.BYTE_CONTENT.toHash(),
                                                                      "Frist!".getBytes(), timeout, scheduler);
        CompletableFuture<HashKey> fs2 = avalanche2.submitTransaction(WellKnownDescriptions.BYTE_CONTENT.toHash(),
                                                                      "Second".getBytes(), timeout, scheduler);
        assertNotNull(fs1);
        assertNotNull(fs2);

        HashKey result1 = fs1.get(30, TimeUnit.SECONDS);
        assertNotNull(result1);
        HashKey result2 = fs2.get(30, TimeUnit.SECONDS);
        assertNotNull(result2);
        System.out.println("Rounds: " + avalanche1.getRoundCounter());
        // Graphviz.fromGraph(DagViz.visualize("smoke", avalanche1.getDslContext(), false))
        // .render(Format.PNG)
        // .toFile(new File("smoke.png"));
    }
}
