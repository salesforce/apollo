/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowstorm;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.snow.Context;
import com.salesforce.apollo.snow.EventDispatcher;
import com.salesforce.apollo.snow.choices.Status;
import com.salesforce.apollo.snow.consensus.snowball.Parameters;
import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
abstract public class ConsensusTest {

    private static final Logger log = LoggerFactory.getLogger(ConsensusTest.class);

    private static TestTransaction Red, Green, Blue, Alpha;

    public static Context defaultTestContext() {
        return Context.newBuilder()
                      .setNetworkID(0)
                      .setSubnetID(ID.ORIGIN)
                      .setChainID(ID.ORIGIN)
                      .setNodeID(ShortID.ORIGIN)
                      .setLog(log)
                      .setDecisionDispatcher(emptyEventDispatcher())
                      .setConsensusDispatcher(emptyEventDispatcher())
                      .setNamespace("")
                      .setMetrics(new MetricRegistry())
                      .build();
    }

    public static EventDispatcher emptyEventDispatcher() {
        return new EventDispatcher() {

            @Override
            public void accept(Context ctx, ID containerID, byte[] container) {
            }

            @Override
            public void issue(Context ctx, ID containerID, byte[] container) {
            }

            @Override
            public void reject(Context ctx, ID containerID, byte[] container) {
            }
        };
    }

    @BeforeAll
    public static void setup() {
        Red = new TestTransaction();
        Green = new TestTransaction();
        Blue = new TestTransaction();
        Alpha = new TestTransaction();
        TestTransaction[] colors = new TestTransaction[] { Red, Green, Blue, Alpha };
        for (int i = 0; i < colors.length; i++) {
            TestTransaction color = colors[i];
            color.idV = ID.ORIGIN.prefix(i);
            color.acceptV = null;
            color.rejectV = null;
            color.statusV = Status.PROCESSING;

            color.dependenciesV = null;
            color.inputIDsV.clear();
            color.verifyV = null;
            color.bytesV = new byte[] { (byte) i };
        }

        ID X = ID.ORIGIN.prefix(4);
        ID Y = ID.ORIGIN.prefix(5);
        ID Z = ID.ORIGIN.prefix(6);

        Red.inputIDsV.add(X);

        Green.inputIDsV.add(X);
        Green.inputIDsV.add(Y);

        Blue.inputIDsV.add(Y);
        Blue.inputIDsV.add(Z);

        Alpha.inputIDsV.add(Z);
    }

    abstract public Consensus createConsensus(Context ctx, Parameters params);

    @Test
    public void quiesce() {
        Parameters params = Parameters.newBuilder()
                                      .setMetrics(new MetricRegistry())
                                      .setK(2)
                                      .setAlpha(2)
                                      .setBetaVirtuous(1)
                                      .setBetaRogue(1)
                                      .setConcurrentRepolls(1)
                                      .build();

        Consensus graph = createConsensus(defaultTestContext(), params);

        assertTrue(graph.quiesce());
        graph.add(Red);
        assertFalse(graph.quiesce());
        graph.add(Green);
        assertFalse(graph.quiesce());
    }
}
