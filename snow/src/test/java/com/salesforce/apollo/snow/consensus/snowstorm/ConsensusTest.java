/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowstorm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.snow.Context;
import com.salesforce.apollo.snow.EventDispatcher;
import com.salesforce.apollo.snow.choices.Status;
import com.salesforce.apollo.snow.consensus.snowball.Parameters;
import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
abstract public class ConsensusTest {

    private static class singleAcceptTx implements Tx {
        private boolean accepted = false;
        private Tx      tx;

        public singleAcceptTx(Tx tx) {
            this.tx = tx;
        }

        @Override
        public void accept() {
            if (accepted) {
                fail("already accepted");
            }
            accepted = true;
            tx.accept();
        }

        public byte[] bytes() {
            return tx.bytes();
        }

        public Collection<Tx> dependencies() {
            return tx.dependencies();
        }

        public ID id() {
            return tx.id();
        }

        public Collection<ID> inputIDs() {
            return tx.inputIDs();
        }

        public void reject() {
            tx.reject();
        }

        public Status status() {
            return tx.status();
        }

    }

    private static final Logger log = LoggerFactory.getLogger(ConsensusTest.class);

    private TestTransaction Red, Green, Blue, Alpha;

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

    @BeforeEach
    public void setup() {
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

    @Test
    public void acceptingDependency() {
        Parameters params = Parameters.newBuilder()
                                      .setMetrics(new MetricRegistry())
                                      .setK(1)
                                      .setAlpha(1)
                                      .setBetaVirtuous(1)
                                      .setBetaRogue(2)
                                      .setConcurrentRepolls(1)
                                      .build();

        Consensus graph = createConsensus(defaultTestContext(), params);

        TestTransaction purple = new TestTransaction();
        purple.idV = ID.ORIGIN.prefix(7);
        purple.statusV = Status.PROCESSING;
        purple.dependenciesV.add(Red);
        purple.inputIDsV.add(ID.ORIGIN.prefix(8));

        graph.add(Red);
        graph.add(Green);
        graph.add(purple);

        assertEquals(2, graph.preferences().size());
        assertTrue(graph.preferences().contains(Red.id()));
        assertTrue(graph.preferences().contains(purple.id()));
        assertEquals(Status.PROCESSING, Red.status());
        assertEquals(Status.PROCESSING, Green.status());
        assertEquals(Status.PROCESSING, purple.status());

        Bag g = new Bag();
        g.add(Green.id());

        assertTrue(graph.recordPoll(g));
        assertEquals(2, graph.preferences().size());
        assertTrue(graph.preferences().contains(Green.id()));
        assertTrue(graph.preferences().contains(purple.id()));
        assertEquals(Status.PROCESSING, Red.status());
        assertEquals(Status.PROCESSING, Green.status());
        assertEquals(Status.PROCESSING, purple.status());

        Bag rp = new Bag();
        rp.add(Red.id(), purple.id());

        assertFalse(graph.recordPoll(rp));
        assertEquals(2, graph.preferences().size());
        assertTrue(graph.preferences().contains(Green.id()));
        assertTrue(graph.preferences().contains(purple.id()));
        assertEquals(Status.PROCESSING, Red.status());
        assertEquals(Status.PROCESSING, Green.status());
        assertEquals(Status.PROCESSING, purple.status());

        Bag r = new Bag();
        r.add(Red.id());
        assertTrue(graph.recordPoll(r));
        assertEquals(0, graph.preferences().size());
        assertEquals(Status.ACCEPTED, Red.status());
        assertEquals(Status.REJECTED, Green.status());
        assertEquals(Status.ACCEPTED, purple.status());
    }

    @Test
    public void acceptingSlowDependency() {
        Parameters params = Parameters.newBuilder()
                                      .setMetrics(new MetricRegistry())
                                      .setK(1)
                                      .setAlpha(1)
                                      .setBetaVirtuous(1)
                                      .setBetaRogue(2)
                                      .setConcurrentRepolls(1)
                                      .build();

        Consensus graph = createConsensus(defaultTestContext(), params);

        TestTransaction rawPurple = new TestTransaction();
        rawPurple.idV = ID.ORIGIN.prefix(7);
        rawPurple.statusV = Status.PROCESSING;
        rawPurple.dependenciesV.add(Red);
        rawPurple.inputIDsV.add(ID.ORIGIN.prefix(8));

        singleAcceptTx purple = new singleAcceptTx(rawPurple);

        graph.add(Red);
        graph.add(Green);
        graph.add(purple);

        assertEquals(2, graph.preferences().size());
        assertTrue(graph.preferences().contains(Red.id()));
        assertTrue(graph.preferences().contains(purple.id()));
        assertEquals(Status.PROCESSING, Red.status());
        assertEquals(Status.PROCESSING, Green.status());
        assertEquals(Status.PROCESSING, purple.status());

        Bag g = new Bag();
        g.add(Green.id());

        assertTrue(graph.recordPoll(g));
        assertEquals(2, graph.preferences().size());
        assertTrue(graph.preferences().contains(Green.id()));
        assertTrue(graph.preferences().contains(purple.id()));
        assertEquals(Status.PROCESSING, Red.status());
        assertEquals(Status.PROCESSING, Green.status());
        assertEquals(Status.PROCESSING, purple.status());

        Bag p = new Bag();
        p.add(purple.id());
        assertFalse(graph.recordPoll(p));
        assertEquals(2, graph.preferences().size());
        assertTrue(graph.preferences().contains(Green.id()));
        assertTrue(graph.preferences().contains(purple.id()));
        assertEquals(Status.PROCESSING, Red.status());
        assertEquals(Status.PROCESSING, Green.status());
        assertEquals(Status.PROCESSING, purple.status());

        Bag rp = new Bag();
        rp.add(Red.id(), purple.id());
        assertFalse(graph.recordPoll(rp));
        assertEquals(2, graph.preferences().size());
        assertTrue(graph.preferences().contains(Green.id()));
        assertTrue(graph.preferences().contains(purple.id()));
        assertEquals(Status.PROCESSING, Red.status());
        assertEquals(Status.PROCESSING, Green.status());
        assertEquals(Status.PROCESSING, purple.status());

        Bag r = new Bag();
        r.add(Red.id());
        assertTrue(graph.recordPoll(r));
        assertEquals(0, graph.preferences().size());
        assertEquals(Status.ACCEPTED, Red.status());
        assertEquals(Status.REJECTED, Green.status());
        assertEquals(Status.ACCEPTED, purple.status());
    }

    @Test
    public void conflicts() {
        Parameters params = Parameters.newBuilder()
                                      .setMetrics(new MetricRegistry())
                                      .setK(1)
                                      .setAlpha(1)
                                      .setBetaVirtuous(1)
                                      .setBetaRogue(2)
                                      .setConcurrentRepolls(1)
                                      .build();

        Consensus graph = createConsensus(defaultTestContext(), params);

        ID conflictInputID = ID.ORIGIN.prefix(0);

        Set<ID> insPurple = new HashSet<>();
        insPurple.add(conflictInputID);
        TestTransaction purple = new TestTransaction();
        purple.idV = ID.ORIGIN.prefix(6);
        purple.statusV = Status.PROCESSING;
        purple.inputIDsV = insPurple;

        Set<ID> insOrange = new HashSet<>();
        insOrange.add(conflictInputID);
        TestTransaction orange = new TestTransaction();
        orange.idV = ID.ORIGIN.prefix(7);
        orange.statusV = Status.PROCESSING;
        orange.inputIDsV = insOrange;

        graph.add(purple);

        assertEquals(1, graph.conflicts(orange).size());
        assertTrue(graph.conflicts(orange).contains(purple.id()));

        graph.add(orange);

        assertEquals(1, graph.conflicts(orange).size());
        assertTrue(graph.conflicts(orange).contains(purple.id()));

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
        assertTrue(graph.quiesce());
    }
}
