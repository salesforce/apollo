/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.rbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag.DagImpl;
import com.salesforce.apollo.ethereal.DagFactory;
import com.salesforce.apollo.ethereal.DagFactory.DagAdder;
import com.salesforce.apollo.ethereal.DagReader;
import com.salesforce.apollo.ethereal.DagTest;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class RbcAdderTest {

    private HashMap<Short, Map<Integer, List<Unit>>> units;

    @BeforeEach
    public void before() throws IOException, FileNotFoundException {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        units = DagTest.collectUnits(d.dag());
    }

    @Test
    public void dealingAllPids() throws Exception {
        var context = Context.newBuilder().setCardinality(10).build();
        List<SigningMember> members = IntStream.range(0, 4)
                                               .mapToObj(i -> (SigningMember) new SigningMemberImpl(Utils.getMember(0)))
                                               .toList();
        members.forEach(m -> context.activate(m));
        final var config = Config.deterministic()
                                 .setnProc((short) members.size())
                                 .setVerifiers(members.toArray(new Verifier[members.size()]))
                                 .setSigner(members.get(0))
                                 .setPid((short) 0)
                                 .build();
        final var dag = new DagImpl(config, 0);

        var adder = new ChRbcAdder(0, dag, 1024 * 1024, config, new ConcurrentSkipListSet<>());

        // PID 0
        var u = units.get((short) 0).get(0).get(0);
        adder.produce(u);
        adder.prevote(u.hash(), (short) 1);
        adder.prevote(u.hash(), (short) 2);
        adder.prevote(u.hash(), (short) 3);
        adder.commit(u.hash(), (short) 1);
        adder.commit(u.hash(), (short) 2);

        assertEquals(0, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        // PID 1
        u = units.get((short) 1).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s());

        assertEquals(1, adder.getPrevotes().size());
        assertEquals(1, adder.getPrevotes().get(u.hash()).size());

        adder.prevote(u.hash(), (short) 1);

        assertEquals(2, adder.getPrevotes().get(u.hash()).size());
        assertEquals(0, adder.getCommits().size());

        adder.prevote(u.hash(), (short) 2);

        assertEquals(3, adder.getPrevotes().get(u.hash()).size());

        assertNull(dag.get(u.hash()));

        assertEquals(1, adder.getCommits().size());
        adder.commit(u.hash(), (short) 1);
        assertEquals(2, adder.getCommits().get(u.hash()).size());

        assertNotNull(u.hash());

        adder.commit(u.hash(), (short) 2);

        assertEquals(0, adder.getCommits().size());
        assertEquals(0, adder.getPrevotes().size());

        assertNotNull(dag.get(u.hash()));

        // PID 2
        u = units.get((short) 2).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s());

        assertEquals(1, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        assertEquals(1, adder.getPrevotes().get(u.hash()).size());

        adder.prevote(u.hash(), (short) 1);
        adder.prevote(u.hash(), (short) 3);

        assertEquals(3, adder.getPrevotes().get(u.hash()).size());

        adder.commit(u.hash(), (short) 1);

        assertEquals(2, adder.getCommits().get(u.hash()).size());

        assertNull(dag.get(u.hash()));

        adder.commit(u.hash(), (short) 2);

        assertEquals(0, adder.getCommits().size());
        assertEquals(0, adder.getPrevotes().size());

        assertNotNull(dag.get(u.hash()));

        adder.commit(u.hash(), (short) 3);
        assertEquals(0, adder.getCommits().size());

        // PID 3
        u = units.get((short) 3).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s());

        assertEquals(1, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        adder.prevote(u.hash(), (short) 1);

        assertEquals(0, adder.getCommits().size());

        adder.prevote(u.hash(), (short) 2);

        assertEquals(1, adder.getCommits().size());
        assertEquals(3, adder.getPrevotes().get(u.hash()).size());
        assertEquals(1, adder.getCommits().get(u.hash()).size());

        adder.commit(u.hash(), (short) 1);

        assertNull(dag.get(u.hash()));

        assertEquals(1, adder.getCommits().size());

        assertNull(dag.get(u.hash()));

        adder.commit(u.hash(), (short) 2);

        assertEquals(0, adder.getCommits().size());

        assertNotNull(dag.get(u.hash()));

        adder.commit(u.hash(), (short) 3);
        assertEquals(0, adder.getCommits().size());
        assertEquals(0, adder.getPrevotes().size());
    }

    @Test
    public void dealingPid0() throws Exception {
        var context = Context.newBuilder().setCardinality(10).build();
        List<SigningMember> members = IntStream.range(0, 4)
                                               .mapToObj(i -> (SigningMember) new SigningMemberImpl(Utils.getMember(0)))
                                               .toList();
        members.forEach(m -> context.activate(m));
        final var config = Config.deterministic()
                                 .setnProc((short) members.size())
                                 .setVerifiers(members.toArray(new Verifier[members.size()]))
                                 .setSigner(members.get(0))
                                 .setPid((short) 0)
                                 .build();
        final var dag = new DagImpl(config, 0);

        var adder = new ChRbcAdder(0, dag, 1024 * 1024, config, new ConcurrentSkipListSet<>());

        var prime = units.get((short) 0).get(0).get(0);
        var u = prime;
        adder.produce(u);

        assertEquals(0, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        u = units.get((short) 1).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s());

        assertEquals(1, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());
        assertEquals(1, adder.getPrevotes().get(u.hash()).size());

        u = units.get((short) 2).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s());

        assertEquals(2, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());
        assertEquals(1, adder.getPrevotes().get(u.hash()).size());

        u = units.get((short) 3).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s());

        assertEquals(3, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());
        assertEquals(1, adder.getPrevotes().get(u.hash()).size());

        adder.prevote(prime.hash(), (short) 1);

        assertEquals(1, adder.getPrevotes().get(u.hash()).size());

        adder.prevote(prime.hash(), (short) 2);

        assertEquals(1, adder.getPrevotes().get(u.hash()).size());

        adder.commit(prime.hash(), (short) 1);

        assertEquals(0, adder.getCommits().size());

        adder.commit(prime.hash(), (short) 2);

        assertEquals(0, adder.getCommits().size());

        assertNotNull(dag.get(prime.hash()));
    }

    @Test
    public void round1() throws Exception {
        var context = Context.newBuilder().setCardinality(10).build();
        List<SigningMember> members = IntStream.range(0, 4)
                                               .mapToObj(i -> (SigningMember) new SigningMemberImpl(Utils.getMember(0)))
                                               .toList();
        members.forEach(m -> context.activate(m));
        final var config = Config.deterministic()
                                 .setnProc((short) members.size())
                                 .setVerifiers(members.toArray(new Verifier[members.size()]))
                                 .setSigner(members.get(0))
                                 .setPid((short) 0)
                                 .build();
        final var dag = new DagImpl(config, 0);

        var adder = new ChRbcAdder(0, dag, 1024 * 1024, config, new ConcurrentSkipListSet<>());

        round(0, units, adder);

        assertEquals(0, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        round(1, units, adder);

        assertEquals(0, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        // Dealing units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(units.get(pid).get(0).get(0).hash()));
        }

        // Round 1 units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(units.get(pid).get(1).get(0).hash()));
        }
    }

    @Test
    public void round2() throws Exception {
        var context = Context.newBuilder().setCardinality(10).build();
        List<SigningMember> members = IntStream.range(0, 4)
                                               .mapToObj(i -> (SigningMember) new SigningMemberImpl(Utils.getMember(0)))
                                               .toList();
        members.forEach(m -> context.activate(m));
        final var config = Config.deterministic()
                                 .setnProc((short) members.size())
                                 .setVerifiers(members.toArray(new Verifier[members.size()]))
                                 .setSigner(members.get(0))
                                 .setPid((short) 0)
                                 .build();
        final var dag = new DagImpl(config, 0);

        var adder = new ChRbcAdder(0, dag, 1024 * 1024, config, new ConcurrentSkipListSet<>());

        round(0, units, adder);

        assertEquals(0, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        round(1, units, adder);

        assertEquals(0, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        round(1, units, adder);

        assertEquals(0, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        // Dealing units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(units.get(pid).get(0).get(0).hash()));
        }

        // Round 1 units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(units.get(pid).get(1).get(0).hash()));
        }

        // Round 2 units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(units.get(pid).get(2).get(0).hash()));
        }
    }

    @Test
    public void round3() throws Exception {
        var context = Context.newBuilder().setCardinality(10).build();
        List<SigningMember> members = IntStream.range(0, 4)
                                               .mapToObj(i -> (SigningMember) new SigningMemberImpl(Utils.getMember(0)))
                                               .toList();
        members.forEach(m -> context.activate(m));
        final var config = Config.deterministic()
                                 .setnProc((short) members.size())
                                 .setVerifiers(members.toArray(new Verifier[members.size()]))
                                 .setSigner(members.get(0))
                                 .setPid((short) 0)
                                 .build();
        final var dag = new DagImpl(config, 0);

        var adder = new ChRbcAdder(0, dag, 1024 * 1024, config, new ConcurrentSkipListSet<>());

        round(0, units, adder);
        round(1, units, adder);
        round(2, units, adder);

        round(3, units, adder);

        assertEquals(0, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        // Dealing units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(units.get(pid).get(0).get(0).hash()));
        }

        // Round 1 units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(units.get(pid).get(1).get(0).hash()));
        }

        // Round 2 units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(units.get(pid).get(2).get(0).hash()));
        }

        // Round 3 units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(units.get(pid).get(3).get(0).hash()));
        }
    }

    // All PIDs should be output
    private void round(int round, HashMap<Short, Map<Integer, List<Unit>>> units, ChRbcAdder adder) {
        var u = units.get((short) 0).get(round).get(0);
        adder.produce(u);

        adder.prevote(u.hash(), (short) 1);
        adder.prevote(u.hash(), (short) 2);
        adder.commit(u.hash(), (short) 1);
        adder.commit(u.hash(), (short) 2);

        u = units.get((short) 1).get(round).get(0);
        adder.propose(u.hash(), u.toPreUnit_s());

        adder.prevote(u.hash(), (short) 1);
        adder.prevote(u.hash(), (short) 2);
        adder.commit(u.hash(), (short) 1);
        adder.commit(u.hash(), (short) 2);

        u = units.get((short) 2).get(round).get(0);
        adder.propose(u.hash(), u.toPreUnit_s());
        adder.prevote(u.hash(), (short) 1);
        adder.prevote(u.hash(), (short) 3);
        adder.commit(u.hash(), (short) 1);
        adder.commit(u.hash(), (short) 2);
        adder.commit(u.hash(), (short) 3);

        u = units.get((short) 3).get(round).get(0);
        adder.propose(u.hash(), u.toPreUnit_s());
        adder.prevote(u.hash(), (short) 1);
        adder.prevote(u.hash(), (short) 2);
        adder.commit(u.hash(), (short) 1);
        adder.commit(u.hash(), (short) 2);
        adder.commit(u.hash(), (short) 3);
    }
}
