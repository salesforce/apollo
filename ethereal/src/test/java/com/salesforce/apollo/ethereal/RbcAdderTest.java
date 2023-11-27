/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.ethereal.Adder.State;
import com.salesforce.apollo.ethereal.Dag.DagImpl;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author hal.hildebrand
 */
public class RbcAdderTest {

    private Config config;
    private List<SigningMember> members;
    private HashMap<Short, Map<Integer, List<Unit>>> units;

    @BeforeEach
    public void before() throws Exception {
        Dag d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        units = DagTest.collectUnits(d);
        var context = Context.newBuilder().setCardinality(10).build();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[]{6, 6, 6});
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        members = IntStream.range(0, 4).mapToObj(i -> stereotomy.newIdentifier()).map(cpk -> new ControlledIdentifierMember(cpk)).map(e -> (SigningMember) e).toList();
        members.forEach(m -> context.activate(m));
        config = Config.newBuilder()
                .setnProc((short) members.size())
                .setVerifiers(members.toArray(new Verifier[members.size()]))
                .setSigner(members.get(0))
                .setPid((short) 0)
                .build();
    }

    @Test
    public void dealingAllPids() throws Exception {
        final var dag = new DagImpl(config, 0);

        var adder = new Adder(0, dag, 1024 * 1024, config, new ConcurrentSkipListSet<>());

        // PID 0
        var u = unit(0, 0);
        adder.produce(u);
        adder.prevote(u.hash(), (short) 1);
        adder.prevote(u.hash(), (short) 2);
        adder.prevote(u.hash(), (short) 3);
        adder.commit(u.hash(), (short) 1);
        adder.commit(u.hash(), (short) 2);

        assertEquals(0, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        // PID 1
        u = unit(1, 0);
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
        u = unit(2, 0);
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
        u = unit(3, 0);
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
        final var dag = new DagImpl(config, 0);

        var adder = new Adder(0, dag, 1024 * 1024, config, new ConcurrentSkipListSet<>());

        var prime = unit(0, 0);
        var u = prime;
        adder.produce(u);

        assertEquals(0, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        u = unit(1, 0);
        adder.propose(u.hash(), u.toPreUnit_s());

        assertEquals(1, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());
        assertEquals(1, adder.getPrevotes().get(u.hash()).size());

        u = unit(2, 0);
        adder.propose(u.hash(), u.toPreUnit_s());

        assertEquals(2, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());
        assertEquals(1, adder.getPrevotes().get(u.hash()).size());

        u = unit(3, 0);
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
    public void round3() throws Exception {
        final var dag = new DagImpl(config, 0);
        var adder = new Adder(0, dag, 1024 * 1024, config, new ConcurrentSkipListSet<>());

        round(0, adder);
        round(1, adder);
        round(2, adder);
        round(3, adder);

        assertEquals(0, adder.getPrevotes().size());
        assertEquals(0, adder.getCommits().size());

        // Dealing units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(unit(pid, 0).hash()));
        }

        // Round 1 units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(unit(pid, 1).hash()));
        }

        // Round 2 units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(unit(pid, 2).hash()));
        }

        // Round 3 units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(unit(pid, 3).hash()));
        }
    }

    @Test
    public void waitingForParents() {
        final var dag = new DagImpl(config, 0);
        var adder = new Adder(0, dag, 1024 * 1024, config, new ConcurrentSkipListSet<>());
        round(0, adder);

        // Units from 0, 2, 3 at level 1 proposed. Unit 1 from 0 is added to the DAG, as
        // produced

        adder.produce(unit(0, 1));

        var u = unit(2, 1);
        adder.propose(u.hash(), u.toPreUnit_s());

        u = unit(3, 1);
        adder.propose(u.hash(), u.toPreUnit_s());

        assertEquals(0, adder.getWaitingForRound().size());
        assertEquals(2, adder.getWaiting().size());

        var waiting = adder.getWaiting().get(unit(2, 1).hash());
        assertEquals(State.PREVOTED, waiting.state());

        waiting = adder.getWaiting().get(unit(3, 1).hash());
        assertEquals(State.PREVOTED, waiting.state());

        // Add units from level 2 from all PIDS

        adder.produce(unit(0, 2));

        u = unit(1, 2);
        adder.propose(u.hash(), u.toPreUnit_s());

        u = unit(2, 2);
        adder.propose(u.hash(), u.toPreUnit_s());

        u = unit(3, 2);
        adder.propose(u.hash(), u.toPreUnit_s());

        assertEquals(0, adder.getWaitingForRound().size());
        assertEquals(6, adder.getWaiting().size());

        waiting = adder.getWaiting().get(unit(1, 2).hash());
        assertEquals(State.PREVOTED, waiting.state());

        waiting = adder.getWaiting().get(unit(2, 2).hash());
        assertEquals(State.PREVOTED, waiting.state());

        waiting = adder.getWaiting().get(unit(3, 2).hash());
        assertEquals(State.PREVOTED, waiting.state());

        adder.prevote(unit(2, 2).hash(), (short) 1);
        adder.prevote(unit(2, 2).hash(), (short) 2);

        waiting = adder.getWaiting().get(unit(2, 2).hash());
        assertEquals(State.WAITING_FOR_PARENTS, waiting.state());

        adder.prevote(unit(3, 2).hash(), (short) 1);
        adder.prevote(unit(3, 2).hash(), (short) 2);

        waiting = adder.getWaiting().get(unit(3, 2).hash());
        assertEquals(State.WAITING_FOR_PARENTS, waiting.state());

        adder.prevote(unit(2, 1).hash(), (short) 1);
        adder.prevote(unit(2, 1).hash(), (short) 2);

        waiting = adder.getWaiting().get(unit(2, 1).hash());
        assertEquals(State.COMMITTED, waiting.state());

        waiting = adder.getWaiting().get(unit(1, 2).hash());
        assertEquals(State.PREVOTED, waiting.state());

        waiting = adder.getWaiting().get(unit(2, 2).hash());
        assertEquals(State.WAITING_FOR_PARENTS, waiting.state());

        waiting = adder.getWaiting().get(unit(3, 2).hash());
        assertEquals(State.WAITING_FOR_PARENTS, waiting.state());
    }

    // All PIDs should be output
    private void round(int round, Adder adder) {
        var u = unit(0, round);
        adder.produce(u);

        adder.prevote(u.hash(), (short) 1);
        adder.prevote(u.hash(), (short) 2);
        adder.commit(u.hash(), (short) 1);
        adder.commit(u.hash(), (short) 2);

        u = unit(1, round);
        adder.propose(u.hash(), u.toPreUnit_s());

        adder.prevote(u.hash(), (short) 1);
        adder.prevote(u.hash(), (short) 2);
        adder.commit(u.hash(), (short) 1);
        adder.commit(u.hash(), (short) 2);

        u = unit(2, round);
        adder.propose(u.hash(), u.toPreUnit_s());
        adder.prevote(u.hash(), (short) 1);
        adder.prevote(u.hash(), (short) 3);
        adder.commit(u.hash(), (short) 1);
        adder.commit(u.hash(), (short) 2);
        adder.commit(u.hash(), (short) 3);

        u = unit(3, round);
        adder.propose(u.hash(), u.toPreUnit_s());
        adder.prevote(u.hash(), (short) 1);
        adder.prevote(u.hash(), (short) 2);
        adder.commit(u.hash(), (short) 1);
        adder.commit(u.hash(), (short) 2);
        adder.commit(u.hash(), (short) 3);
    }

    private Unit unit(int pid, int level) {
        return units.get((short) pid).get(level).get(0);
    }
}
