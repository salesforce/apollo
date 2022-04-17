/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.ethereal.proto.SignedCommit;
import com.salesfoce.apollo.ethereal.proto.SignedPreVote;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag.DagImpl;
import com.salesforce.apollo.ethereal.DagFactory;
import com.salesforce.apollo.ethereal.DagFactory.DagAdder;
import com.salesforce.apollo.ethereal.DagReader;
import com.salesforce.apollo.ethereal.DagTest;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.memberships.RbcAdder.ChRbc;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class RbcAdderTest {

    @Test
    public void dealingAllPids() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = DagTest.collectUnits(d.dag());

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

        var adder = new RbcAdder(dag, config, context.toleranceLevel());

        var chRbc = mock(ChRbc.class);

        // PID 0
        var u = units.get((short) 0).get(0).get(0);
        adder.produce(u, chRbc);
        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);
        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);

        verify(chRbc, times(1)).commit(isA(SignedCommit.class));
        verify(chRbc, times(1)).prevote(isA(SignedPreVote.class));

        // PID 1
        u = units.get((short) 1).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);
        verify(chRbc, times(2)).prevote(isA(SignedPreVote.class));
        verify(chRbc, times(1)).commit(isA(SignedCommit.class));

        adder.preVote(u.hash(), (short) 1, chRbc);
        verify(chRbc, times(2)).prevote(isA(SignedPreVote.class));
        adder.preVote(u.hash(), (short) 2, chRbc);
        verify(chRbc, times(2)).prevote(isA(SignedPreVote.class));

        assertNull(dag.get(u.hash()));

        verify(chRbc, times(2)).commit(isA(SignedCommit.class));
        adder.commit(u.hash(), (short) 1, chRbc);

        assertNotNull(u.hash());

        verify(chRbc, times(2)).commit(isA(SignedCommit.class));
        adder.commit(u.hash(), (short) 2, chRbc);

        assertNotNull(dag.get(u.hash()));

        // PID 2
        u = units.get((short) 2).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        verify(chRbc, times(3)).prevote(isA(SignedPreVote.class));
        verify(chRbc, times(2)).commit(isA(SignedCommit.class));

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);

        verify(chRbc, times(3)).commit(isA(SignedCommit.class));
        adder.commit(u.hash(), (short) 1, chRbc);

        assertNull(dag.get(u.hash()));

        verify(chRbc, times(3)).commit(isA(SignedCommit.class));

        adder.commit(u.hash(), (short) 2, chRbc);
        verify(chRbc, times(3)).commit(isA(SignedCommit.class));

        assertNotNull(dag.get(u.hash()));

        adder.commit(u.hash(), (short) 3, chRbc);
        verify(chRbc, times(3)).commit(isA(SignedCommit.class));

        // PID 3
        u = units.get((short) 3).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        verify(chRbc, times(4)).prevote(isA(SignedPreVote.class));
        verify(chRbc, times(3)).commit(isA(SignedCommit.class));

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);

        verify(chRbc, times(4)).commit(isA(SignedCommit.class));
        adder.commit(u.hash(), (short) 1, chRbc);

        assertNull(dag.get(u.hash()));
        verify(chRbc, times(4)).commit(isA(SignedCommit.class));

        assertNull(dag.get(u.hash()));

        adder.commit(u.hash(), (short) 2, chRbc);
        verify(chRbc, times(4)).commit(isA(SignedCommit.class));

        assertNotNull(dag.get(u.hash()));

        adder.commit(u.hash(), (short) 3, chRbc);
        verify(chRbc, times(4)).commit(isA(SignedCommit.class));
    }

    @Test
    public void dealingPid0() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = DagTest.collectUnits(d.dag());

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

        var adder = new RbcAdder(dag, config, context.toleranceLevel());

        var chRbc = mock(ChRbc.class);

        var prime = units.get((short) 0).get(0).get(0);
        var u = prime;
        adder.produce(u, chRbc);
        verify(chRbc, times(1)).prevote(isA(SignedPreVote.class));
        u = units.get((short) 1).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);
        verify(chRbc, times(2)).prevote(isA(SignedPreVote.class));
        u = units.get((short) 2).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);
        verify(chRbc, times(3)).prevote(isA(SignedPreVote.class));
        u = units.get((short) 3).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);
        verify(chRbc, times(4)).prevote(isA(SignedPreVote.class));

        adder.preVote(prime.hash(), (short) 1, chRbc);
        verify(chRbc, times(1)).commit(isA(SignedCommit.class));
        adder.preVote(prime.hash(), (short) 2, chRbc);
        adder.commit(prime.hash(), (short) 1, chRbc);
        verify(chRbc, times(1)).commit(isA(SignedCommit.class));
        adder.commit(prime.hash(), (short) 2, chRbc);
        verify(chRbc, times(1)).commit(isA(SignedCommit.class));

        assertNotNull(dag.get(prime.hash()));
    }

    @Test
    public void round1() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = DagTest.collectUnits(d.dag());

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

        var adder = new RbcAdder(dag, config, context.toleranceLevel());

        var chRbc = mock(ChRbc.class);

        round(0, units, adder, chRbc);
        verify(chRbc, times(4)).prevote(isA(SignedPreVote.class));
        verify(chRbc, times(4)).prevote(isA(SignedPreVote.class));

        var u = units.get((short) 0).get(1).get(0);

        adder.produce(u, chRbc);
        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);

        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);

        verify(chRbc, times(5)).commit(isA(SignedCommit.class));
        verify(chRbc, times(5)).prevote(isA(SignedPreVote.class));

        assertNotNull(dag.get(u.hash()));

        u = units.get((short) 1).get(1).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);

        adder.commit(u.hash(), (short) 1, chRbc);
        assertNull(dag.get(u.hash()));

        adder.commit(u.hash(), (short) 2, chRbc);

        verify(chRbc, times(6)).commit(isA(SignedCommit.class));
        verify(chRbc, times(6)).prevote(isA(SignedPreVote.class));
        assertNotNull(dag.get(u.hash()));
        assertNotNull(dag.get(u.hash()));

        u = units.get((short) 2).get(1).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);

        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);
        adder.commit(u.hash(), (short) 3, chRbc);

        verify(chRbc, times(7)).commit(isA(SignedCommit.class));
        verify(chRbc, times(7)).prevote(isA(SignedPreVote.class));
        assertNotNull(dag.get(u.hash()));
        assertNotNull(dag.get(u.hash()));

        u = units.get((short) 3).get(1).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);

        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);
        adder.commit(u.hash(), (short) 3, chRbc);

        verify(chRbc, times(8)).commit(isA(SignedCommit.class));
        verify(chRbc, times(8)).prevote(isA(SignedPreVote.class));
        assertNotNull(dag.get(u.hash()));
    }

    @Test
    public void round2() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = DagTest.collectUnits(d.dag());

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

        var adder = new RbcAdder(dag, config, context.toleranceLevel());

        var chRbc = mock(ChRbc.class);

        round(0, units, adder, chRbc);
        round(1, units, adder, chRbc);

        var u = units.get((short) 0).get(2).get(0);

        adder.produce(u, chRbc);
        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);
        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);

        u = units.get((short) 1).get(2).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);

        adder.commit(u.hash(), (short) 1, chRbc);
        assertNull(dag.get(u.hash()));
        adder.commit(u.hash(), (short) 2, chRbc);

        verify(chRbc, times(10)).commit(isA(SignedCommit.class));
        verify(chRbc, times(10)).prevote(isA(SignedPreVote.class));
        assertNotNull(dag.get(u.hash()));

        u = units.get((short) 2).get(2).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);

        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);
        adder.commit(u.hash(), (short) 3, chRbc);

        verify(chRbc, times(11)).commit(isA(SignedCommit.class));
        verify(chRbc, times(11)).prevote(isA(SignedPreVote.class));
        assertNotNull(dag.get(u.hash()));

        u = units.get((short) 3).get(2).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);

        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);
        adder.commit(u.hash(), (short) 3, chRbc);

        verify(chRbc, times(12)).commit(isA(SignedCommit.class));
        verify(chRbc, times(12)).prevote(isA(SignedPreVote.class));
        assertNotNull(dag.get(u.hash()));
    }

    @Test
    public void round3() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = DagTest.collectUnits(d.dag());

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

        var adder = new RbcAdder(dag, config, context.toleranceLevel());

        var chRbc = mock(ChRbc.class);

        round(0, units, adder, chRbc);
        round(1, units, adder, chRbc);
        round(2, units, adder, chRbc);

        var u = units.get((short) 0).get(3).get(0);

        adder.produce(u, chRbc);
        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);
        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);

        u = units.get((short) 1).get(3).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);

        adder.commit(u.hash(), (short) 1, chRbc);
        assertNull(dag.get(u.hash()));
        adder.commit(u.hash(), (short) 2, chRbc);

        verify(chRbc, times(14)).commit(isA(SignedCommit.class));
        verify(chRbc, times(14)).prevote(isA(SignedPreVote.class));
        assertNotNull(dag.get(u.hash()));

        u = units.get((short) 2).get(3).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);

        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);
        adder.commit(u.hash(), (short) 3, chRbc);

        verify(chRbc, times(15)).commit(isA(SignedCommit.class));
        verify(chRbc, times(15)).prevote(isA(SignedPreVote.class));
        assertNotNull(dag.get(u.hash()));

        u = units.get((short) 3).get(3).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);

        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);
        adder.commit(u.hash(), (short) 3, chRbc);

        verify(chRbc, times(16)).commit(isA(SignedCommit.class));
        verify(chRbc, times(16)).prevote(isA(SignedPreVote.class));
        assertNotNull(dag.get(u.hash()));
    }

    private void round(int round, HashMap<Short, Map<Integer, List<Unit>>> units, RbcAdder adder, ChRbc chRbc) {
        var u = units.get((short) 0).get(round).get(0);
        adder.produce(u, chRbc);
        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);
        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);

        u = units.get((short) 1).get(round).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);
        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);
        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);

        u = units.get((short) 2).get(round).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);
        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);
        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);
        adder.commit(u.hash(), (short) 3, chRbc);

        u = units.get((short) 3).get(round).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);
        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);
        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);
        adder.commit(u.hash(), (short) 3, chRbc);
    }
}
