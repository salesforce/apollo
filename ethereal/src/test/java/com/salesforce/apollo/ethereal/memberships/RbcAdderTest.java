/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.mockito.ArgumentMatchers;

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
import com.salesforce.apollo.ethereal.memberships.ChRbcAdder.ChRbc;
import com.salesforce.apollo.ethereal.memberships.ChRbcAdder.Signed;
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

        var adder = new ChRbcAdder(dag, config, context.toleranceLevel());

        var chRbc = mock(ChRbc.class);

        // PID 0
        var u = units.get((short) 0).get(0).get(0);
        adder.produce(u, chRbc);
        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);
        adder.commit(u.hash(), (short) 1, chRbc);
        adder.commit(u.hash(), (short) 2, chRbc);

        verify(chRbc, times(1)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());
        verify(chRbc, times(1)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());

        // PID 1
        u = units.get((short) 1).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);
        verify(chRbc, times(2)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        verify(chRbc, times(1)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());

        adder.preVote(u.hash(), (short) 1, chRbc);
        verify(chRbc, times(2)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        adder.preVote(u.hash(), (short) 2, chRbc);
        verify(chRbc, times(2)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());

        assertNull(dag.get(u.hash()));

        verify(chRbc, times(2)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());
        adder.commit(u.hash(), (short) 1, chRbc);

        assertNotNull(u.hash());

        verify(chRbc, times(2)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());
        adder.commit(u.hash(), (short) 2, chRbc);

        assertNotNull(dag.get(u.hash()));

        // PID 2
        u = units.get((short) 2).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        verify(chRbc, times(3)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        verify(chRbc, times(2)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 3, chRbc);

        verify(chRbc, times(3)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());
        adder.commit(u.hash(), (short) 1, chRbc);

        assertNull(dag.get(u.hash()));

        verify(chRbc, times(3)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());

        adder.commit(u.hash(), (short) 2, chRbc);
        verify(chRbc, times(3)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());

        assertNotNull(dag.get(u.hash()));

        adder.commit(u.hash(), (short) 3, chRbc);
        verify(chRbc, times(3)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());

        // PID 3
        u = units.get((short) 3).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);

        verify(chRbc, times(4)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        verify(chRbc, times(3)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());

        adder.preVote(u.hash(), (short) 1, chRbc);
        adder.preVote(u.hash(), (short) 2, chRbc);

        verify(chRbc, times(4)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());
        adder.commit(u.hash(), (short) 1, chRbc);

        assertNull(dag.get(u.hash()));
        verify(chRbc, times(4)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());

        assertNull(dag.get(u.hash()));

        adder.commit(u.hash(), (short) 2, chRbc);
        verify(chRbc, times(4)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());

        assertNotNull(dag.get(u.hash()));

        adder.commit(u.hash(), (short) 3, chRbc);
        verify(chRbc, times(4)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());
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

        var adder = new ChRbcAdder(dag, config, context.toleranceLevel());

        var chRbc = mock(ChRbc.class);

        var prime = units.get((short) 0).get(0).get(0);
        var u = prime;
        adder.produce(u, chRbc);
        verify(chRbc, times(1)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        u = units.get((short) 1).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);
        verify(chRbc, times(2)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        u = units.get((short) 2).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);
        verify(chRbc, times(3)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        u = units.get((short) 3).get(0).get(0);
        adder.propose(u.hash(), u.toPreUnit_s(), chRbc);
        verify(chRbc, times(4)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());

        adder.preVote(prime.hash(), (short) 1, chRbc);
        verify(chRbc, times(1)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());
        adder.preVote(prime.hash(), (short) 2, chRbc);
        adder.commit(prime.hash(), (short) 1, chRbc);
        verify(chRbc, times(1)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());
        adder.commit(prime.hash(), (short) 2, chRbc);
        verify(chRbc, times(1)).commit(ArgumentMatchers.<Signed<SignedCommit>>any());

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

        var adder = new ChRbcAdder(dag, config, context.toleranceLevel());

        var chRbc = mock(ChRbc.class);

        round(0, units, adder, chRbc);
        // dealing units are prevoted and committed
        verify(chRbc, times(4)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        verify(chRbc, times(4)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());

        round(1, units, adder, chRbc);

        // Only the produced unit is prevoted and committed
        verify(chRbc, times(5)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        verify(chRbc, times(5)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());

        round(2, units, adder, chRbc);

        // round 1 units are prevoted and committed, round 2 produced unit is prevoted
        // and committed
        verify(chRbc, times(9)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        verify(chRbc, times(9)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());

        // Dealing units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(units.get(pid).get(0).get(0).hash()));
        }

        // Round 1 units output to DAG
        for (short pid = 0; pid < members.size(); pid++) {
            assertNotNull(dag.contains(units.get(pid).get(1).get(0).hash()));
        }

        // Round 2 produced unit is in DAG
        assertNotNull(dag.contains(units.get((short) 0).get(2).get(0).hash()));
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

        var adder = new ChRbcAdder(dag, config, context.toleranceLevel());

        var chRbc = mock(ChRbc.class);

        round(0, units, adder, chRbc);
        round(1, units, adder, chRbc);

        // Only the produced unit is prevoted and committed
        verify(chRbc, times(5)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        verify(chRbc, times(5)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());

        round(2, units, adder, chRbc);

        // round 1 units are prevoted and committed, round 2 produced unit is prevoted
        // and committed
        verify(chRbc, times(9)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        verify(chRbc, times(9)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());

        round(3, units, adder, chRbc);

        // round 2 units are prevoted and committed, round 3 produced unit is prevoted
        // and committed
        verify(chRbc, times(13)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        verify(chRbc, times(13)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());

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

        // Round 3 produced unit is in DAG
        assertNotNull(dag.contains(units.get((short) 0).get(3).get(0).hash()));
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

        var adder = new ChRbcAdder(dag, config, context.toleranceLevel());

        var chRbc = mock(ChRbc.class);

        round(0, units, adder, chRbc);
        round(1, units, adder, chRbc);
        round(2, units, adder, chRbc);
        round(3, units, adder, chRbc);
        round(4, units, adder, chRbc);

        // round 3 units are prevoted and committed, round 4 produced unit is prevoted
        // and committed
        verify(chRbc, times(17)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());
        verify(chRbc, times(17)).prevote(ArgumentMatchers.<Signed<SignedPreVote>>any());

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

        // Round 4 produced unit is in DAG
        assertNotNull(dag.contains(units.get((short) 0).get(4).get(0).hash()));
    }

    private void round(int round, HashMap<Short, Map<Integer, List<Unit>>> units, ChRbcAdder adder, ChRbc chRbc) {
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
