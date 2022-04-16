/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

import com.salesfoce.apollo.ethereal.proto.SignedCommit;
import com.salesfoce.apollo.ethereal.proto.SignedPreVote;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DagFactory;
import com.salesforce.apollo.ethereal.DagFactory.DagAdder;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.ethereal.DagReader;
import com.salesforce.apollo.ethereal.DagTest;
import com.salesforce.apollo.ethereal.memberships.RbcAdder.ChRbc;
import com.salesforce.apollo.ethereal.Dag.DagImpl;

/**
 * @author hal.hildebrand
 *
 */
public class RbcAdderTest {

    @Test
    public void dealing() throws Exception {
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
        verify(chRbc, times(0)).commit(isA(SignedCommit.class));
        adder.preVote(prime.hash(), (short) 2, chRbc);
        verify(chRbc, times(1)).commit(isA(SignedCommit.class));

        assertNull(dag.get(prime.hash()));

        adder.commit(prime.hash(), (short) 1, chRbc);
        assertNull(dag.get(prime.hash()));
        verify(chRbc, times(1)).commit(isA(SignedCommit.class));
        adder.commit(prime.hash(), (short) 2, chRbc);
        verify(chRbc, times(1)).commit(isA(SignedCommit.class));

        assertNotNull(dag.get(prime.hash()));
    }
}
