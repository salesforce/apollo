/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag.DagImpl;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class RbcGossiperTest {

    @Test
    public void smokin() {
        String prefix = UUID.randomUUID().toString();
        Executor exec = Executors.newFixedThreadPool(2);
        var context = Context.newBuilder().setCardinality(10).build();
        List<SigningMember> members = IntStream.range(0, 4)
                                               .mapToObj(i -> (SigningMember) new SigningMemberImpl(Utils.getMember(0)))
                                               .toList();
        List<LocalRouter> comms = new ArrayList<>();
        members.forEach(m -> context.activate(m));
        var builder = Config.deterministic()
                            .setnProc((short) members.size())
                            .setVerifiers(members.toArray(new Verifier[members.size()]));
        List<RbcGossiper> gossipers = new ArrayList<>();
        for (short i = 0; i < members.size(); i++) {
            var comm = new LocalRouter(prefix, ServerConnectionCache.newBuilder(), exec, null);
            comms.add(comm);
            gossipers.add(new RbcGossiper(context, members.get(i),
                                          new RbcAdder(new DagImpl(builder.setSigner(members.get(i)).setPid(i).build(),
                                                                   0),
                                                       builder.setSigner(members.get(i)).setPid(i).build(),
                                                       context.toleranceLevel()),
                                          comm, exec, null));
        }
    }
}
