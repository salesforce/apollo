/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class ContextTests {

    @Test
    public void consistency() {
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 3);
        List<SigningMember> members = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            SigningMemberImpl m = new SigningMemberImpl(Utils.getMember(i));
            members.add(m);
            context.activate(m);
        }

        List<Member> predecessors = context.predecessors(members.get(0));
        SigningMember expected = members.get(7);
        assertEquals(expected, predecessors.get(0));


        List<Member> successors = context.successors(members.get(1));
        expected = members.get(4);
        assertEquals(expected, successors.get(0));
        assertEquals(expected, context.ring(0).successor(members.get(0)));
    }
}
