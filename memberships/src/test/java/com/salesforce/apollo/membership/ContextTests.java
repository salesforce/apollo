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
        Context<Member> context = new ContextImpl<Member>(10, 0.2, 2, DigestAlgorithm.DEFAULT.getOrigin().prefix(1));
        List<SigningMember> members = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            SigningMemberImpl m = new SigningMemberImpl(Utils.getMember(i));
            members.add(m);
            context.activate(m);
        }

        List<Member> predecessors = context.predecessors(members.get(0));
        SigningMember expected = members.get(9);
        assertEquals(expected, predecessors.get(2));


        List<Member> successors = context.successors(members.get(1)); 
        assertEquals(members.get(5), successors.get(0));
        assertEquals(members.get(9), context.ring(1).successor(members.get(0)));
    }
}
