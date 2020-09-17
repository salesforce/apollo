/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import static com.salesforce.apollo.membership.TestCertUtils.generate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.bouncycastle.util.Arrays;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class RingTest {
    /**
     * 
     */
    private static final int    MEMBER_COUNT = 10;
    private static List<Member> members;
    private static final byte[] PROTO        = new byte[32];

    @BeforeAll
    public static void beforeClass() {
        members = new ArrayList<>();
        for (int i = 0; i < MEMBER_COUNT; i++) {
            Member m = createMember(i);
            members.add(m);
        }
    }

    private static Member createMember(int i) {
        byte[] hash = Arrays.copyOf(PROTO, PROTO.length);
        hash[31] = (byte) i;
        return new Member(new HashKey(hash), generate());
    }

    private Ring<Member>    ring;
    private Context<Member> context;

    @BeforeEach
    public void before() {
        context = new Context<>(new HashKey(new byte[] { 0, 1, 2 }), 1);
        ring = context.rings().findFirst().get();
        members.forEach(m -> context.activate(m));

        Collections.sort(members, new Comparator<Member>() {
            @Override
            public int compare(Member o1, Member o2) {
                return context.hashFor(o1, 0).compareTo(context.hashFor(o2, 0));
            }
        });
    }

    @Test
    public void betweenPredecessor() {
        int start = 5;
        int stop = 3;
        int index = start - 1;

        for (Member test : ring.betweenPredecessors(members.get(start), members.get(stop))) {
            if (index == -1) {
                index = members.size() - 1; // wrap around
            }
            assertEquals(index, members.indexOf(test));
            index--;
        }

        start = 3;
        stop = 5;
        index = start - 1;

        for (Member test : ring.betweenPredecessors(members.get(start), members.get(stop))) {
            if (index == -1) {
                index = members.size() - 1; // wrap around
            }
            assertEquals(index, members.indexOf(test));
            index--;
        }
    }

    @Test
    public void betweenSuccessor() {
        int start = 5;
        int stop = 3;
        int index = start + 1;

        for (Member test : ring.betweenSuccessor(members.get(start), members.get(stop))) {
            if (index == members.size()) {
                index = 0; // wrap around
            }
            assertEquals(members.get(index), test, "error at index: " + index);
            index++;
        }
    }

    @Test
    public void predecessors() {
        Collection<Member> predecessors = ring.streamPredecessors(members.get(5),
                                                                  m -> m.equals(members.get(members.size() - 3)))
                                              .collect(Collectors.toList());
        assertFalse(predecessors.isEmpty());
        assertEquals(7, predecessors.size());
    }

    @Test
    public void successors() {
        Collection<Member> successors = ring.streamSuccessors(members.get(5), m -> m.equals(members.get(3)))
                                            .collect(Collectors.toList());
        assertFalse(successors.isEmpty());
        assertEquals(7, successors.size());
    }

    @Test
    public void predecessor() {
        assertEquals(5, members.indexOf(ring.predecessor(members.get(6))));
        assertEquals(members.size() - 1, members.indexOf(ring.predecessor(members.get(0))));
    }

    @Test
    public void rank() {
        assertEquals(members.size() - 2, ring.rank(members.get(0), members.get(members.size() - 1)));

        assertEquals(members.size() - 2, ring.rank(members.get(members.size() - 1), members.get(members.size() - 2)));

        assertEquals(members.size() - 2, ring.rank(members.get(1), members.get(0)));

        assertEquals(2, ring.rank(members.get(0), members.get(3)));

        assertEquals(6, ring.rank(members.get(0), members.get(7)));
    }

    @Test
    public void successor() {
        assertEquals(5, members.indexOf(ring.successor(members.get(4))));
        assertEquals(0, members.indexOf(ring.successor(members.get(members.size() - 1))));

        for (int i = 0; i < members.size(); i++) {
            int successor = (i + 1) % members.size();
            assertEquals(successor, members.indexOf(ring.successor(members.get(i))));
        }
    }

    @Test
    public void noRing() {
        context = new Context<>(new HashKey(new byte[] { 0, 1, 2 }));
        assertEquals(0, context.getRings().length);
        members.forEach(m -> context.activate(m));
        assertEquals(MEMBER_COUNT, context.getActive().size());
    }

    @Test
    public void theRing() {

        assertEquals(members.size(), ring.size());

        for (int start = 0; start < members.size(); start++) {
            int index = start + 1;
            for (Member member : ring.traverse(members.get(start))) {
                if (index == members.size()) {
                    index = 0; // wrap around
                }
                assertEquals(members.get(index), member);
                index++;
            }
        }
    }
}
