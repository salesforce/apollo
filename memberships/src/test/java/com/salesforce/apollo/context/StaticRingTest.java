/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.context;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.cryptography.cert.Certificates;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.MemberImpl;
import com.salesforce.apollo.utils.Utils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class StaticRingTest {

    private static final int    MEMBER_COUNT = 10;
    private static final byte[] PROTO        = new byte[32];

    private static List<Member>    members;
    private        Context<Member> context;

    @BeforeAll
    public static void beforeClass() {
        members = new ArrayList<>();
        for (int i = 1; i <= MEMBER_COUNT; i++) {
            Member m = createMember(i);
            members.add(m);
        }
        members.sort(Comparator.naturalOrder());
    }

    private static Member createMember(int i) {
        byte[] hash = PROTO.clone();
        hash[0] = (byte) i;
        KeyPair keyPair = SignatureAlgorithm.ED_25519.generateKeyPair();
        var notBefore = Instant.now();
        var notAfter = Instant.now().plusSeconds(10_000);
        Digest id = new Digest(DigestAlgorithm.DEFAULT, hash);
        X509Certificate generated = Certificates.selfSign(false, Utils.encode(id, "foo.com", i, keyPair.getPublic()),
                                                          keyPair, notBefore, notAfter, Collections.emptyList());
        return new MemberImpl(id, generated, generated.getPublicKey());
    }

    @BeforeEach
    public void before() {
        Random entropy = new Random(0x1638);
        byte[] id = new byte[32];
        entropy.nextBytes(id);
        var initial = new DynamicContextImpl<Member>(new Digest(DigestAlgorithm.DEFAULT, id), members.size(), 0.2, 2);
        members.forEach(m -> initial.activate(m));
        context = new StaticContext<>(initial);

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

        for (Member test : context.betweenPredecessors(0, members.get(start), members.get(stop))) {
            if (index == -1) {
                index = members.size() - 1; // wrap around
            }
            assertEquals(test, members.get(index), "index: " + index + " not equals");
            index--;
        }

        start = 3;
        stop = 5;
        index = start - 1;

        for (Member test : context.betweenPredecessors(0, members.get(start), members.get(stop))) {
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

        for (Member test : context.betweenSuccessor(0, members.get(start), members.get(stop))) {
            if (index == members.size()) {
                index = 0; // wrap around
            }
            assertEquals(members.get(index), test, "error at index: " + index);
            index++;
        }
    }

    @Test
    public void predecessor() {
        Member predecessor = context.predecessor(0, members.get(6));
        assertEquals(5, members.indexOf(predecessor));
        assertEquals(members.size() - 1, members.indexOf(context.predecessor(0, members.get(0))));
    }

    @Test
    public void predecessors() {
        Collection<Member> predecessors = context.streamPredecessors(0, members.get(5),
                                                                     m -> m.equals(members.get(members.size() - 3)))
                                                 .collect(Collectors.toList());
        assertFalse(predecessors.isEmpty());
        assertEquals(7, predecessors.size());
    }

    @Test
    public void rank() {
        assertEquals(members.size() - 2, context.rank(0, members.get(0), members.get(members.size() - 1)));

        assertEquals(members.size() - 2,
                     context.rank(0, members.get(members.size() - 1), members.get(members.size() - 2)));

        assertEquals(members.size() - 2, context.rank(0, members.get(1), members.get(0)));

        assertEquals(7, context.rank(0, members.get(5), members.get(3)));

        assertEquals(4, context.rank(0, members.get(2), members.get(7)));
    }

    @Test
    public void successor() {
        assertEquals(5, members.indexOf(context.successor(0, members.get(4))));
        assertEquals(0, members.indexOf(context.successor(0, members.get(members.size() - 1))));

        for (int i = 0; i < members.size(); i++) {
            int successor = (i + 1) % members.size();
            assertEquals(successor, members.indexOf(context.successor(0, members.get(i))));
        }
    }

    @Test
    public void successors() {
        Collection<Member> successors = context.streamSuccessors(0, members.get(5), m -> m.equals(members.get(3)))
                                               .collect(Collectors.toList());
        assertFalse(successors.isEmpty());
        assertEquals(7, successors.size());
    }

    @Test
    public void theRing() {

        assertEquals(members.size(), context.size());

        for (int start = 0; start < members.size(); start++) {
            int index = start + 1;
            for (Member member : context.traverse(0, members.get(start))) {
                if (index == members.size()) {
                    index = 0; // wrap around
                }
                assertEquals(members.get(index), member);
                index++;
            }
        }
    }
}
