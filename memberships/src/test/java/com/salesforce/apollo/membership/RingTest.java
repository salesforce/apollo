/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.cert.Certificates;
import com.salesforce.apollo.membership.impl.MemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class RingTest {

    private static final int    MEMBER_COUNT = 10;
    private static List<Member> members;
    private static final byte[] PROTO        = new byte[32];

    @BeforeAll
    public static void beforeClass() {
        members = new ArrayList<>();
        for (int i = 1; i <= MEMBER_COUNT; i++) {
            Member m = createMember(i);
            members.add(m);
        }
    }

    private static Member createMember(int i) {
        byte[] hash = PROTO.clone();
        hash[31] = (byte) i;
        KeyPair keyPair = SignatureAlgorithm.ED_25519.generateKeyPair();
        Date notBefore = Date.from(Instant.now());
        Date notAfter = Date.from(Instant.now().plusSeconds(10_000));
        Digest id = new Digest(DigestAlgorithm.DEFAULT, hash);
        X509Certificate generated = Certificates.selfSign(false, Utils.encode(id, "foo.com", i, keyPair.getPublic()),
                                                          Utils.secureEntropy(), keyPair, notBefore, notAfter,
                                                          Collections.emptyList());
        return new MemberImpl(id, generated, generated.getPublicKey());
    }

    private Context<Member> context;
    private Ring<Member>    ring;

    @BeforeEach
    public void before() {
        Random entropy = new Random(0x1638);
        byte[] id = new byte[32];
        entropy.nextBytes(id);
        context = new Context<>(new Digest(DigestAlgorithm.DEFAULT, id), 1);
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
    public void noRing() {
        context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin());
        assertEquals(1, context.getRingCount());
        members.forEach(m -> context.activate(m));
        assertEquals(MEMBER_COUNT, context.getActive().size());
    }

    @Test
    public void predecessor() {
        assertEquals(5, members.indexOf(ring.predecessor(members.get(6))));
        assertEquals(members.size() - 1, members.indexOf(ring.predecessor(members.get(0))));
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
    public void successors() {
        Collection<Member> successors = ring.streamSuccessors(members.get(5), m -> m.equals(members.get(3)))
                                            .collect(Collectors.toList());
        assertFalse(successors.isEmpty());
        assertEquals(7, successors.size());
    }

    @Test
    public void testRingCalculation() {
        double epsilon = 0.99999;
        double[] probabilityByzantine = new double[] { 0.01, 0.10, 0.15, 0.20, 0.25, 0.33 };
        int[] cardinality = new int[] { 10, 100, 1_0000, 10_0000, 1_000_000, 10_000_000 };

        for (double pByz : probabilityByzantine) {
            for (int card : cardinality) {
                int t = Context.minMajority(pByz, card, epsilon);
                System.out.println(String.format("T: %s K: %s Pbyz: %s Cardinality: %s", t, 2 * t + 1, pByz, card));
            }
        }
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
