/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.cryptography.cert.Certificates;
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
public class RingTest {

    private static final int          MEMBER_COUNT = 10;
    private static final byte[]       PROTO        = new byte[32];
    private static       List<Member> members;
    private Context<Member> context;
    private Ring<Member>    ring;

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
        context = new ContextImpl<Member>(new Digest(DigestAlgorithm.DEFAULT, id), members.size(), 0.2, 2);
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
            assertEquals(test, members.get(index), "index: " + index + " not equals");
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
    public void incrementBreaksTwoThirdsMajority() {
        double epsilon = 0.99999;
        double[] probabilityByzantine = new double[] { 0.01, 0.10, 0.15, 0.20 };

        for (double pByz : probabilityByzantine) {
            int tPrev = 0;
            for (int card = 4; card < 10_000; card++) {
                try {
                    var t = Context.minMajority(pByz, card, epsilon, 3);
                    if (t != tPrev) {
                        System.out.printf("Bias: 3 T: %s K: %s Pbyz: %s Cardinality: %s%n", t, (3 * t) + 1, pByz, card);
                    }
                    tPrev = t;
                } catch (Exception e) {
                    System.out.printf("Cannot calulate Pbyz: %s Cardinality: %s%n", pByz, card);
                }
            }
        }
    }

    @Test
    public void predecessor() {
        Member predecessor = ring.predecessor(members.get(6));
        assertEquals(5, members.indexOf(predecessor));
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

        assertEquals(7, ring.rank(members.get(5), members.get(3)));

        assertEquals(4, ring.rank(members.get(2), members.get(7)));
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
        int[] cardinality = new int[] { 10, 100, 1_000, 10_000, 1_000_000, 10_000_000 };

        for (double pByz : probabilityByzantine) {
            for (int card : cardinality) {
                int t = Context.minMajority(pByz, card, epsilon);
                System.out.printf("Bias: 2 T: %s K: %s Pbyz: %s Cardinality: %s%n", t, (2 * t) + 1, pByz, card);
            }
        }
    }

    @Test
    public void testRingCalculationTwoThirdsMajority() {
        double epsilon = 0.99999;
        double[] probabilityByzantine = new double[] { 0.01, 0.10, 0.15, 0.20 };
        int[] cardinality = new int[] { 10, 100, 1_000, 10_000, 1_000_000, 10_000_000 };

        for (double pByz : probabilityByzantine) {
            for (int card : cardinality) {
                try {
                    int t = Context.minMajority(pByz, card, epsilon, 3);
                    System.out.printf("Bias: 3 T: %s K: %s Pbyz: %s Cardinality: %s%n", t, (3 * t) + 1, pByz, card);
                } catch (Exception e) {
                    System.out.printf("Cannot calulate Pbyz: %s Cardinality: %s%n", pByz, card);
                }
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
