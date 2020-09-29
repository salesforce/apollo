/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.PregenPopulation.getCa;
import static com.salesforce.apollo.fireflies.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.proto.AccusationDigest;
import com.salesfoce.apollo.proto.CertificateDigest;
import com.salesfoce.apollo.proto.Digests;
import com.salesfoce.apollo.proto.Gossip;
import com.salesfoce.apollo.proto.NoteDigest;
import com.salesfoce.apollo.proto.Update;
import com.salesforce.apollo.comm.LocalCommSimm;
import com.salesforce.apollo.membership.CertWithKey;
import com.salesforce.apollo.membership.Util;
import com.salesforce.apollo.protocols.HashKey;

import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class ViewTest {

    private static final RootCertificate           ca         = getCa();
    private static final Map<HashKey, CertWithKey> certs      = new HashMap<>();
    private static List<Participant>               members    = new ArrayList<>();
    private static Node                            node;
    private static final FirefliesParameters       parameters = new FirefliesParameters(ca.getX509Certificate());

    @BeforeAll
    public static void beforeClass() {
        node = new Node(getMember(1), parameters);
        members = IntStream.range(2, 10).parallel().mapToObj(i -> {
            CertWithKey cert = getMember(i);
            Participant member = new Participant(cert.getCertificate(), parameters);
            certs.put(member.getId(), cert);
            return member;
        }).collect(Collectors.toList());
    }

    @BeforeEach
    public void before() {
        node.reset();
        members.forEach(e -> e.reset());
    }

    /**
     * Test 2 rounds of gossip between 2 views
     */
    @Test
    public void rumors() {
        LocalCommSimm clientFactory = mock(LocalCommSimm.class);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        View viewNode = new View(node, clientFactory, scheduler);
        View viewM0 = new View(new Node(certs.get(members.get(0).getId()), parameters), clientFactory, scheduler);

        viewNode.getService().start(Duration.ofMillis(20_000), Collections.emptyList());
        viewM0.getService().start(Duration.ofMillis(20_000), Collections.emptyList());

        Participant m0 = viewM0.getView().get(members.get(0).getId());

        Digests vN = viewNode.commonDigests();
        Digests vM0 = viewM0.commonDigests();

        Gossip vNreply = viewNode.getService().rumors(0, vM0, m0.getId(), m0.getCertificate(), m0.getSignedNote());
        assertNotNull(vNreply);
        assertFalse(vNreply.getRedirect());
        assertEquals(0, vNreply.getCertificates().getDigestsCount());
        assertEquals(1, vNreply.getCertificates().getUpdatesCount());
        assertEquals(0, vNreply.getNotes().getDigestsCount());
        assertEquals(1, vNreply.getNotes().getUpdatesCount());

        Gossip vM0reply = viewM0.getService().rumors(0, vN, node.getId(), node.getCertificate(), node.getSignedNote());
        assertNotNull(vM0reply);
        assertEquals(0, vM0reply.getCertificates().getDigestsCount());
        assertEquals(1, vM0reply.getCertificates().getUpdatesCount());
        assertEquals(0, vM0reply.getNotes().getDigestsCount());
        assertEquals(1, vM0reply.getCertificates().getUpdatesCount());

        Update vM0nextRound = viewM0.response(vNreply);
        assertNotNull(vM0nextRound);
        assertEquals(0, vM0nextRound.getCertificatesCount());
        assertEquals(0, vM0nextRound.getNotesCount());

        Update vNnextRound = viewNode.response(vM0reply);
        assertNotNull(vNnextRound);
        assertEquals(0, vNnextRound.getCertificatesCount());
        assertEquals(0, vNnextRound.getNotesCount());

        viewNode.getService().update(0, vM0nextRound, viewM0.getNode().getId());
        assertEquals(2, viewNode.getLive().size());

        viewM0.getService().update(0, vNnextRound, m0.getId());
        assertEquals(2, viewM0.getLive().size());

    }

    @Test
    public void parameters() {
        int first = Util.minMajority(0.20, 0.01);
        int second = Util.minMajority(0.20, 100, 1);

        assertEquals(second, first);
    }

    @Test
    public void smoke() throws Exception {
        LocalCommSimm clientFactory = mock(LocalCommSimm.class);
        View view = new View(node, clientFactory, Executors.newSingleThreadScheduledExecutor());
        view.getService().start(Duration.ofMillis(20_000), Collections.emptyList());
        assertEquals(1, view.getLive().size());

        List<CertificateDigest> certGossip = view.gatherCertificateDigests();
        assertEquals(0, certGossip.size());
        List<NoteDigest> noteGossip = view.gatherNoteDigests();
        assertEquals(0, noteGossip.size());
        List<AccusationDigest> accGossip = view.gatherAccusationDigests();
        assertEquals(0, accGossip.size());

        Participant m0 = members.get(0);
        view.add(m0.getCertificate());
        assertEquals(1, view.getLive().size());
        assertEquals(1, view.getFailed().size());

        Participant testMember = view.getView().get(m0.getId());
        assertNotNull(testMember);
        BitSet mask = new BitSet(parameters.rings);
        for (int i = 0; i < parameters.toleranceLevel + 1; i++) {
            mask.set(i);
        }
        view.add(generateNote(m0, 1, mask));

        assertEquals(2, view.getLive().size());
        certGossip = view.gatherCertificateDigests();
        assertEquals(1, certGossip.size());

        noteGossip = view.gatherNoteDigests();
        assertEquals(1, noteGossip.size());
        view.add(node.accuse(testMember, 1));
        assertTrue(testMember.isAccused());
        assertTrue(view.getPendingRebutals().containsKey(testMember.getId()));

        accGossip = view.gatherAccusationDigests();
        assertEquals(1, accGossip.size());

        view.add(generateNote(m0, 2, mask));
        assertFalse(testMember.isAccused());
        assertFalse(view.getPendingRebutals().containsKey(testMember.getId()));
    }

    private Note generateNote(Participant m, int epoch, BitSet mask) throws NoSuchAlgorithmException,
                                                                     InvalidKeyException {
        Signature s = Signature.getInstance(node.getParameters().signatureAlgorithm);
        s.initSign(certs.get(m.getId()).getPrivateKey());
        return new Note(m.getId(), epoch, mask, s);
    }
}
