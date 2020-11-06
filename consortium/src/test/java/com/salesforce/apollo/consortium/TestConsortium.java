/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getCa;
import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Body;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.Certification;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Genesis;
import com.salesfoce.apollo.consortium.proto.Header;
import com.salesfoce.apollo.consortium.proto.Reconfigure;
import com.salesfoce.apollo.consortium.proto.ViewMember;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.consortium.Consortium.CommitteeMember;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.membership.CertWithKey;
import com.salesforce.apollo.membership.messaging.Messenger.Parameters;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;
import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 *
 */
public class TestConsortium {

    private static final RootCertificate                   ca         = getCa();
    private static Map<HashKey, CertificateWithPrivateKey> certs;
    private static final FirefliesParameters               parameters = new FirefliesParameters(
            ca.getX509Certificate());

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 100)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Utils.getMemberId(cert.getX509Certificate()), cert -> cert));
    }

    private File                   baseDir;
    private Builder                builder        = ServerConnectionCache.newBuilder().setTarget(30);
    private Map<HashKey, Router>   communications = new HashMap<>();
    private final List<Consortium> consortium     = new ArrayList<>();
    private Random                 entropy;
    private List<Node>             members;
    private List<X509Certificate>  seeds;
    private List<View>             views;

    @AfterEach
    public void after() {
        consortium.forEach(e -> e.stop());
        consortium.clear();
        views.forEach(e -> e.getService().stop());
        views.clear();
        communications.values().forEach(e -> e.close());
        communications.clear();
    }

    @BeforeEach
    public void before() {

        baseDir = new File(System.getProperty("user.dir"), "target/cluster");
        Utils.clean(baseDir);
        baseDir.mkdirs();
        entropy = new Random(0x666);

        seeds = new ArrayList<>();
        int testCardinality = 99;
        assertTrue(certs.size() >= testCardinality);

        members = new ArrayList<>();
        for (CertificateWithPrivateKey cert : certs.values()) {
            if (members.size() < testCardinality) {
                members.add(new Node(new CertWithKey(cert.getX509Certificate(), cert.getPrivateKey()), parameters));
            } else {
                break;
            }
        }

        assertEquals(testCardinality, members.size());

        while (seeds.size() < Math.min(parameters.toleranceLevel + 1, certs.size())) {
            CertificateWithPrivateKey cert = certs.get(members.get(entropy.nextInt(testCardinality)).getId());
            if (!seeds.contains(cert.getX509Certificate())) {
                seeds.add(cert.getX509Certificate());
            }
        }

        System.out.println("Test cardinality: " + testCardinality + " seeds: "
                + seeds.stream().map(e -> Utils.getMemberId(e)).collect(Collectors.toList()));

        AtomicBoolean frist = new AtomicBoolean(true);
        views = members.stream().map(node -> {
            Router comms = new LocalRouter(node.getId(), builder);
            communications.put(node.getId(), comms);
            frist.set(false);
            return new View(HashKey.ORIGIN, node, comms, null);
        }).collect(Collectors.toList());
    }

    @Test
    public void smoke() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(20);
        Duration ffRound = Duration.ofMillis(1_00);

        communications.values().forEach(e -> e.start());
        views.parallelStream().forEach(view -> view.getService().start(ffRound, seeds, scheduler));
        System.out.println("Stabilizing view");
        assertTrue(Utils.waitForCondition(60_000, 3_000, () -> {
            return views.stream()
                        .map(view -> view.getLive().size() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }), "Could not stabilize view membership)");
        System.out.println("Stabilized view across " + views.size() + " members");

        Duration gossipDuration = Duration.ofMillis(100);
        Parameters msgParameters = Parameters.newBuilder().setEntropy(new SecureRandom()).build();
        views.stream()
             .map(v -> new Consortium(Consortium.Parameters.newBuilder()
                                                           .setExecutor(t -> Collections.emptyList())
                                                           .setMember(v.getNode())
                                                           .setSignature(() -> v.getNode().forSigning())
                                                           .setContext(v.getContext())
                                                           .setMsgParameters(msgParameters)
                                                           .setCommunications(communications.get(v.getNode().getId()))
                                                           .setGossipDuration(gossipDuration)
                                                           .setScheduler(scheduler)
                                                           .build()))
             .forEach(e -> consortium.add(e));

        List<Consortium> blueRibbon = new ArrayList<>();
        for (int i = 0; i < parameters.rings; i++) {
            blueRibbon.add(consortium.get(i));
        }
        CertifiedBlock genesis = createGenesis(blueRibbon);

        System.out.println("starting consortium");

        consortium.forEach(e -> e.start());

        System.out.println("processing genesis block");

        consortium.parallelStream().forEach(c -> c.process(genesis));

        System.out.println("genesis block processed");

        Consortium client = consortium.get(blueRibbon.size() + 1);
        try {
            client.submit(Arrays.asList("Hello world".getBytes()), h -> {
            });
        } catch (TimeoutException e) {
            fail();
        }

        boolean submitted = Utils.waitForCondition(10_000, 1_000, () -> blueRibbon.stream().map(collaborator -> {
            CommitteeMember member = (CommitteeMember) collaborator.getState();
            return member.pending.isEmpty();
        }).filter(b -> b).count() == 0);

        assertTrue(submitted,
                   "Transaction not submitted to consortium, missing: " + blueRibbon.stream().map(collaborator -> {
                       CommitteeMember member = (CommitteeMember) collaborator.getState();
                       return member.pending.isEmpty();
                   }).filter(b -> b).count());

    }

    private CertifiedBlock createGenesis(List<Consortium> blueRibbon) {
        byte[] viewID = new byte[32];
        entropy.nextBytes(viewID);
        Reconfigure.Builder genesisView = Reconfigure.newBuilder()
                                                     .setCheckpointBlocks(256)
                                                     .setId(ByteString.copyFrom(viewID))
                                                     .setToleranceLevel(parameters.toleranceLevel);
        blueRibbon.forEach(e -> {
            genesisView.addView(ViewMember.newBuilder()
                                          .setId(e.getMember().getId().toByteString())
                                          .setConsensusKey(ByteString.copyFrom(e.getMember()
                                                                                .getCertificate()
                                                                                .getPublicKey()
                                                                                .getEncoded())));
        });
        Body genesisBody = Body.newBuilder()
                               .setConsensusId(0)
                               .setType(BodyType.GENESIS)
                               .setContents(Genesis.newBuilder()
                                                   .setGenesisData(ByteString.copyFromUtf8("hello world"))
                                                   .setInitialView(genesisView)
                                                   .build()
                                                   .toByteString())
                               .build();
        byte[] bodyHash = Conversion.hashOf(genesisBody.toByteArray());

        Header header = Header.newBuilder().setHeight(0).setBodyHash(ByteString.copyFrom(bodyHash)).build();
        CertifiedBlock.Builder certifiedGenesis = CertifiedBlock.newBuilder()
                                                                .setBlock(Block.newBuilder()
                                                                               .setHeader(header)
                                                                               .setBody(genesisBody));
        byte[] headerBytes = header.toByteArray();
        blueRibbon.stream().map(c -> c.getMember()).map(c -> (Node) c).forEach(n -> {
            Signature signature = n.forSigning();
            try {
                signature.update(headerBytes);
                certifiedGenesis.addCertifications(Certification.newBuilder()
                                                                .setId(n.getId().toByteString())
                                                                .setSignature(ByteString.copyFrom(signature.sign())));
            } catch (SignatureException e1) {
                throw new IllegalStateException(e1);
            }
        });

        CertifiedBlock genesis = certifiedGenesis.build();
        return genesis;
    }

}
