/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.PregenPopulation.getCa;
import static com.salesforce.apollo.fireflies.PregenPopulation.getMember;
import static org.junit.Assert.assertEquals;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import com.salesforce.apollo.fireflies.communications.FfLocalCommSim;
import com.salesforce.apollo.fireflies.stats.DropWizardStatsPlugin;

import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FunctionalTest {
    private static final RootCertificate ca = getCa();
    private static Map<UUID, CertWithKey> certs;
    private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

    @BeforeClass
    public static void beforeClass() {
        certs = IntStream.range(1, 11)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberId(cert.getCertificate()),
                                                   cert -> cert));
    }

    @Test
    public void e2e() throws Exception {
        Random entropy = new Random(0x666);

        List<X509Certificate> seeds = new ArrayList<>();
        MetricRegistry registry = new MetricRegistry();
        FfLocalCommSim communications = new FfLocalCommSim(new DropWizardStatsPlugin(registry));
        communications.checkStarted(false);
        List<Node> members = certs.values()
                                  .parallelStream()
                                  .map(cert -> new CertWithKey(cert.getCertificate(), cert.getPrivateKey()))
                                  .map(cert -> new Node(cert, parameters))
                                  .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());

        while (seeds.size() < parameters.toleranceLevel + 1) {
            CertWithKey cert = certs.get(members.get(entropy.nextInt(members.size())).getId());
            if (!seeds.contains(cert.getCertificate())) {
                seeds.add(cert.getCertificate());
            }
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        List<View> views = members.parallelStream()
                                  .map(node -> new View(node, communications, seeds, scheduler))
                                  .collect(Collectors.toList());
        assertEquals(members.size(), communications.getServers().size());

        for (int i = 0; i < parameters.rings + 2; i++) {
            views.forEach(view -> view.getService().gossip());
        }
        for (int i = 0; i < parameters.rings + 2; i++) {
            views.forEach(view -> view.getService().gossip());
        }

        List<View> invalid = views.stream()
                                  .map(view -> view.getLive().size() != views.size() ? view : null)
                                  .filter(view -> view != null)
                                  .collect(Collectors.toList());
        assertEquals(invalid.stream().map(view -> {
            Set<?> difference = Sets.difference(views.stream()
                                                     .map(v -> v.getNode().getId())
                                                     .collect(Collectors.toSet()),
                                                view.getLive().keySet());
            return "Invalid membership: " + view.getNode() + ", missing: " + difference.size();
        }).collect(Collectors.toList()).toString(), 0, invalid.size());

        View frist = views.get(0);
        for (View view : views) {
            for (int ring = 0; ring < parameters.rings; ring++) {
                Ring trueRing = frist.getRing(ring);
                Ring comparedTo = view.getRing(ring);
                assertEquals(trueRing.getRing(), comparedTo.getRing());
                assertEquals(trueRing.successor(view.getNode()), comparedTo.successor(view.getNode()));
            }
        }
    }
}
