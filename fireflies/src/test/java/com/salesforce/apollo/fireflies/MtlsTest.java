/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import com.salesforce.apollo.comm.EndpointProvider;
import com.salesforce.apollo.comm.MtlsRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCacheMetricsImpl;
import com.salesforce.apollo.comm.StandardEpProvider;
import com.salesforce.apollo.comm.grpc.ClientContextSupplier;
import com.salesforce.apollo.comm.grpc.MtlsServer;
import com.salesforce.apollo.comm.grpc.ServerContextSupplier;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Utils;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MtlsTest {
    private static final int                                                   CARDINALITY;
    private static final Map<Digest, CertificateWithPrivateKey>                certs       = new HashMap<>();
    private static final Map<Digest, InetSocketAddress>                        endpoints   = new HashMap<>();
    private static final Random                                                entropy     = new Random(0x666);
    private static Map<Digest, ControlledIdentifier<SelfAddressingIdentifier>> identities;
    private static final boolean                                               LARGE_TESTS = Boolean.getBoolean("large_tests");
    static {
        CARDINALITY = LARGE_TESTS ? 100 : 10;
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        String localhost = InetAddress.getLocalHost().getHostName();
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        identities = IntStream.range(0, CARDINALITY)
                              .parallel()
                              .mapToObj(i -> stereotomy.newIdentifier().get())
                              .collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(),
                                                        controlled -> controlled));
        identities.entrySet().forEach(e -> {
            InetSocketAddress endpoint = new InetSocketAddress(localhost, Utils.allocatePort());
            certs.put(e.getKey(),
                      e.getValue().provision(Instant.now(), Duration.ofDays(1), SignatureAlgorithm.DEFAULT).get());
            endpoints.put(e.getKey(), endpoint);
        });
    }

    private List<Router> communications = new ArrayList<>();
    private List<View>   views;

    @AfterEach
    public void after() {
        if (views != null) {
            views.forEach(e -> e.stop());
            views.clear();
        }
        if (communications != null) {
            communications.forEach(e -> e.close());
            communications.clear();
        }
    }

    @Test
    public void smoke() throws Exception {
        var registry = new MetricRegistry();
        var node0Registry = new MetricRegistry();

        var seeds = new ArrayList<Seed>();
        var members = identities.values().stream().map(identity -> new ControlledIdentifierMember(identity)).toList();
        var ctxBuilder = Context.<Participant>newBuilder().setCardinality(CARDINALITY);

        while (seeds.size() < ctxBuilder.build().getRingCount() + 1) {
            var member = members.get(entropy.nextInt(members.size()));
            var id = new Seed(member.getEvent().getCoordinates(), endpoints.get(member.getId()));
            if (!seeds.contains(id)) {
                seeds.add(id);
            }
        }
        var scheduler = Executors.newScheduledThreadPool(members.size());
        Executor exec = Executors.newFixedThreadPool(CARDINALITY);

        var builder = ServerConnectionCache.newBuilder().setTarget(2);
        var frist = new AtomicBoolean(true);
        Function<Member, SocketAddress> resolver = m -> {
            var p = (Participant) m;
            return p.endpoint();
        };
        var clientContextSupplier = clientContextSupplier();
        views = members.stream().map(node -> {
            Context<Participant> context = ctxBuilder.build();
            FireflyMetricsImpl metrics = new FireflyMetricsImpl(context.getId(),
                                                                frist.getAndSet(false) ? node0Registry : registry);
            EndpointProvider ep = new StandardEpProvider(endpoints.get(node.getId()), ClientAuth.REQUIRE,
                                                         CertificateValidator.NONE, resolver);
            builder.setMetrics(new ServerConnectionCacheMetricsImpl(frist.getAndSet(false) ? node0Registry : registry));
            CertificateWithPrivateKey certWithKey = certs.get(node.getId());
            MtlsRouter comms = new MtlsRouter(builder, ep, serverContextSupplier(certWithKey),
                                              Executors.newFixedThreadPool(3), clientContextSupplier);
            communications.add(comms);
            return new View(context, node, endpoints.get(node.getId()), EventValidation.NONE, comms, 0.0125,
                            DigestAlgorithm.DEFAULT, metrics, exec);
        }).collect(Collectors.toList());

        var then = System.currentTimeMillis();
        communications.forEach(e -> e.start());
        views.forEach(view -> view.start(Duration.ofMillis(200), seeds, scheduler));

        assertTrue(Utils.waitForCondition(60_000, 1_000, () -> {
            return views.stream()
                        .map(view -> view.getContext().activeCount() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }), "view did not stabilize: "
        + views.stream().map(view -> view.getContext().activeCount()).collect(Collectors.toList()));
        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
        + views.size() + " members");

        System.out.println("Checking views for consistency");
        var invalid = views.stream()
                           .map(view -> view.getContext().activeCount() != views.size() ? view : null)
                           .filter(view -> view != null)
                           .collect(Collectors.toList());
        assertEquals(0, invalid.size(), invalid.stream().map(view -> {
            var difference = Sets.difference(views.stream().map(v -> v.getNode().getId()).collect(Collectors.toSet()),
                                             view.getContext()
                                                 .active()
                                                 .map(m -> m.getId())
                                                 .collect(Collectors.toSet()));
            return "Invalid membership: " + view.getNode() + ", missing: " + difference.size();
        }).collect(Collectors.toList()).toString());

        System.out.println("Stoping views");
        views.forEach(view -> view.stop());

        System.out.println("Restarting views");
        views.forEach(view -> view.start(Duration.ofMillis(1000), seeds, scheduler));

        assertTrue(Utils.waitForCondition(30_000, 100, () -> {
            return views.stream()
                        .map(view -> view.getContext().activeCount() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }));

        System.out.println("Stabilized, now sleeping to see if views remain stabilized");
        Thread.sleep(10_000);
        assertTrue(Utils.waitForCondition(30_000, 100, () -> {
            return views.stream()
                        .map(view -> view.getContext().activeCount() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }));
        System.out.println("View has stabilized after restart in " + (System.currentTimeMillis() - then) + " Ms");

        System.out.println("Checking views for consistency");
        invalid = views.stream()
                       .map(view -> view.getContext().activeCount() != views.size() ? view : null)
                       .filter(view -> view != null)
                       .collect(Collectors.toList());
        assertEquals(0, invalid.size());

        System.out.println("Stoping views");
        views.forEach(view -> view.stop());

        ConsoleReporter.forRegistry(node0Registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
    }

    private Function<Member, ClientContextSupplier> clientContextSupplier() {
        return m -> {
            return new ClientContextSupplier() {
                @Override
                public SslContext forClient(ClientAuth clientAuth, String alias, CertificateValidator validator,
                                            Provider provider, String tlsVersion) {
                    CertificateWithPrivateKey certWithKey = certs.get(m.getId());
                    return MtlsServer.forClient(clientAuth, alias, certWithKey.getX509Certificate(),
                                                certWithKey.getPrivateKey(), validator);
                }
            };
        };
    }

    private ServerContextSupplier serverContextSupplier(CertificateWithPrivateKey certWithKey) {
        return new ServerContextSupplier() {
            @Override
            public SslContext forServer(ClientAuth clientAuth, String alias, CertificateValidator validator,
                                        Provider provider, String tlsVersion) {
                return MtlsServer.forServer(clientAuth, alias, certWithKey.getX509Certificate(),
                                            certWithKey.getPrivateKey(), validator);
            }

            @Override
            public Digest getMemberId(X509Certificate key) {
                return ((SelfAddressingIdentifier) Stereotomy.decode(key).get().identifier()).getDigest();
            }
        };
    }
}
