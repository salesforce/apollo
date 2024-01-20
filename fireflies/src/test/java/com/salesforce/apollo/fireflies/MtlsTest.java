/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.archipelago.*;
import com.salesforce.apollo.comm.grpc.ClientContextSupplier;
import com.salesforce.apollo.comm.grpc.ServerContextSupplier;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.cryptography.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.cryptography.ssl.CertificateValidator;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.*;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Utils;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MtlsTest {
    private static final int                                                         CARDINALITY;
    private static final Map<Digest, CertificateWithPrivateKey>                      certs       = new HashMap<>();
    private static final Map<Digest, InetSocketAddress>                              endpoints   = new HashMap<>();
    private static final boolean                                                     LARGE_TESTS = Boolean.getBoolean(
    "large_tests");
    private static       Map<Digest, ControlledIdentifier<SelfAddressingIdentifier>> identities;

    static {
        CARDINALITY = LARGE_TESTS ? 20 : 10;
    }

    private final List<Router> communications = new ArrayList<>();
    private final Executor     executor       = Executors.newFixedThreadPool(10);
    private       List<View>   views;

    @BeforeAll
    public static void beforeClass() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        String localhost = InetAddress.getLoopbackAddress().getHostName();
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        identities = IntStream.range(0, CARDINALITY).mapToObj(i -> {
            return stereotomy.newIdentifier();
        }).collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(), controlled -> controlled));
        identities.entrySet().forEach(e -> {
            InetSocketAddress endpoint = new InetSocketAddress(localhost, Utils.allocatePort());
            certs.put(e.getKey(),
                      e.getValue().provision(Instant.now(), Duration.ofDays(1), SignatureAlgorithm.DEFAULT));
            endpoints.put(e.getKey(), endpoint);
        });
    }

    @AfterEach
    public void after() {
        if (views != null) {
            views.forEach(e -> e.stop());
            views.clear();
        }
        if (communications != null) {
            communications.forEach(e -> e.close(Duration.ofSeconds(1)));
            communications.clear();
        }
    }

    @Test
    public void smoke() throws Exception {
        var parameters = Parameters.newBuilder().setMaximumTxfr(20).build();
        final Duration duration = Duration.ofMillis(50);
        var registry = new MetricRegistry();
        var node0Registry = new MetricRegistry();

        var members = identities.values().stream().map(identity -> new ControlledIdentifierMember(identity)).toList();
        var ctxBuilder = Context.<Participant>newBuilder().setCardinality(CARDINALITY);

        var seeds = members.stream()
                           .map(m -> new Seed(m.getIdentifier().getIdentifier(), endpoints.get(m.getId())))
                           .limit(LARGE_TESTS ? 24 : 3)
                           .toList();

        var builder = ServerConnectionCache.newBuilder().setTarget(30);
        var frist = new AtomicBoolean(true);
        Function<Member, SocketAddress> resolver = m -> ((Participant) m).endpoint();

        var clientContextSupplier = clientContextSupplier();
        views = members.stream().map(node -> {
            Context<Participant> context = ctxBuilder.build();
            FireflyMetricsImpl metrics = new FireflyMetricsImpl(context.getId(),
                                                                frist.getAndSet(false) ? node0Registry : registry);
            EndpointProvider ep = new StandardEpProvider(endpoints.get(node.getId()), ClientAuth.REQUIRE,
                                                         CertificateValidator.NONE, resolver);
            builder.setMetrics(new ServerConnectionCacheMetricsImpl(frist.getAndSet(false) ? node0Registry : registry));
            CertificateWithPrivateKey certWithKey = certs.get(node.getId());
            Router comms = new MtlsServer(node, ep, clientContextSupplier, serverContextSupplier(certWithKey),
                                          executor).router(builder);
            communications.add(comms);
            return new View(context, node, endpoints.get(node.getId()), EventValidation.NONE, Verifiers.NONE, comms,
                            parameters, DigestAlgorithm.DEFAULT, metrics);
        }).collect(Collectors.toList());

        var then = System.currentTimeMillis();
        communications.forEach(e -> e.start());

        var countdown = new AtomicReference<>(new CountDownLatch(1));

        views.get(0).start(() -> countdown.get().countDown(), duration, Collections.emptyList());

        assertTrue(countdown.get().await(30, TimeUnit.SECONDS), "KERNEL did not stabilize");

        var seedlings = views.subList(1, seeds.size());
        var kernel = seeds.subList(0, 1);

        countdown.set(new CountDownLatch(seedlings.size()));

        seedlings.forEach(view -> view.start(() -> countdown.get().countDown(), duration, kernel));

        assertTrue(countdown.get().await(30, TimeUnit.SECONDS), "Seeds did not stabilize");

        countdown.set(new CountDownLatch(views.size() - seeds.size()));
        views.forEach(view -> view.start(() -> countdown.get().countDown(), duration, seeds));

        assertTrue(Utils.waitForCondition(120_000, 1_000, () -> {
            return views.stream()
                        .map(view -> view.getContext().activeCount() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }), "view did not stabilize: " + views.stream()
                                              .map(view -> view.getContext().activeCount())
                                              .collect(Collectors.toList()));
        System.out.println(
        "View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all " + views.size()
        + " members");

        System.out.println("Checking views for consistency");
        var failed = views.stream()
                          .filter(e -> e.getContext().activeCount() != views.size())
                          .map(v -> String.format("%s : %s ", v.getNode().getId(), v.getContext().activeCount()))
                          .toList();
        assertEquals(0, failed.size(),
                     " expected: " + views.size() + " failed: " + failed.size() + " views: " + failed);

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
                                            String tlsVersion) {
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
                                        Provider provider) {
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
