/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import static com.salesforce.apollo.comm.grpc.DomainSockets.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getEventLoopGroup;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getServerDomainSocketChannelClass;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesforce.apollo.archipelago.Portal;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.ViewLifecycleListener;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.demesnes.Demesne;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.thoth.KerlDHT;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * The logical domain of the current "Process" - OS and Simulation defined,
 * 'natch.
 * <p>
 * The ProcessDomain represents a member node in the top level domain and
 * represents the top level container model for the distributed system. This top
 * level domain contains every sub domain as decendents. The membership of this
 * domain is the entirety of all process members in the system. The Context of
 * this domain is also the foundational fireflies membership domain of the
 * entire system.
 * 
 * @author hal.hildebrand
 *
 */
public class ProcessDomain extends Domain {

    private final static Class<? extends io.netty.channel.Channel> channelType = getChannelType();

    private final static EventLoopGroup eventLoopGroup = getEventLoopGroup();
    private final static Logger         log            = LoggerFactory.getLogger(ProcessDomain.class);

    private final DomainSocketAddress  bridge;
    private final KerlDHT              dht;
    private final View                 foundation;
    private final Map<Digest, Demesne> hostedDomains = new ConcurrentHashMap<>();
    private final DomainSocketAddress  kerlEndpoint;
    private final Server               kerlService;
    private final UUID                 listener;
    private final Portal<Member>       portal;
    private final DomainSocketAddress  portalEndpoint;
    private final DomainSocketAddress  signingEndpoint;
    private final Server               signingService;

    public ProcessDomain(Digest group, ControlledIdentifierMember member, Builder builder, String dbURL,
                         Path checkpointBaseDir, Parameters.RuntimeParameters.Builder runtime,
                         InetSocketAddress endpoint, Path commDirectory,
                         com.salesforce.apollo.fireflies.Parameters.Builder ff, TransactionConfiguration txnConfig,
                         EventValidation eventValidation) {
        super(member, builder, dbURL, checkpointBaseDir, runtime, txnConfig);
        var base = Context.<Participant>newBuilder()
                          .setId(group)
                          .setCardinality(params.runtime().foundation().getFoundation().getMembershipCount())
                          .build();
        this.foundation = new View(base, getMember(), endpoint, eventValidation, params.communications(), ff.build(),
                                   DigestAlgorithm.DEFAULT, null, params.exec());
        listener = foundation.register(listener());
        bridge = new DomainSocketAddress(commDirectory.resolve(UUID.randomUUID().toString()).toFile());
        portalEndpoint = new DomainSocketAddress(commDirectory.resolve(UUID.randomUUID().toString()).toFile());
        portal = new Portal<Member>(NettyServerBuilder.forAddress(portalEndpoint)
                                                      .protocolNegotiator(new DomainSocketNegotiator())
                                                      .channelType(getServerDomainSocketChannelClass())
                                                      .workerEventLoopGroup(getEventLoopGroup())
                                                      .bossEventLoopGroup(getEventLoopGroup())
                                                      .intercept(new DomainSocketServerInterceptor()),
                                    s -> handler(portalEndpoint), bridge, runtime.getExec(), Duration.ofMillis(1));
        signingEndpoint = new DomainSocketAddress(commDirectory.resolve(UUID.randomUUID().toString()).toFile());
        signingService = NettyServerBuilder.forAddress(signingEndpoint)
                                           .protocolNegotiator(new DomainSocketNegotiator())
                                           .channelType(getServerDomainSocketChannelClass())
                                           .workerEventLoopGroup(getEventLoopGroup())
                                           .bossEventLoopGroup(getEventLoopGroup())
                                           .build();
        kerlEndpoint = new DomainSocketAddress(commDirectory.resolve(UUID.randomUUID().toString()).toFile());
        kerlService = NettyServerBuilder.forAddress(kerlEndpoint)
                                        .protocolNegotiator(new DomainSocketNegotiator())
                                        .channelType(getServerDomainSocketChannelClass())
                                        .workerEventLoopGroup(getEventLoopGroup())
                                        .bossEventLoopGroup(getEventLoopGroup())
                                        .build();
        final var url = String.format("jdbc:h2:mem:%s-%s;DB_CLOSE_DELAY=-1", member.getId(), "");
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create(url, "", "");
        connectionPool.setMaxConnections(10);
        dht = new KerlDHT(Duration.ofMillis(10), foundation.getContext(), member, connectionPool,
                          params.digestAlgorithm(), params.communications(), params.exec(), Duration.ofSeconds(1),
                          params.runtime().scheduler(), 0.00125, null);
    }

    public View getFoundation() {
        return foundation;
    }

    public CompletableFuture<CertificateWithPrivateKey> provision(Duration duration,
                                                                  SignatureAlgorithm signatureAlgorithm) {
        return member.getIdentifier().provision(Instant.now(), duration, signatureAlgorithm);
    }

    public void spawn(DemesneParameters.Builder prototype) {

    }

    @Override
    public void start() {
        startServices();
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
        foundation.deregister(listener);
        stopServices();
    }

    private ManagedChannel handler(DomainSocketAddress address) {
        return NettyChannelBuilder.forAddress(address)
                                  .eventLoopGroup(eventLoopGroup)
                                  .channelType(channelType)
                                  .keepAliveTime(1, TimeUnit.SECONDS)
                                  .usePlaintext()
                                  .build();
    }

    private ViewLifecycleListener listener() {
        return (context, id, join, leaving) -> {
            for (var d : join) {
                if (d.getIdentifier() instanceof SelfAddressingIdentifier sai) {
                    params.context().activate(context.getMember(sai.getDigest()));
                }
            }
            for (var d : leaving) {
                params.context().remove(d);
            }

            hostedDomains.forEach((viewId, demesne) -> {
                demesne.viewChange(viewId, join, leaving);
            });

            log.info("View change: {} for: {} joining: {} leaving: {} on: {}", id, params.context().getId(),
                     join.size(), leaving.size(), params.member().getId());
        };
    }

    private void startServices() {
        dht.start(params.scheduler(), Duration.ofMillis(10)); // TODO parameterize gossip frequency
        try {
            portal.start();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to start portal, local address: " + bridge.path() + " on: "
            + params.member().getId());
        }
        try {
            kerlService.start();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to start KERL service, local address: " + kerlEndpoint.path()
            + " on: " + params.member().getId());
        }

        try {
            signingService.start();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to start signing service, local address: " + signingEndpoint.path()
            + " on: " + params.member().getId());
        }
    }

    private void stopServices() {
        portal.close(Duration.ofSeconds(30));
        kerlService.shutdown();
        try {
            kerlService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        signingService.shutdown();
        try {
            signingService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        dht.stop();
    }
}
