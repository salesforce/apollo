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
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.demesne.proto.SubContext;
import com.salesfoce.apollo.model.proto.Request;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesfoce.apollo.utils.proto.Sig;
import com.salesforce.apollo.archipelago.Portal;
import com.salesforce.apollo.archipelago.RoutableService;
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
import com.salesforce.apollo.model.comms.SigningServer;
import com.salesforce.apollo.model.demesnes.Demesne;
import com.salesforce.apollo.model.demesnes.comm.DemesneKERLServer;
import com.salesforce.apollo.model.demesnes.comm.OuterContextServer;
import com.salesforce.apollo.model.demesnes.comm.OuterContextService;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;
import com.salesforce.apollo.thoth.KerlDHT;

import io.grpc.BindableService;
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
    private final UUID                 listener;
    private final DomainSocketAddress  outerContextEndpoint;
    private final Server               outerContextService;
    private final Portal<Member>       portal;
    private final DomainSocketAddress  portalEndpoint;

    private final Map<String, DomainSocketAddress> routes = new HashMap<>();

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
        final var url = String.format("jdbc:h2:mem:%s-%s;DB_CLOSE_DELAY=-1", member.getId(), "");
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create(url, "", "");
        connectionPool.setMaxConnections(10);
        dht = new KerlDHT(Duration.ofMillis(10), foundation.getContext(), member, connectionPool,
                          params.digestAlgorithm(), params.communications(), params.exec(), Duration.ofSeconds(1),
                          params.runtime().scheduler(), 0.00125, null);
        listener = foundation.register(listener());
        bridge = new DomainSocketAddress(commDirectory.resolve(UUID.randomUUID().toString()).toFile());
        portalEndpoint = new DomainSocketAddress(commDirectory.resolve(UUID.randomUUID().toString()).toFile());
        portal = new Portal<Member>(NettyServerBuilder.forAddress(portalEndpoint)
                                                      .protocolNegotiator(new DomainSocketNegotiator())
                                                      .channelType(getServerDomainSocketChannelClass())
                                                      .workerEventLoopGroup(getEventLoopGroup())
                                                      .bossEventLoopGroup(getEventLoopGroup())
                                                      .intercept(new DomainSocketServerInterceptor()),
                                    s -> handler(portalEndpoint), bridge, runtime.getExec(), Duration.ofMillis(1),
                                    s -> routes.get(s));
        outerContextEndpoint = new DomainSocketAddress(commDirectory.resolve(UUID.randomUUID().toString()).toFile());
        outerContextService = NettyServerBuilder.forAddress(outerContextEndpoint)
                                                .protocolNegotiator(new DomainSocketNegotiator())
                                                .channelType(getServerDomainSocketChannelClass())
                                                .addService(signingService())
                                                .addService((BindableService) new DemesneKERLServer(dht, null))
                                                .addService(outerContextService())
                                                .workerEventLoopGroup(getEventLoopGroup())
                                                .bossEventLoopGroup(getEventLoopGroup())
                                                .build();
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
        return new ViewLifecycleListener() {

            @Override
            public void update(EventCoordinates update) {
                // TODO Auto-generated method stub

            }

            @Override
            public void viewChange(Context<Participant> context, Digest id, List<EventCoordinates> join,
                                   List<Digest> leaving) {
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
            }
        };
    }

    private BindableService outerContextService() {
        return new OuterContextServer(new OuterContextService() {

            @Override
            public void deregister(Digeste context) {
                routes.remove(qb64(Digest.from(context)));
            }

            @Override
            public void register(SubContext context) {
//                routes.put("",qb64(Digest.from(context)));
            }
        }, null);
    }

    private BindableService signingService() {
        RoutableService<ProtoKERLService> router = new RoutableService<>(params.exec());
        router.bind(foundation.getContext().getId(), dht);
        return new SigningServer(new com.salesforce.apollo.model.comms.Signer() {

            @Override
            public Sig sign(Request request, Digest from) {
                // TODO Auto-generated method stub
                return null;
            }
        }, null, null);
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
            outerContextService.start();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to start outer context service, local address: "
            + outerContextEndpoint.path() + " on: " + params.member().getId());
        }
    }

    private void stopServices() {
        portal.close(Duration.ofSeconds(30));
        outerContextService.shutdown();
        try {
            outerContextService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        dht.stop();
    }
}
