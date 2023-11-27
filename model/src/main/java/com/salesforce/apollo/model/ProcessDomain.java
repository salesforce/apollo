/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import com.salesfoce.apollo.cryptography.proto.Digeste;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.demesne.proto.SubContext;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.archipelago.Portal;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.cryptography.*;
import com.salesforce.apollo.cryptography.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.ViewLifecycleListener;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.demesnes.Demesne;
import com.salesforce.apollo.model.demesnes.JniBridge;
import com.salesforce.apollo.model.demesnes.comm.DemesneKERLServer;
import com.salesforce.apollo.model.demesnes.comm.OuterContextServer;
import com.salesforce.apollo.model.demesnes.comm.OuterContextService;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.thoth.KerlDHT;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import org.h2.jdbcx.JdbcConnectionPool;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.salesforce.apollo.comm.grpc.DomainSockets.*;
import static com.salesforce.apollo.cryptography.QualifiedBase64.qb64;

/**
 * The logical domain of the current "Process" - OS and Simulation defined, 'natch.
 * <p>
 * The ProcessDomain represents a member node in the top level domain and represents the top level container model for
 * the distributed system. This top level domain contains every sub domain as decendents. The membership of this domain
 * is the entirety of all process members in the system. The Context of this domain is also the foundational fireflies
 * membership domain of the entire system.
 *
 * @author hal.hildebrand
 */
public class ProcessDomain extends Domain {

    private final static Class<? extends io.netty.channel.Channel> channelType = getChannelType();

    private final static Logger log = LoggerFactory.getLogger(ProcessDomain.class);

    private final DomainSocketAddress                                       bridge;
    private final EventLoopGroup                                            clientEventLoopGroup  = getEventLoopGroup();
    private final Path                                                      communicationsDirectory;
    private final EventLoopGroup                                            contextEventLoopGroup = getEventLoopGroup();
    private final KerlDHT                                                   dht;
    private final View                 foundation;
    private final Map<Digest, Demesne> hostedDomains = new ConcurrentHashMap<>();
    private final UUID                 listener;
    private final DomainSocketAddress                                       outerContextEndpoint;
    private final Server                                                    outerContextService;
    private final Portal<Member>                                            portal;
    private final DomainSocketAddress                                       portalEndpoint;
    private final EventLoopGroup                                            portalEventLoopGroup  = getEventLoopGroup();
    private final Map<String, DomainSocketAddress>                          routes                = new HashMap<>();
    private final IdentifierSpecification.Builder<SelfAddressingIdentifier> subDomainSpecification;

    public ProcessDomain(Digest group, ControlledIdentifierMember member, Builder builder, String dbURL,
                         Path checkpointBaseDir, Parameters.RuntimeParameters.Builder runtime,
                         InetSocketAddress endpoint, Path commDirectory,
                         com.salesforce.apollo.fireflies.Parameters.Builder ff, EventValidation eventValidation,
                         IdentifierSpecification.Builder<SelfAddressingIdentifier> subDomainSpecification) {
        super(member, builder, dbURL, checkpointBaseDir, runtime);
        communicationsDirectory = commDirectory;
        var base = Context.<Participant>newBuilder()
                          .setId(group)
                          .setCardinality(params.runtime().foundation().getFoundation().getMembershipCount())
                          .build();
        this.foundation = new View(base, getMember(), endpoint, eventValidation, params.communications(), ff.build(),
                                   DigestAlgorithm.DEFAULT, null);
        final var url = String.format("jdbc:h2:mem:%s-%s;DB_CLOSE_DELAY=-1", member.getId(), "");
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create(url, "", "");
        connectionPool.setMaxConnections(10);
        dht = new KerlDHT(Duration.ofMillis(10), foundation.getContext(), member, connectionPool,
                          params.digestAlgorithm(), params.communications(), Duration.ofSeconds(1), 0.00125, null);
        listener = foundation.register(listener());
        bridge = new DomainSocketAddress(communicationsDirectory.resolve(UUID.randomUUID().toString()).toFile());
        portalEndpoint = new DomainSocketAddress(
        communicationsDirectory.resolve(UUID.randomUUID().toString()).toFile());
        portal = new Portal<>(member.getId(), NettyServerBuilder.forAddress(portalEndpoint)
                                                                .protocolNegotiator(new DomainSocketNegotiator())
                                                                .channelType(getServerDomainSocketChannelClass())
                                                                .workerEventLoopGroup(portalEventLoopGroup)
                                                                .bossEventLoopGroup(portalEventLoopGroup)
                                                                .intercept(new DomainSocketServerInterceptor()),
                              s -> handler(portalEndpoint), bridge, Duration.ofMillis(1), s -> routes.get(s));
        outerContextEndpoint = new DomainSocketAddress(
        communicationsDirectory.resolve(UUID.randomUUID().toString()).toFile());
        outerContextService = NettyServerBuilder.forAddress(outerContextEndpoint)
                                                .protocolNegotiator(new DomainSocketNegotiator())
                                                .channelType(getServerDomainSocketChannelClass())
                                                .addService(new DemesneKERLServer(dht, null))
                                                .addService(outerContextService())
                                                .workerEventLoopGroup(contextEventLoopGroup)
                                                .bossEventLoopGroup(contextEventLoopGroup)
                                                .build();
        this.subDomainSpecification = subDomainSpecification;
    }

    public View getFoundation() {
        return foundation;
    }

    public CertificateWithPrivateKey provision(Duration duration, SignatureAlgorithm signatureAlgorithm) {
        return member.getIdentifier().provision(Instant.now(), duration, signatureAlgorithm);
    }

    public SelfAddressingIdentifier spawn(DemesneParameters.Builder prototype) {
        final var witness = member.getIdentifier().newEphemeral().get();
        final var cloned = prototype.clone();
        var parameters = cloned.setCommDirectory(communicationsDirectory.toString())
                               .setPortal(portalEndpoint.path())
                               .setParent(outerContextEndpoint.path())
                               .build();
        var ctxId = Digest.from(parameters.getContext());
        final AtomicBoolean added = new AtomicBoolean();
        final var demesne = new JniBridge(parameters);
        var computed = hostedDomains.computeIfAbsent(ctxId, k -> {
            added.set(true);
            return demesne;
        });
        if (added.get()) {
            var newSpec = subDomainSpecification.clone();
            // the receiver is a witness to the sub domain's delegated key
            var newWitnesses = new ArrayList<>(subDomainSpecification.getWitnesses());
            newWitnesses.add(new BasicIdentifier(witness.getPublic()));
            newSpec.setWitnesses(newWitnesses);
            var incp = demesne.inception(member.getIdentifier().getIdentifier().toIdent(), newSpec);
            var sigs = new HashMap<Integer, JohnHancock>();
            sigs.put(0, new Signer.SignerImpl(witness.getPrivate(), ULong.MIN).sign(incp.toKeyEvent_().toByteString()));
            var attached = new com.salesforce.apollo.stereotomy.event.AttachmentEvent.AttachmentImpl(sigs);
            var seal = Seal.EventSeal.construct(incp.getIdentifier(), incp.hash(dht.digestAlgorithm()),
                                                incp.getSequenceNumber().longValue());
            var builder = InteractionSpecification.newBuilder().addAllSeals(Collections.singletonList(seal));
            KeyState_ ks = dht.append(AttachmentEvent.newBuilder()
                                                     .setCoordinates(incp.getCoordinates().toEventCoords())
                                                     .setAttachment(attached.toAttachemente())
                                                     .build());
            var coords = member.getIdentifier().seal(builder);
            demesne.commit(coords.toEventCoords());
            demesne.start();
            return (SelfAddressingIdentifier) incp.getIdentifier();
        }
        return computed.getId();
    }

    @Override
    public void start() {
        startServices();
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
        hostedDomains.values().forEach(d -> d.stop());
        foundation.deregister(listener);
        try {
            stopServices();
        } catch (RejectedExecutionException e) {

        }
        var portalELG = portalEventLoopGroup.shutdownGracefully(100, 1_000, TimeUnit.MILLISECONDS);
        var serverELG = contextEventLoopGroup.shutdownGracefully(100, 1_000, TimeUnit.MILLISECONDS);
        var clientELG = clientEventLoopGroup.shutdownGracefully(100, 1_000, TimeUnit.MILLISECONDS);
        try {
            if (clientELG.await(30, TimeUnit.SECONDS)) {
                log.info("Did not completely shutdown client event loop group for process: {}", member.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            if (!serverELG.await(30, TimeUnit.SECONDS)) {
                log.info("Did not completely shutdown server event loop group for process: {}", member.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            if (!portalELG.await(30, TimeUnit.SECONDS)) {
                log.info("Did not completely shutdown portal event loop group for process: {}", member.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ManagedChannel handler(DomainSocketAddress address) {
        return NettyChannelBuilder.forAddress(address)
                                  .executor(executor)
                                  .eventLoopGroup(clientEventLoopGroup)
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

    private void startServices() {
        dht.start(Duration.ofMillis(10)); // TODO parameterize gossip frequency
        try {
            portal.start();
        } catch (IOException e) {
            throw new IllegalStateException(
            "Unable to start portal, local address: " + bridge.path() + " on: " + params.member().getId());
        }
        try {
            outerContextService.start();
        } catch (IOException e) {
            throw new IllegalStateException(
            "Unable to start outer context service, local address: " + outerContextEndpoint.path() + " on: "
            + params.member().getId());
        }
    }

    private void stopServices() {
        portal.close(Duration.ofSeconds(30));
        try {
            outerContextService.shutdown();
        } catch (RejectedExecutionException e) {
            // eat
        } catch (Throwable t) {
            log.error("Exception shutting down process domain: {}", member.getId(), t);
        }
        try {
            outerContextService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        dht.stop();
    }
}
