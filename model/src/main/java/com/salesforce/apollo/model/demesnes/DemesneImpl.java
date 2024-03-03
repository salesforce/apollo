/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.demesnes;

import com.salesforce.apollo.archipelago.Enclave;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.demesne.proto.DemesneParameters;
import com.salesforce.apollo.demesne.proto.SubContext;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.context.DynamicContext;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.membership.stereotomy.IdentifierMember;
import com.salesforce.apollo.model.SubDomain;
import com.salesforce.apollo.model.demesnes.comm.OuterContextClient;
import com.salesforce.apollo.stereotomy.*;
import com.salesforce.apollo.stereotomy.caching.CachingKERL;
import com.salesforce.apollo.stereotomy.event.DelegatedInceptionEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedRotationEvent;
import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.jks.JksKeyStore;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.CommonKERLClient;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLAdapter;
import com.salesforce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc;
import com.salesforce.apollo.thoth.Thoth;
import com.salesforce.apollo.utils.Hex;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.salesforce.apollo.archipelago.RouterImpl.clientInterceptor;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.IMPL;

/**
 * Isolate for the Apollo SubDomain stack
 *
 * @author hal.hildebrand
 */
public class DemesneImpl implements Demesne {
    private static final Class<? extends Channel> channelType             = IMPL.getChannelType();
    private static final Duration                 DEFAULT_GOSSIP_INTERVAL = Duration.ofMillis(5);
    private static final EventLoopGroup           eventLoopGroup          = IMPL.getEventLoopGroup();
    private static final Logger                   log                     = LoggerFactory.getLogger(DemesneImpl.class);

    private final    Executor               executor = Executors.newVirtualThreadPerTaskExecutor();
    private final    KERL.AppendKERL        kerl;
    private final    OuterContextClient     outer;
    private final    DemesneParameters      parameters;
    private final    AtomicBoolean          started  = new AtomicBoolean();
    private final    Thoth                  thoth;
    private final    DynamicContext<Member> context;
    private volatile SubDomain              domain;
    private volatile Enclave                enclave;

    public DemesneImpl(DemesneParameters parameters) throws GeneralSecurityException, IOException {
        assert parameters.hasContext() : "Must define context id";
        this.parameters = parameters;
        context = DynamicContext.newBuilder().setId(Digest.from(parameters.getContext())).build();
        final var commDirectory = commDirectory();
        var outerContextAddress = commDirectory.resolve(parameters.getParent()).toFile();

        outer = outerFrom(outerContextAddress);
        final var pwd = new byte[64];
        final var entropy = SecureRandom.getInstanceStrong();
        entropy.nextBytes(pwd);
        final var password = Hex.hexChars(pwd);
        final Supplier<char[]> passwordProvider = () -> password;
        final var keystore = KeyStore.getInstance("JCEKS");

        keystore.load(null, password);

        kerl = kerlFrom(outerContextAddress);
        Stereotomy stereotomy = new StereotomyImpl(new JksKeyStore(keystore, passwordProvider), kerl, entropy);

        thoth = new Thoth(stereotomy);
    }

    @Override
    public boolean active() {
        final var current = domain;
        return current != null && current.active();
    }

    @Override
    public void commit(EventCoords coordinates) {
        thoth.commit(EventCoordinates.from(coordinates));
        final var commDirectory = commDirectory();
        var outerContextAddress = commDirectory.resolve(parameters.getParent()).toFile();

        context.activate(thoth.member());

        log.info("Creating Demesne: {} bridge: {} on: {}", context.getId(), outerContextAddress,
                 thoth.member().getId());

        enclave = new Enclave(thoth.member(), new DomainSocketAddress(outerContextAddress),
                              new DomainSocketAddress(commDirectory.resolve(parameters.getPortal()).toFile()),
                              this::registerContext);
        domain = subdomainFrom(parameters, thoth.member(), context);
    }

    @Override
    public SelfAddressingIdentifier getId() {
        return thoth.identifier();
    }

    @Override
    public DelegatedInceptionEvent inception(Ident id, Builder<SelfAddressingIdentifier> specification) {
        var identifier = (SelfAddressingIdentifier) Identifier.from(id);
        return thoth.inception(identifier, specification);
    }

    @Override
    public DelegatedRotationEvent rotate(RotationSpecification.Builder specification) {
        return thoth.rotate(specification);
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        final var current = domain;
        if (current == null) {
            log.error("Inception has not occurred");
        } else {
            current.start();
        }
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        final var current = domain;
        if (current != null) {
            current.stop();
        }
    }

    @Override
    public void viewChange(Digest viewId, List<EventCoordinates> joining, List<Digest> leaving) {
        final var current = domain;
        joining.forEach(coords -> ((DynamicContext) current.getContext()).activate(
        new IdentifierMember(coords.getIdentifier().getDigest(kerl.getDigestAlgorithm()),
                             new KerlVerifier<>(coords.getIdentifier(), kerl))));
        leaving.forEach(id -> ((DynamicContext) current.getContext()).remove(id));
    }

    private Path commDirectory() {
        return Path.of(
        parameters.getCommDirectory().isEmpty() ? System.getProperty("user.home") : parameters.getCommDirectory());
    }

    private CachingKERL kerlFrom(File address) {
        Digest kerlContext = context.getId();
        final var serverAddress = new DomainSocketAddress(address);
        log.info("Kerl context: {} address: {}", kerlContext, serverAddress);
        return new CachingKERL(f -> {
            ManagedChannel channel = null;
            try {
                channel = NettyChannelBuilder.forAddress(serverAddress)
                                             .executor(executor)
                                             .intercept(clientInterceptor(kerlContext))
                                             .eventLoopGroup(eventLoopGroup)
                                             .channelType(channelType)
                                             .keepAliveTime(1, TimeUnit.SECONDS)
                                             .usePlaintext()
                                             .build();
                var stub = KERLServiceGrpc.newBlockingStub(channel);
                return f.apply(new KERLAdapter(new CommonKERLClient(stub, null), DigestAlgorithm.DEFAULT));
            } catch (Throwable t) {
                return f.apply(null);
            } finally {
                if (channel != null) {
                    channel.shutdown();
                }
            }
        });
    }

    private OuterContextClient outerFrom(File address) {
        return new OuterContextClient(NettyChannelBuilder.forAddress(new DomainSocketAddress(address))
                                                         .executor(executor)
                                                         .intercept(clientInterceptor(context.getId()))
                                                         .eventLoopGroup(eventLoopGroup)
                                                         .channelType(channelType)
                                                         .usePlaintext()
                                                         .build(), null);
    }

    private void registerContext(Digest ctxId) {
        outer.register(
        SubContext.newBuilder().setEnclave(context.getId().toDigeste()).setContext(ctxId.toDigeste()).build());
    }

    private RuntimeParameters.Builder runtimeParameters(DemesneParameters parameters, ControlledIdentifierMember member,
                                                        DynamicContext<Member> context) {
        final var current = enclave;
        return RuntimeParameters.newBuilder()
                                .setCommunications(current.router())
                                .setKerl(member::kerl)
                                .setContext(context)
                                .setFoundation(parameters.getFoundation());
    }

    private SubDomain subdomainFrom(DemesneParameters parameters, ControlledIdentifierMember member,
                                    DynamicContext<Member> context) {
        final var gossipInterval = parameters.getGossipInterval();
        final var interval = gossipInterval.getSeconds() != 0 || gossipInterval.getNanos() != 0 ? Duration.ofSeconds(
        gossipInterval.getSeconds(), gossipInterval.getNanos()) : DEFAULT_GOSSIP_INTERVAL;
        return new SubDomain(member, Parameters.newBuilder(), runtimeParameters(parameters, member, context),
                             parameters.getMaxTransfer(), interval, parameters.getFalsePositiveRate());
    }
}
