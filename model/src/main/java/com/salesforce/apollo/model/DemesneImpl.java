/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import static com.salesforce.apollo.comm.grpc.DomainSockets.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getEventLoopGroup;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc;
import com.salesforce.apollo.archipelago.Enclave;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.membership.stereotomy.IdentifierMember;
import com.salesforce.apollo.model.Domain.TransactionConfiguration;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.caching.CachingKERL;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.jks.JksKeyStore;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.CommonKERLClient;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLAdapter;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * Isolate for the Apollo SubDomain stack
 *
 * @author hal.hildebrand
 *
 */
public class DemesneImpl implements Demesne {

    private static final Class<? extends Channel> channelType    = getChannelType();
    private static final EventLoopGroup           eventLoopGroup = getEventLoopGroup();
    private static final Logger                   log            = LoggerFactory.getLogger(DemesneImpl.class);

    private static ClientInterceptor clientInterceptor(Digest ctx) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, io.grpc.Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(Router.METADATA_CONTEXT_KEY, qb64(ctx));
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    private final SubDomain     domain;
    private final String        inbound;
    private final KERL          kerl;
    private final AtomicBoolean started = new AtomicBoolean();
    private final Stereotomy    stereotomy;

    public DemesneImpl(DemesneParameters parameters, char[] pwd) throws GeneralSecurityException, IOException {
        final var kpa = parameters.getKeepAlive();
        Duration keepAlive = !kpa.isInitialized() ? Duration.ofMillis(1)
                                                  : Duration.ofSeconds(kpa.getSeconds(), kpa.getNanos());
        final var commDirectory = Path.of(parameters.getCommDirectory().isEmpty() ? System.getProperty("user.home")
                                                                                  : parameters.getCommDirectory());
        final var address = commDirectory.resolve(UUID.randomUUID().toString()).toFile();
        inbound = address.getCanonicalPath();

        final var password = Arrays.copyOf(pwd, pwd.length);
        Arrays.fill(pwd, ' ');

        final var keystore = KeyStore.getInstance("JKS");
        keystore.load(parameters.getKeyStore().newInput(), password);
        Digest kerlContext = Digest.from(parameters.getKerlContext());
        kerl = new CachingKERL(f -> {
            var channel = NettyChannelBuilder.forAddress(new DomainSocketAddress(commDirectory.resolve(parameters.getKerlService())
                                                                                              .toFile()))
                                             .intercept(clientInterceptor(kerlContext))
                                             .eventLoopGroup(eventLoopGroup)
                                             .channelType(channelType)
                                             .keepAliveTime(1, TimeUnit.SECONDS)
                                             .usePlaintext()
                                             .build();
            try {
                var stub = KERLServiceGrpc.newFutureStub(channel);
                return f.apply(new KERLAdapter(new CommonKERLClient(stub, null), DigestAlgorithm.DEFAULT));
            } finally {
                channel.shutdown();
            }
        });
        stereotomy = new StereotomyImpl(new JksKeyStore(keystore, () -> password), kerl,
                                        SecureRandom.getInstanceStrong());

        ControlledIdentifierMember member;
        try {
            member = new ControlledIdentifierMember(stereotomy.controlOf((SelfAddressingIdentifier) Identifier.from(parameters.getMember()))
                                                              .get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            domain = null;
            return;
        } catch (ExecutionException e) {
            throw new IllegalStateException("Invalid state", e.getCause());
        }

        var context = Context.newBuilder().build();
        context.activate(member);
        var exec = Executors.newVirtualThreadPerTaskExecutor();

        domain = new SubDomain(member, Parameters.newBuilder(),
                               RuntimeParameters.newBuilder()
                                                .setCommunications(new Enclave(member, new DomainSocketAddress(address),
                                                                               exec,
                                                                               new DomainSocketAddress(commDirectory.resolve(parameters.getOutbound())
                                                                                                                    .toFile()),
                                                                               keepAlive, ctxId -> {
                                                                                   registerContext(ctxId);
                                                                               }).router(exec))
                                                .setExec(exec)
                                                .setScheduler(Executors.newScheduledThreadPool(5,
                                                                                               Thread.ofVirtual()
                                                                                                     .factory()))
                                                .setKerl(() -> {
                                                    try {
                                                        return member.kerl().get();
                                                    } catch (InterruptedException e) {
                                                        Thread.currentThread().interrupt();
                                                        return null;
                                                    } catch (ExecutionException e) {
                                                        throw new IllegalStateException(e.getCause());
                                                    }
                                                })
                                                .setContext(context)
                                                .setFoundation(parameters.getFoundation()),
                               new TransactionConfiguration(exec,
                                                            Executors.newScheduledThreadPool(5, Thread.ofVirtual()
                                                                                                      .factory())));
    }

    @Override
    public boolean active() {
        return domain == null ? false : domain.active();
    }

    public String getInbound() {
        return inbound;
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        domain.start();
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        domain.stop();
    }

    @Override
    public void viewChange(Digest viewId, List<Digest> joining, List<Digest> leaving) {
        joining.stream().filter(id -> domain.getContext().isMember(id)).forEach(id -> {
            EstablishmentEvent keyEvent;
            try {
                keyEvent = kerl.getKeyState(new SelfAddressingIdentifier(id))
                               .thenApply(ks -> ks.getLastEstablishmentEvent())
                               .thenCompose(coords -> kerl.getKeyEvent(coords))
                               .thenApply(ke -> (EstablishmentEvent) ke)
                               .get();
                domain.activate(new IdentifierMember(keyEvent));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error("Error retrieving last establishment event for: {}", id, e.getCause());
            }
        });
        leaving.forEach(id -> domain.getContext().remove(id));
    }

    private void registerContext(Digest ctxId) {
        // TODO Auto-generated method stub
    }
}
