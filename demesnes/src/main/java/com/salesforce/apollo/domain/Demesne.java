/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.domain;

import static com.salesforce.apollo.comm.grpc.DomainSockets.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getEventLoopGroup;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.word.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc;
import com.salesforce.apollo.archipelago.Enclave;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.Domain.TransactionConfiguration;
import com.salesforce.apollo.model.SubDomain;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.caching.CachingKERL;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.jks.JksKeyStore;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.CommonKERLClient;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.DelegatedKERL;

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
 * GraalVM Isolate for the Apollo SubDomain stack
 *
 * @author hal.hildebrand
 *
 */
public class Demesne {

    private static final Class<? extends Channel> channelType    = getChannelType();
    private static final AtomicReference<Demesne> demesne        = new AtomicReference<>();
    private static final EventLoopGroup           eventLoopGroup = getEventLoopGroup();
    private static final Logger                   log            = LoggerFactory.getLogger(Demesne.class);

    @CEntryPoint(name = "Java_com_salesforce_apollo_domain_Demesne_createIsolate", builtin = CEntryPoint.Builtin.CREATE_ISOLATE)
    public static native IsolateThread createIsolate();

    @CEntryPoint(name = "Java_com_salesforce_apollo_domain_Demesne_active")
    private static boolean active(Pointer jniEnv, Pointer clazz,
                                  @CEntryPoint.IsolateThreadContext long isolateId) throws GeneralSecurityException {
        final var d = demesne.get();
        return d == null ? false : d.active();
    }

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

    private static String launch(ByteBuffer paramBytes, char[] pwd) throws GeneralSecurityException, IOException {
        try {
            return launch(DemesneParameters.parseFrom(paramBytes), pwd);
        } finally {
            Arrays.fill(pwd, ' ');
        }
    }

    private static String launch(DemesneParameters parameters, char[] pwd) throws GeneralSecurityException,
                                                                           IOException {
        if (demesne.get() == null) {
            return null;
        }
        final var pretending = new Demesne(parameters, pwd);
        if (!demesne.compareAndSet(null, pretending)) {
            return null;
        }
        return pretending.getInbound();
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_domain_Demesne_launch")
    private static String launch(Pointer jniEnv, Pointer clazz, @CEntryPoint.IsolateThreadContext long isolateId,
                                 byte[] parameters, char[] ksPassword) throws GeneralSecurityException, IOException {

        String canonicalPath;
        try {
            canonicalPath = launch(ByteBuffer.wrap(parameters), ksPassword);
        } catch (InvalidProtocolBufferException e) {
            log.error("Cannot launch demesne", e);
            return null;
        } finally {
            Arrays.fill(ksPassword, ' ');
        }
        return canonicalPath;
    }

    private final SubDomain  domain;
    private final String     inbound;
    private final Stereotomy stereotomy;

    Demesne() {
        domain = null;
        stereotomy = null;
        inbound = null;
    }

    Demesne(DemesneParameters parameters, char[] pwd) throws GeneralSecurityException, IOException {
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

        stereotomy = new StereotomyImpl(new JksKeyStore(keystore, () -> password), new CachingKERL(f -> {
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
                return f.apply(new DelegatedKERL(new CommonKERLClient(stub, null), DigestAlgorithm.DEFAULT));
            } finally {
                channel.shutdown();
            }
        }), SecureRandom.getInstanceStrong());

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
        Context<? extends Member> context = Context.newBuilder().build();

        domain = new SubDomain(member, Parameters.newBuilder(),
                               RuntimeParameters.newBuilder()
                                                .setCommunications(new Enclave(member, new DomainSocketAddress(address),
                                                                               ForkJoinPool.commonPool(),
                                                                               new DomainSocketAddress(commDirectory.resolve(parameters.getOutbound())
                                                                                                                    .toFile()),
                                                                               keepAlive, ctxId -> {
                                                                                   registerContext(ctxId);
                                                                               }).router(ForkJoinPool.commonPool()))
                                                .setExec(ForkJoinPool.commonPool())
                                                .setScheduler(Executors.newSingleThreadScheduledExecutor())
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
                               new TransactionConfiguration(ForkJoinPool.commonPool(),
                                                            Executors.newSingleThreadScheduledExecutor()));
    }

    public boolean active() {
        return domain == null ? false : domain.active();
    }

    public void start() {
        domain.start();
    }

    private String getInbound() {
        return inbound;
    }

    private void registerContext(Digest ctxId) {
        // TODO Auto-generated method stub

    }
}
