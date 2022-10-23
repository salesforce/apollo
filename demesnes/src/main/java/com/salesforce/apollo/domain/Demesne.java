/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.domain;

import static com.salesforce.apollo.comm.grpc.DomainSockets.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getEventLoopGroup;

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
import org.graalvm.nativeimage.ObjectHandle;
import org.graalvm.nativeimage.ObjectHandles;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc;
import com.salesforce.apollo.archipelago.Enclave;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.Domain.TransactionConfiguration;
import com.salesforce.apollo.model.SubDomain;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.caching.CachingKERL;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.jks.JksKeyStore;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.CommonKERLClient;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.DelegatedKERL;

import io.grpc.ManagedChannel;
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

    public static final class NativeImpl {
        @CEntryPoint(name = "Java_org_pkg_apinative_Native_createIsolate", builtin = CEntryPoint.Builtin.CREATE_ISOLATE)
        public static native IsolateThread createIsolate();
    }

    private static Class<? extends Channel>       channelType    = getChannelType();
    private static final AtomicReference<Demesne> demesne        = new AtomicReference<>();
    private static final EventLoopGroup           eventLoopGroup = getEventLoopGroup();
    private static final Logger                   log            = LoggerFactory.getLogger(Demesne.class);

    @CEntryPoint
    private static boolean active(@CEntryPoint.IsolateThreadContext IsolateThread domainContext) throws GeneralSecurityException {
        final var d = demesne.get();
        return d == null ? false : d.active();
    }

    @CEntryPoint
    private static ObjectHandle createByteBuffer(IsolateThread outer, Pointer address, int length) {
        ByteBuffer direct = CTypeConversion.asByteBuffer(address, length);
        ByteBuffer copy = ByteBuffer.allocate(length);
        copy.put(direct).rewind();
        return ObjectHandles.getGlobal().create(copy);
    }

    private static ManagedChannel handler(DomainSocketAddress address) {
        return NettyChannelBuilder.forAddress(address)
                                  .eventLoopGroup(eventLoopGroup)
                                  .channelType(channelType)
                                  .keepAliveTime(1, TimeUnit.SECONDS)
                                  .usePlaintext()
                                  .build();
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

    @CEntryPoint
    private static ObjectHandle launch(@CEntryPoint.IsolateThreadContext IsolateThread domainContext,
                                       IsolateThread controlContext, ObjectHandle parameters,
                                       ObjectHandle ksPassword) throws GeneralSecurityException, IOException {
        ByteBuffer paramBytes = ObjectHandles.getGlobal().get(parameters);
        ObjectHandles.getGlobal().destroy(parameters);

        char[] pwd = ObjectHandles.getGlobal().get(ksPassword);
        ObjectHandles.getGlobal().destroy(ksPassword);

        String canonicalPath;
        try {
            canonicalPath = launch(paramBytes, pwd);
        } catch (InvalidProtocolBufferException e) {
            log.error("Cannot launch demesne", e);
            return ObjectHandles.getGlobal().create(' ');
        } finally {
            Arrays.fill(pwd, ' ');
        }
        return ObjectHandles.getGlobal().create(canonicalPath);
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

        stereotomy = new StereotomyImpl(new JksKeyStore(KeyStore.getInstance("JKS"), () -> password),
                                        new CachingKERL(f -> {
                                            var channel = handler(new DomainSocketAddress(commDirectory.resolve(parameters.getKerlService())
                                                                                                       .toFile()));
                                            try {
                                                var stub = KERLServiceGrpc.newFutureStub(channel);
                                                return f.apply(new DelegatedKERL(new CommonKERLClient(stub, null),
                                                                                 DigestAlgorithm.DEFAULT));
                                            } finally {
                                                channel.shutdown();
                                            }
                                        }), SecureRandom.getInstanceStrong());

        @SuppressWarnings("unchecked")
        ControlledIdentifierMember member = new ControlledIdentifierMember((ControlledIdentifier<SelfAddressingIdentifier>) stereotomy.controlOf(Identifier.from(parameters.getMember())));
        Context<? extends Member> context = Context.newBuilder().build();

        domain = new SubDomain(member, Parameters.newBuilder(),
                               RuntimeParameters.newBuilder()
                                                .setCommunications(new Enclave(member, new DomainSocketAddress(address),
                                                                               ForkJoinPool.commonPool(),
                                                                               new DomainSocketAddress(commDirectory.resolve(parameters.getOutbound())
                                                                                                                    .toFile()),
                                                                               keepAlive, ctxId -> {
                                                                                   throw new UnsupportedOperationException("Not yet implemented");
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

    private String getInbound() {
        return inbound;
    }
}
