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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.word.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc;
import com.salesfoce.apollo.utils.proto.Digeste;
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
import com.salesforce.apollo.model.SubDomain;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.caching.CachingKERL;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
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
public class DemesneIsolate {

    private static final Class<? extends Channel>        channelType    = getChannelType();
    private static final AtomicReference<DemesneIsolate> demesne        = new AtomicReference<>();
    private static final EventLoopGroup                  eventLoopGroup = getEventLoopGroup();
    private static final Logger                          log            = LoggerFactory.getLogger(DemesneIsolate.class);

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

    private static Digest digest(byte[] digest) {
        try {
            return Digest.from(Digeste.parseFrom(digest));
        } catch (InvalidProtocolBufferException e) {
            log.error("Invalid digest: {}", digest);
            throw new IllegalArgumentException("Invalid digest: " + digest, e);
        }
    }

    private static String launch(Pointer jniEnv, ByteBuffer paramBytes, Pointer clazz,
                                 char[] pwd) throws GeneralSecurityException, IOException {
        try {
            return launch(jniEnv, DemesneParameters.parseFrom(paramBytes), clazz, pwd);
        } finally {
            Arrays.fill(pwd, ' ');
        }
    }

    private static String launch(Pointer jniEnv, DemesneParameters parameters, Pointer clazz,
                                 char[] pwd) throws GeneralSecurityException, IOException {
        if (demesne.get() != null) {
            return null;
        }
        final var pretending = new DemesneIsolate(jniEnv, parameters, clazz, pwd);
        if (!demesne.compareAndSet(null, pretending)) {
            return null;
        }
        return pretending.getInbound();
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_domain_Demesne_launch")
    private static void launch(Pointer jniEnv, Pointer clazz, @CEntryPoint.IsolateThreadContext long isolateId,
                               byte[] parameters, char[] ksPassword) throws GeneralSecurityException, IOException {
        log.info("Launch isolate: {} parameters: {} pswd: {}", isolateId, parameters.length, ksPassword.length);
        try {
            launch(jniEnv, ByteBuffer.wrap(parameters), clazz, ksPassword);
        } catch (InvalidProtocolBufferException e) {
            log.error("Cannot launch demesne", e);
        } finally {
            Arrays.fill(ksPassword, ' ');
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_domain_Demesne_start")
    private static void start(Pointer jniEnv, Pointer clazz,
                              @CEntryPoint.IsolateThreadContext long isolateId) throws GeneralSecurityException {
        final var d = demesne.get();
        if (d != null) {
            d.start();
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_domain_Demesne_stop")
    private static void stop(Pointer jniEnv, Pointer clazz,
                             @CEntryPoint.IsolateThreadContext long isolateId) throws GeneralSecurityException {
        final var d = demesne.get();
        if (d != null) {
            d.stop();
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_domain_Demesne_viewChange")
    private static void viewChange(Pointer jniEnv, Pointer clazz, @CEntryPoint.IsolateThreadContext long isolateId,
                                   byte[] viewId, byte[][] joins,
                                   byte[][] leaves) throws GeneralSecurityException, IOException {

        final var current = demesne.get();
        if (current == null) {
            return;
        }
        current.viewChange(digest(viewId),
                           IntStream.range(0, joins.length)
                                    .mapToObj(i -> digest(joins[i]))
                                    .filter(d -> d != null)
                                    .toList(),
                           IntStream.range(0, leaves.length)
                                    .mapToObj(i -> digest(leaves[i]))
                                    .filter(d -> d != null)
                                    .toList());
    }

    private final Pointer       clazz;
    private final SubDomain     domain;
    private final String        inbound;
    private final Pointer       jniEnv;
    private final KERL          kerl;
    private final AtomicBoolean started = new AtomicBoolean();
    private final Stereotomy    stereotomy;

    DemesneIsolate(Pointer jniEnv, DemesneParameters parameters, Pointer clazz,
                   char[] pwd) throws GeneralSecurityException, IOException {
        this.jniEnv = jniEnv;
        this.clazz = clazz;
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
                return f.apply(new DelegatedKERL(new CommonKERLClient(stub, null), DigestAlgorithm.DEFAULT));
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

        domain = new SubDomain(member, Parameters.newBuilder(),
                               RuntimeParameters.newBuilder()
                                                .setCommunications(new Enclave(member, new DomainSocketAddress(address),
                                                                               Executors.newFixedThreadPool(2,
                                                                                                            Thread.ofVirtual()
                                                                                                                  .factory()),
                                                                               new DomainSocketAddress(commDirectory.resolve(parameters.getOutbound())
                                                                                                                    .toFile()),
                                                                               keepAlive, ctxId -> {
                                                                                   registerContext(ctxId);
                                                                               }).router(Executors.newFixedThreadPool(2,
                                                                                                                      Thread.ofVirtual()
                                                                                                                            .factory())))
                                                .setExec(Executors.newFixedThreadPool(2, Thread.ofVirtual().factory()))
                                                .setScheduler(Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual()
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
                               new TransactionConfiguration(Executors.newFixedThreadPool(2,
                                                                                         Thread.ofVirtual().factory()),
                                                            Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual()
                                                                                                             .factory())));
    }

    public boolean active() {
        return domain == null ? false : domain.active();
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        domain.start();
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        domain.stop();
    }

    private String getInbound() {
        return inbound;
    }

    private void registerContext(Digest ctxId) {
        // TODO Auto-generated method stub
    }

    private void viewChange(Digest viewId, List<Digest> joining, List<Digest> leaving) {
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
}
