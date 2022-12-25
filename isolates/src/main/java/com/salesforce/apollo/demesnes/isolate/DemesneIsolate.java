/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demesnes.isolate;

import static com.salesforce.apollo.comm.grpc.DomainSockets.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getEventLoopGroup;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.word.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.model.Demesne;
import com.salesforce.apollo.model.DemesneImpl;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

/**
 * GraalVM Isolate for the Apollo SubDomain stack
 *
 * @author hal.hildebrand
 *
 */
public class DemesneIsolate {

    private static final Class<? extends Channel>     channelType    = getChannelType();
    private static final AtomicReference<DemesneImpl> demesne        = new AtomicReference<>();
    private static final EventLoopGroup               eventLoopGroup = getEventLoopGroup();
    private static final Logger                       log            = LoggerFactory.getLogger(DemesneIsolate.class);

    @CEntryPoint(name = "Java_com_salesforce_apollo_domain_Demesne_createIsolate", builtin = CEntryPoint.Builtin.CREATE_ISOLATE)
    public static native IsolateThread createIsolate();

    @CEntryPoint(name = "Java_com_salesforce_apollo_domain_Demesne_active")
    private static boolean active(Pointer jniEnv, Pointer clazz,
                                  @CEntryPoint.IsolateThreadContext long isolateId) throws GeneralSecurityException {
        final Demesne d = demesne.get();
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
        final var pretending = new DemesneImpl(parameters, pwd);
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
        final Demesne d = demesne.get();
        if (d != null) {
            d.start();
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_domain_Demesne_stop")
    private static void stop(Pointer jniEnv, Pointer clazz,
                             @CEntryPoint.IsolateThreadContext long isolateId) throws GeneralSecurityException {
        final Demesne d = demesne.get();
        if (d != null) {
            d.stop();
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_domain_Demesne_viewChange")
    private static void viewChange(Pointer jniEnv, Pointer clazz, @CEntryPoint.IsolateThreadContext long isolateId,
                                   byte[] viewId, byte[][] joins,
                                   byte[][] leaves) throws GeneralSecurityException, IOException {

        final Demesne current = demesne.get();
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
}
