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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.demesne.proto.ViewChange;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.model.demesnes.Demesne;
import com.salesforce.apollo.model.demesnes.DemesneImpl;
import com.salesforce.apollo.stereotomy.EventCoordinates;

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
    static {
        System.setProperty(".level", "CONFIG");
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_createIsolate", builtin = CEntryPoint.Builtin.CREATE_ISOLATE)
    public static native IsolateThread createIsolate();

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_active")
    private static boolean active(JNIEnvironment jniEnv, JClass clazz,
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

    private static void configureLogging(final DemesneParameters parameters) {
        final var loggingConfig = parameters.getLoggingConfig();
        File configFile = new File(loggingConfig);
        if (!loggingConfig.isBlank() || configFile.exists()) {
            System.err.println("Using logging configuration: " + configFile.getAbsolutePath());
            try {
                final var config = new FileInputStream(configFile);
                LogManager.getLogManager().updateConfiguration(config, s -> (o, n) -> n);
            } catch (FileNotFoundException e) {
                System.err.println("No logging configuration found: " + configFile.getAbsolutePath());
                System.setProperty(".level", "INFO");
            } catch (SecurityException | IOException e) {
                System.setProperty(".level", "FINEST");
                log.error("Unable to initialize logging configuration", e);
                throw new IllegalStateException("Unable to initialize logging configuration", e);
            }
        } else {
            System.err.println("No logging configuration");
            System.setProperty(".level", "INFO");
        }

        log.trace("Testing");
    }

    private static EventCoordinates coords(byte[] coords) {
        try {
            return EventCoordinates.from(EventCoords.parseFrom(coords));
        } catch (InvalidProtocolBufferException e) {
            log.error("Invalid digest: {}", coords);
            throw new IllegalArgumentException("Invalid digest: " + coords, e);
        }
    }

    private static Digest digest(byte[] digest) {
        try {
            return Digest.from(Digeste.parseFrom(digest));
        } catch (InvalidProtocolBufferException e) {
            log.error("Invalid digest: {}", digest);
            throw new IllegalArgumentException("Invalid digest: " + digest, e);
        }
    }

    private static void launch(JNIEnvironment jniEnv, ByteBuffer data, char[] password,
                               JClass clazz) throws GeneralSecurityException, IOException {
        final var parameters = DemesneParameters.parseFrom(data);
        configureLogging(parameters);
        launch(jniEnv, parameters, password, clazz);
    }

    private static void launch(JNIEnvironment jniEnv, DemesneParameters parameters, char[] password,
                               JClass clazz) throws GeneralSecurityException, IOException {
        if (demesne.get() != null) {
            return;
        }
        final var pretending = new DemesneImpl(parameters, password);
        if (!demesne.compareAndSet(null, pretending)) {
            return;
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_launch")
    private static boolean launch(JNIEnvironment jniEnv, JClass clazz, @CEntryPoint.IsolateThreadContext long isolateId,
                                  JByteArray parameters, int parametersLen, JByteArray pwd, int pwdLen) {
        var parametersBuff = CTypeConversion.asByteBuffer(jniEnv.getFunctions()
                                                                .getGetByteArrayElements()
                                                                .call(jniEnv, parameters, false),
                                                          parametersLen);
        var passwordBuff = CTypeConversion.asByteBuffer(jniEnv.getFunctions()
                                                              .getGetByteArrayElements()
                                                              .call(jniEnv, pwd, false),
                                                        pwdLen);
        var password = StandardCharsets.UTF_8.decode(passwordBuff);
        log.trace("Launch Demesne Isolate: {}", isolateId);
        try {
            launch(jniEnv, parametersBuff, password.array(), clazz);
            return true;
        } catch (InvalidProtocolBufferException e) {
            log.error("Cannot launch demesne", e);
            return false;
        } catch (GeneralSecurityException e) {
            log.error("Cannot launch demesne", e);
            return false;
        } catch (IOException e) {
            log.error("Cannot launch demesne", e);
            return false;
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_start")
    private static void start(JNIEnvironment jniEnv, JClass clazz,
                              @CEntryPoint.IsolateThreadContext long isolateId) throws GeneralSecurityException {
        final Demesne d = demesne.get();
        if (d != null) {
            d.start();
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_stop")
    private static void stop(JNIEnvironment jniEnv, JClass clazz,
                             @CEntryPoint.IsolateThreadContext long isolateId) throws GeneralSecurityException {
        final Demesne d = demesne.get();
        if (d != null) {
            d.stop();
        }
    }

    @CEntryPoint(name = "Java_com_salesforce_apollo_model_demesnes_JniBridge_viewChange")
    private static boolean viewChange(JNIEnvironment jniEnv, JClass clazz,
                                      @CEntryPoint.IsolateThreadContext long isolateId, JByteArray vc, int size) {
        var buff = CTypeConversion.asByteBuffer(jniEnv.getFunctions().getGetByteArrayElements().call(jniEnv, vc, false),
                                                size);
        final Demesne current = demesne.get();
        if (current == null) {
            log.warn("No Demesne created");
            return false;
        }
        ViewChange viewChange;
        try {
            viewChange = ViewChange.parseFrom(buff);
        } catch (InvalidProtocolBufferException e) {
            log.error("Unable to parse DemesnesViewChange", e);
            return false;
        }
        current.viewChange(Digest.from(viewChange.getView()),
                           viewChange.getJoiningList().stream().map(j -> EventCoordinates.from(j)).toList(),
                           viewChange.getLeavingList().stream().map(d -> Digest.from(d)).toList());
        return true;
    }
}
