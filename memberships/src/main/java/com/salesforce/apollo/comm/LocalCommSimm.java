/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import static com.salesforce.apollo.comm.grpc.MtlsServer.getMemberId;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionCacheBuilder;
import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionFactory;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.BindableService;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.util.MutableHandlerRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class LocalCommSimm implements Communications {
    private static final Logger logger = LoggerFactory.getLogger(LocalCommSimm.class);

    public static class LocalServerConnectionFactory implements ServerConnectionFactory {

        @Override
        public ManagedChannel connectTo(Member to, Member from) {
            ClientInterceptor clientInterceptor = new ClientInterceptor() {

                @Override
                public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                           CallOptions callOptions, Channel next) {
                    callCertificate.set(from.getCertificate());
                    return next.newCall(method, callOptions);
                }
            };
            return InProcessChannelBuilder.forName(to.getId().b64Encoded())
                                          .directExecutor()
                                          .intercept(clientInterceptor)
                                          .build();
        }
    }

    private static final class ThreadIdentity implements ClientIdentity {

        @Override
        public X509Certificate getCert() {
            return callCertificate.get();
        }

        @Override
        public Certificate[] getCerts() {
            return new Certificate[] { (Certificate) getCert() };
        }

        @Override
        public HashKey getFrom() {
            return getMemberId(getCert());
        }

    }

    public static ThreadLocal<X509Certificate> callCertificate = new ThreadLocal<>();
    public static final ThreadIdentity         LOCAL_IDENTITY  = new ThreadIdentity();

    private final ServerConnectionFactory factory  = new LocalServerConnectionFactory();
    private final MutableHandlerRegistry  registry = new MutableHandlerRegistry();
    private final Server                  server;
    private final ServerConnectionCache   cache;
    private final HashKey                 id;

    public LocalCommSimm(ServerConnectionCacheBuilder builder, HashKey id) {
        this.id = id;
        cache = builder.setFactory(factory).build();
        server = InProcessServerBuilder.forName(id.b64Encoded())
                                       .directExecutor() // directExecutor is fine for unit tests
                                       .fallbackHandlerRegistry(registry)
                                       .build();
        try {
            server.start();
        } catch (IOException e) {
            logger.error("Cannot start in process server for: " + id, e);
        }
        logger.info("Starting server for: " + id);
    }

    @Override
    public void close() {
        server.shutdownNow();
        cache.close();
    }

    @Override
    public <T> CommonCommunications<T> create(Member member, CreateClientCommunications<T> createFunction,
                                              BindableService service) {

        registry.addService(service);
        logger.info("Communications created for: " + member.getId());
        return new CommonCommunications<T>(cache, createFunction);
    }

    @Override
    public ClientIdentity getClientIdentityProvider() {
        return LOCAL_IDENTITY;
    }

    @Override
    public void start() {
        try {
            if (server.isShutdown() || server.isTerminated()) {
                server.start();
            }
        } catch (IOException ex) {
            logger.info("Server start failed for: " + id);
        }
    }
}
