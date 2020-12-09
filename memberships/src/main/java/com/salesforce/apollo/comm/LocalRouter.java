/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionFactory;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

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
public class LocalRouter extends Router {

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
            return new Certificate[] { getCert() };
        }

        @Override
        public HashKey getFrom() {
            return Utils.getMemberId(getCert());
        }

    }

    public static ThreadLocal<X509Certificate> callCertificate = new ThreadLocal<>();
    public static final ThreadIdentity         LOCAL_IDENTITY  = new ThreadIdentity();

    private static final Logger log = LoggerFactory.getLogger(LocalRouter.class);

    private final HashKey id;
    private Server        server;

    public LocalRouter(HashKey id, ServerConnectionCache.Builder builder) {
        this(id, builder, new MutableHandlerRegistry());
    }

    public LocalRouter(HashKey id, ServerConnectionCache.Builder builder, MutableHandlerRegistry registry) {
        super(builder.setFactory(new LocalServerConnectionFactory()).build(), registry);
        this.id = id;

        server = InProcessServerBuilder.forName(id.b64Encoded())
                                       .directExecutor() // directExecutor is fine for local tests
                                       .fallbackHandlerRegistry(registry)
                                       .build();
    }

    @Override
    public void close() {
        server.shutdownNow();
        super.close();
    }

    @Override
    public void start() {
        try {
            server.start();
        } catch (IOException e) {
            log.error("Cannot start in process server for: " + id, e);
        }
        log.info("Starting server for: " + id);
    }

    @Override
    public ClientIdentity getClientIdentityProvider() {
        return LOCAL_IDENTITY;
    }

}
