/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.io.IOException;
import java.util.concurrent.Executor;

import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionFactory;
import com.salesforce.apollo.comm.grpc.MtlsClient;
import com.salesforce.apollo.comm.grpc.MtlsServer;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.ManagedChannel;
import io.grpc.util.MutableHandlerRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class MtlsRouter extends Router {

    public static class MtlsServerConnectionFactory implements ServerConnectionFactory {
        private final EndpointProvider epProvider;

        public MtlsServerConnectionFactory(EndpointProvider epProvider) {
            this.epProvider = epProvider;
        }

        @Override
        public ManagedChannel connectTo(Member to, Member from) {
            return new MtlsClient(epProvider.addressFor(to), epProvider.getClientAuth(), epProvider.getAlias(),
                    epProvider.getCertificate(), epProvider.getPrivateKey(), epProvider.getValiator()).getChannel();
        }
    }

    private final EndpointProvider epProvider;
    private final MtlsServer       server;

    public MtlsRouter(ServerConnectionCache.Builder builder, EndpointProvider ep, Executor executor) {
        this(builder, ep, new MutableHandlerRegistry(), executor);
    }

    public MtlsRouter(ServerConnectionCache.Builder builder, EndpointProvider ep, MutableHandlerRegistry registry,
            Executor executor) {
        super(builder.setFactory(new MtlsServerConnectionFactory(ep)).build(), registry);
        epProvider = ep;
        this.server = new MtlsServer(epProvider.getBindAddress(), epProvider.getClientAuth(), epProvider.getAlias(),
                epProvider.getCertificate(), epProvider.getPrivateKey(), epProvider.getValiator(), registry, executor);
    }

    @Override
    public ClientIdentity getClientIdentityProvider() {
        return server;
    }

    @Override
    public void start() {
        try {
            server.start();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot start server", e);
        }
    }

    @Override
    public void close() {
        server.stop();
        super.close();
    }
}
