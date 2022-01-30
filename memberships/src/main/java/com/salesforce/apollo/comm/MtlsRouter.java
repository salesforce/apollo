/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.io.IOException;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionFactory;
import com.salesforce.apollo.comm.grpc.MtlsClient;
import com.salesforce.apollo.comm.grpc.MtlsServer;
import com.salesforce.apollo.comm.grpc.ServerContextSupplier;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.ManagedChannel;
import io.grpc.util.MutableHandlerRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class MtlsRouter extends Router {
    private static final Logger log = LoggerFactory.getLogger(MtlsRouter.class);

    public static class MtlsServerConnectionFactory implements ServerConnectionFactory {
        private final EndpointProvider epProvider;

        public MtlsServerConnectionFactory(EndpointProvider epProvider) {
            this.epProvider = epProvider;
        }

        @Override
        public ManagedChannel connectTo(Member to, SigningMember from) {
            return new MtlsClient(epProvider.addressFor(to), epProvider.getClientAuth(), epProvider.getAlias(), from,
                                  epProvider.getValiator()).getChannel();
        }
    }

    private final EndpointProvider epProvider;
    private final MtlsServer       server;

    public MtlsRouter(ServerConnectionCache.Builder builder, EndpointProvider ep, ServerContextSupplier supplier,
                      Executor executor) {
        this(builder, ep, supplier, new MutableHandlerRegistry(), executor);
    }

    public MtlsRouter(ServerConnectionCache.Builder builder, EndpointProvider ep, ServerContextSupplier supplier,
                      MutableHandlerRegistry registry, Executor executor) {
        super(builder.setFactory(new MtlsServerConnectionFactory(ep)).build(), registry);
        epProvider = ep;
        this.server = new MtlsServer(epProvider.getBindAddress(), epProvider.getClientAuth(), epProvider.getAlias(),
                                     supplier, epProvider.getValiator(), registry, executor);
    }

    @Override
    public ClientIdentity getClientIdentityProvider() {
        return server;
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        try {
            server.start();
        } catch (IOException e) {
            log.error("Cannot start server", e);
            throw new IllegalStateException("Cannot start server", e);
        }
    }

    @Override
    public void close() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        server.stop();
        super.close();
    }
}
