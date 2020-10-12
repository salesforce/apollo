/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.io.IOException;

import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionCacheBuilder;
import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionFactory;
import com.salesforce.apollo.comm.grpc.MtlsClient;
import com.salesforce.apollo.comm.grpc.MtlsServer;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;

/**
 * @author hal.hildebrand
 *
 */
public class MtlsCommunications implements Communications {

    public class MtlsServerConnectionFactory implements ServerConnectionFactory {

        @Override
        public ManagedChannel connectTo(Member to, Member from) {
            return new MtlsClient(epProvider.addressFor(to), epProvider.getClientAuth(), epProvider.getAlias(),
                    epProvider.getCertificate(), epProvider.getPrivateKey(), epProvider.getValiator()).getChannel();
        }
    }

    private final EndpointProvider            epProvider;
    private final ServerConnectionCache       cache;
    private final MtlsServerConnectionFactory factory = new MtlsServerConnectionFactory();
    private final MtlsServer                  server;

    public MtlsCommunications(ServerConnectionCacheBuilder builder, EndpointProvider ep) {
        epProvider = ep;
        this.server = new MtlsServer(epProvider.getBindAddress(), epProvider.getClientAuth(), epProvider.getAlias(),
                epProvider.getCertificate(), epProvider.getPrivateKey(), epProvider.getValiator());
        this.cache = builder.setFactory(factory).build();
    }

    @Override
    public void close() {
        server.stop();
    }

    @Override
    public <T> CommonCommunications<T> create(Member member, CreateClientCommunications<T> createFunction,
                                              BindableService service) {
        server.bind(service);
        return new CommonCommunications<T>(cache, createFunction);
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

}
