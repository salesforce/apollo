/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import static com.salesforce.apollo.comm.grpc.MtlsServer.forClient;

import java.net.SocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;

/**
 * @author hal.hildebrand
 *
 */
public class MtlsClient {

    private final ManagedChannel channel;

    public MtlsClient(SocketAddress address, ClientAuth clientAuth, String alias, X509Certificate certificate,
            PrivateKey privateKey, X509Certificate ca) {

        channel = NettyChannelBuilder.forAddress(address)
                                     .sslContext(forClient(clientAuth, alias, certificate, privateKey, ca))
                                     .build();

    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public void stop() {
        channel.shutdown();
    }
}
