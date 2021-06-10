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

import com.salesforce.apollo.crypto.ProviderUtils;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;

/**
 * @author hal.hildebrand
 *
 */
public class MtlsClient {

    private final ManagedChannel channel;

    public MtlsClient(SocketAddress address, ClientAuth clientAuth, String alias, ClientContextSupplier supplier,
            CertificateValidator validator) {

        channel = NettyChannelBuilder.forAddress(address)
                                     .sslContext(supplier.forClient(clientAuth, alias, validator,
                                                                    ProviderUtils.getProviderBCJSSE(),
                                                                    MtlsServer.TL_SV1_3))
                                     .withOption(ChannelOption.TCP_NODELAY, true)
                                     .build();

    }

    public MtlsClient(SocketAddress address, ClientAuth clientAuth, String alias, X509Certificate certificate,
            PrivateKey privateKey, CertificateValidator validator) {

        channel = NettyChannelBuilder.forAddress(address)
                                     .sslContext(forClient(clientAuth, alias, certificate, privateKey, validator))
                                     .withOption(ChannelOption.TCP_NODELAY, true)
                                     .build();

    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public void stop() {
        channel.shutdown();
    }
}
