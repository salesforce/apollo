/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.grpc.client.ConcurrencyLimitClientInterceptor;
import com.netflix.concurrency.limits.grpc.client.GrpcClientLimiterBuilder;
import com.netflix.concurrency.limits.grpc.client.GrpcClientRequestContext;
import com.salesforce.apollo.cryptography.ssl.CertificateValidator;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.ClientAuth;

import java.net.SocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.salesforce.apollo.comm.grpc.MtlsServer.forClient;

/**
 * @author hal.hildebrand
 */
public class MtlsClient {
    private static final Executor       exec = Executors.newVirtualThreadPerTaskExecutor();
    private final        ManagedChannel channel;

    public MtlsClient(SocketAddress address, ClientAuth clientAuth, String alias, ClientContextSupplier supplier,
                      CertificateValidator validator, Executor exec) {

        Limiter<GrpcClientRequestContext> limiter = new GrpcClientLimiterBuilder().blockOnLimit(false).build();
        channel = NettyChannelBuilder.forAddress(address)
                                     .executor(exec)
                                     .sslContext(supplier.forClient(clientAuth, alias, validator, MtlsServer.TL_SV1_3))
                                     .intercept(new ConcurrencyLimitClientInterceptor(limiter,
                                                                                      () -> Status.RESOURCE_EXHAUSTED.withDescription(
                                                                                      "Client side concurrency limit exceeded")))
                                     .build();

    }

    public MtlsClient(SocketAddress address, ClientAuth clientAuth, String alias, X509Certificate certificate,
                      PrivateKey privateKey, CertificateValidator validator) {

        Limiter<GrpcClientRequestContext> limiter = new GrpcClientLimiterBuilder().blockOnLimit(false).build();
        channel = NettyChannelBuilder.forAddress(address)
                                     .executor(exec)
                                     .sslContext(forClient(clientAuth, alias, certificate, privateKey, validator))
                                     .intercept(new ConcurrencyLimitClientInterceptor(limiter,
                                                                                      () -> Status.RESOURCE_EXHAUSTED.withDescription(
                                                                                      "Client side concurrency limit exceeded")))
                                     .build();

    }

    public ManagedChannel getChannel() {
        return channel;
    }

    public void stop() {
        channel.shutdown();
    }
}
