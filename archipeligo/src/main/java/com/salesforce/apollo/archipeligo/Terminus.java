/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getEventLoopGroup;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getServerDomainSocketChannelClass;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.salesforce.apollo.crypto.Digest;

import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * GRPC multiplexer and demultiplexer
 *
 * @author hal.hildebrand
 *
 */
public class Terminus {

    @SuppressWarnings("unused")
    private static final Metadata.Key<String> TARGET_METADATA_KEY = Metadata.Key.of("to.Endpoint",
                                                                                    Metadata.ASCII_STRING_MARSHALLER);

    @SuppressWarnings("unused")
    private final DomainSocketAddress              bridge;
    private final Map<Digest, DomainSocketAddress> enclaves = new ConcurrentSkipListMap<>();
    private final Server                           outboundBridge;
    private final Server                           server;
    private final AtomicBoolean                    started  = new AtomicBoolean();

    public Terminus(ServerBuilder<?> serverBuilder, DomainSocketAddress bridge) {
        serverBuilder.intercept(Router.serverInterceptor()).fallbackHandlerRegistry(new GrpcProxy() {
            @Override
            protected Channel getChannel() {
                var address = enclaves.get(Router.SERVER_CONTEXT_KEY.get());
                if (address == null) {
                    return null;
                }
                return handler(address);
            }
        }.newRegistry());

        this.bridge = bridge;
        server = serverBuilder.build();
        outboundBridge = NettyServerBuilder.forAddress(bridge)
                                           .protocolNegotiator(new DomainSocketNegotiator())
                                           .channelType(getServerDomainSocketChannelClass())
                                           .workerEventLoopGroup(getEventLoopGroup())
                                           .bossEventLoopGroup(getEventLoopGroup())
                                           .build();
    }

    public void close(Duration await) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        outboundBridge.shutdown();
        server.shutdown();
        try {
            server.awaitTermination(await.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            outboundBridge.awaitTermination(await.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void register(Digest ctx, DomainSocketAddress address) {
        enclaves.put(ctx, address);
    }

    public void start() throws IOException {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        outboundBridge.start();
        server.start();
    }

    private Channel handler(DomainSocketAddress address) {
        return NettyChannelBuilder.forAddress(address)
                                  .eventLoopGroup(getEventLoopGroup())
                                  .channelType(getChannelType())
                                  .keepAliveTime(1, TimeUnit.MILLISECONDS)
                                  .usePlaintext()
                                  .build();
    }
}
