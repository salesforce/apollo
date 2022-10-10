/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo.dmux;

import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getEventLoopGroup;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.salesforce.apollo.archipeligo.Router;
import com.salesforce.apollo.crypto.Digest;

import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * GRPC inbound context based demultiplexer
 *
 * @author hal.hildebrand
 *
 */
public class Ingress {
    private final Map<Digest, DomainSocketAddress> enclaves = new ConcurrentSkipListMap<>();
    private final Server                           server;
    private final AtomicBoolean                    started  = new AtomicBoolean();

    public Ingress(ServerBuilder<?> serverBuilder) {
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

        server = serverBuilder.build();
    }

    public void close(Duration await) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        server.shutdown();
        try {
            server.awaitTermination(await.toNanos(), TimeUnit.NANOSECONDS);
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
