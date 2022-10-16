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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.membership.Member;

import io.grpc.ManagedChannel;
import io.grpc.ServerBuilder;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * Local "service mesh" for in process Isolate Enclaves. The Portal provides the
 * externally visible GRPC endpoint that all enclaves are multiplexed through.
 * The Portal also serves as the exit point from the process that all Isolate
 * Enclaves use to talk to each other and Enclaves in other processes
 *
 * @author hal.hildebrand
 *
 */
public class Portal<To extends Member> {

    Map<String, DomainSocketAddress>                        routes         = new ConcurrentHashMap<>();
    private final DomainSocketAddress                       bridge;
    private final Class<? extends io.netty.channel.Channel> channelType    = getChannelType();
    private final EventLoopGroup                            eventLoopGroup = getEventLoopGroup();
    private final Demultiplexer                             inbound;
    private final Duration                                  keepAlive;
    private final Demultiplexer                             outbound;

    public Portal(ServerBuilder<?> inbound, Function<String, ManagedChannel> outbound, DomainSocketAddress bridge,
                  Executor executor, Duration keepAlive) {
        this.inbound = new Demultiplexer(inbound, Router.METADATA_CONTEXT_KEY, d -> handler(routes.get(d)));
        this.outbound = new Demultiplexer(NettyServerBuilder.forAddress(bridge)
                                                            .executor(executor)
                                                            .protocolNegotiator(new DomainSocketNegotiator())
                                                            .channelType(getServerDomainSocketChannelClass())
                                                            .workerEventLoopGroup(getEventLoopGroup())
                                                            .bossEventLoopGroup(getEventLoopGroup())
                                                            .intercept(new DomainSocketServerInterceptor()),
                                          Router.METADATA_TARGET_KEY, outbound);
        this.bridge = bridge;
        this.keepAlive = keepAlive;
    }

    public void close(Duration await) {
        inbound.close(await);
        outbound.close(await);
    }

    /**
     * Remove the route if it was previously associated with the supplied address.
     *
     * @param route  - the mapped route
     * @param target - the expected target for the route
     * @return true if the route was mapped to the target, false otherwise
     */
    public boolean deregister(String route, DomainSocketAddress target) {
        return routes.remove(route, target);
    }

    /**
     * 
     * @return the domain socket address for outbound demultiplexing
     */
    public DomainSocketAddress getBridge() {
        return bridge;
    }

    /**
     * Map the route to the target if the route has not already been established
     *
     * @param route  - the mapped route
     * @param target - the target of the route
     * @return true if the route has not been previously mapped, false otherwise
     */
    public boolean register(String route, DomainSocketAddress target) {
        return routes.putIfAbsent(route, target) == null;
    }

    public void start() throws IOException {
        outbound.start();
        inbound.start();
    }

    private ManagedChannel handler(DomainSocketAddress address) {
        return NettyChannelBuilder.forAddress(address)
                                  .eventLoopGroup(eventLoopGroup)
                                  .channelType(channelType)
                                  .keepAliveTime(keepAlive.toNanos(), TimeUnit.NANOSECONDS)
                                  .usePlaintext()
                                  .build();
    }
}
