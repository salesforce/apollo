/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import static com.salesforce.apollo.comm.grpc.DomainSockets.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getEventLoopGroup;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getServerDomainSocketChannelClass;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.QualifiedBase64;
import com.salesforce.apollo.membership.Member;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
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
    private static final Executor executor = Executors.newVirtualThreadPerTaskExecutor();
    private final static Class<? extends io.netty.channel.Channel> channelType = getChannelType();

    private final String         agent;
    private final EventLoopGroup eventLoopGroup = getEventLoopGroup();
    private final Demultiplexer  inbound;
    private final Duration       keepAlive;
    private final Demultiplexer  outbound;

    public Portal(Digest agent, ServerBuilder<?> inbound, Function<String, ManagedChannel> outbound,
                  DomainSocketAddress bridge,  Duration keepAlive,
                  Function<String, DomainSocketAddress> router) {
        this.inbound = new Demultiplexer(inbound, Router.METADATA_CONTEXT_KEY, d -> handler(router.apply(d)));
        this.outbound = new Demultiplexer(NettyServerBuilder.forAddress(bridge)
                                                            .executor(executor)
                                                            .protocolNegotiator(new DomainSocketNegotiator())
                                                            .channelType(getServerDomainSocketChannelClass())
                                                            .workerEventLoopGroup(getEventLoopGroup())
                                                            .bossEventLoopGroup(getEventLoopGroup())
                                                            .intercept(new DomainSocketServerInterceptor()),
                                          Router.METADATA_TARGET_KEY, outbound);
        this.keepAlive = keepAlive;
        this.agent = QualifiedBase64.qb64(agent);
    }

    public void close(Duration await) {
        inbound.close(await);
        outbound.close(await);
    }

    public void start() throws IOException {
        outbound.start();
        inbound.start();
    }

    private ManagedChannel handler(DomainSocketAddress address) {
        var clientInterceptor = new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(Router.METADATA_AGENT_KEY, agent);
                        super.start(responseListener, headers);
                    }
                };
            }
        };
        return NettyChannelBuilder.forAddress(address)
                                  .eventLoopGroup(eventLoopGroup)
                                  .channelType(channelType)
                                  .keepAliveTime(keepAlive.toNanos(), TimeUnit.NANOSECONDS)
                                  .intercept(clientInterceptor)
                                  .usePlaintext()
                                  .build();
    }
}
