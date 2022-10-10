/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo.dmux;

import static com.salesforce.apollo.archipeligo.Router.SERVER_TARGET_KEY;
import static com.salesforce.apollo.archipeligo.Router.TARGET_METADATA_KEY;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getEventLoopGroup;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getServerDomainSocketChannelClass;
import static com.salesforce.apollo.crypto.QualifiedBase64.digest;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.archipeligo.Router;
import com.salesforce.apollo.crypto.Digest;

import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * GRPC outbound demultiplexer.
 *
 * @author hal.hildebrand
 *
 */
public class Egress {
    private static final Logger log = LoggerFactory.getLogger(Egress.class);

    private static ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                         final Metadata requestHeaders,
                                                                         ServerCallHandler<ReqT, RespT> next) {
                String target = requestHeaders.get(TARGET_METADATA_KEY);
                if (target == null) {
                    log.error("No target id in call headers: {}", requestHeaders.keys());
                    throw new StatusRuntimeException(Status.UNKNOWN.withDescription("No target ID in call"));
                }

                return Contexts.interceptCall(Context.current().withValue(SERVER_TARGET_KEY, digest(target)), call,
                                              requestHeaders, next);
            }
        };
    }

    private final DomainSocketAddress bridge;
    private final Server              server;
    private final AtomicBoolean       started = new AtomicBoolean();

    public Egress(DomainSocketAddress bridge, Function<Digest, ManagedChannel> outboundFactory) {
        this.bridge = bridge;
        server = NettyServerBuilder.forAddress(bridge)
                                   .protocolNegotiator(new DomainSocketNegotiator())
                                   .channelType(getServerDomainSocketChannelClass())
                                   .workerEventLoopGroup(getEventLoopGroup())
                                   .bossEventLoopGroup(getEventLoopGroup())
                                   .intercept(serverInterceptor())
                                   .fallbackHandlerRegistry(new GrpcProxy() {
                                       @Override
                                       protected Channel getChannel() {
                                           return outboundFactory.apply(Router.SERVER_TARGET_KEY.get());
                                       }
                                   }.newRegistry())
                                   .build();
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

    public DomainSocketAddress getBridge() {
        return bridge;
    }

    public void start() throws IOException {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        server.start();
    }
}
