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
import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.grpc.server.ConcurrencyLimitServerInterceptor;
import com.netflix.concurrency.limits.grpc.server.GrpcServerLimiterBuilder;
import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.LimitsRegistry;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * Enclave Server for routing from a process endpoint in the default Isolate
 * into individual Isolates.
 *
 * @author hal.hildebrand
 *
 */
public class Enclave implements RouterSupplier {
    private static final Logger log = LoggerFactory.getLogger(Enclave.class);

    private final DomainSocketAddress                       bridge;
    private final Class<? extends io.netty.channel.Channel> channelType    = getChannelType();
    private final Consumer<Digest>                          contextRegistration;
    private final DomainSocketAddress                       endpoint;
    private final EventLoopGroup                            eventLoopGroup = getEventLoopGroup();
    private final Executor                                  executor;
    private final String                                    from;
    private final Duration                                  keepAlive;

    public Enclave(Digest from, DomainSocketAddress endpoint, Executor executor, DomainSocketAddress bridge,
                   Duration keepAlive, Consumer<Digest> contextRegistration) {
        this.bridge = bridge;
        this.executor = executor;
        this.endpoint = endpoint;
        this.keepAlive = keepAlive;
        this.contextRegistration = contextRegistration;
        this.from = qb64(from);
    }

    /**
     * 
     * @return the DomainSocketAddress for this Enclave
     */
    public DomainSocketAddress getEndpoint() {
        return endpoint;
    }

    @Override
    public Router router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit, Executor executor,
                         LimitsRegistry limitsRegistry) {
        var limitsBuilder = new GrpcServerLimiterBuilder().limit(serverLimit.get());
        if (limitsRegistry != null) {
            limitsBuilder.metricRegistry(limitsRegistry);
        }
        ServerBuilder<?> serverBuilder = NettyServerBuilder.forAddress(endpoint)
                                                           .protocolNegotiator(new DomainSocketNegotiator())
                                                           .channelType(getServerDomainSocketChannelClass())
                                                           .workerEventLoopGroup(getEventLoopGroup())
                                                           .bossEventLoopGroup(getEventLoopGroup())
                                                           .intercept(new DomainSocketServerInterceptor())
                                                           .intercept(ConcurrencyLimitServerInterceptor.newBuilder(limitsBuilder.build())
                                                                                                       .statusSupplier(() -> Status.RESOURCE_EXHAUSTED.withDescription("Server concurrency limit reached"))
                                                                                                       .build())
                                                           .intercept(serverInterceptor());
        return new Router(serverBuilder, cacheBuilder.setFactory(t -> connectTo(t)), new ClientIdentity() {
            @Override
            public Digest getFrom() {
                return Router.SERVER_CLIENT_ID_KEY.get();
            }
        }, contextRegistration, executor);
    }

    private ManagedChannel connectTo(Member to) {
        var clientInterceptor = new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(Router.METADATA_TARGET_KEY, qb64(to.getId()));
                        headers.put(Router.METADATA_CLIENT_ID_KEY, from);
                        super.start(responseListener, headers);
                    }
                };
            }
        };
        final var builder = NettyChannelBuilder.forAddress(bridge)
                                               .eventLoopGroup(eventLoopGroup)
                                               .channelType(channelType)
                                               .keepAliveTime(keepAlive.toNanos(), TimeUnit.NANOSECONDS)
                                               .usePlaintext()
                                               .executor(executor)
                                               .intercept(clientInterceptor);
        return builder.build();
    }

    private ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                         final Metadata requestHeaders,
                                                                         ServerCallHandler<ReqT, RespT> next) {
                String id = requestHeaders.get(Router.METADATA_CLIENT_ID_KEY);
                if (id == null) {
                    log.error("No member id in call headers: {}", requestHeaders.keys());
                    throw new IllegalStateException("No member ID in call");
                }
                Context ctx = Context.current().withValue(Router.SERVER_CLIENT_ID_KEY, digest(id));
                return Contexts.interceptCall(ctx, call, requestHeaders, next);
            }
        };
    }
}
