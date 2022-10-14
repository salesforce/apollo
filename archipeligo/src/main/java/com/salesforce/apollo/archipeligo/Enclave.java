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

import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
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
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.internal.ManagedChannelImplBuilder;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * Enclave Server for routing from a process endpoint in the default Isolate
 * into individual Isolates.
 *
 * @author hal.hildebrand
 *
 */
public class Enclave<To extends Member> implements RouterSupplier<To> {
    private static final Logger log = LoggerFactory.getLogger(Enclave.class);

    private final DomainSocketAddress bridge;
    private final DomainSocketAddress endpoint;
    private final Executor            executor;

    public Enclave(DomainSocketAddress endpoint, Executor executor, DomainSocketAddress bridge) {
        this.bridge = bridge;
        this.executor = executor;
        this.endpoint = endpoint;
    }

    /**
     * 
     * @return the DomainSocketAddress for this Enclave
     */
    public DomainSocketAddress getEndpoint() {
        return endpoint;
    }

    @Override
    public Router<To> router(ServerConnectionCache.Builder<To> cacheBuilder, Supplier<Limit> serverLimit,
                             Executor executor, LimitsRegistry limitsRegistry) {
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
        return new Router<To>(serverBuilder, cacheBuilder.setFactory(t -> connectTo(t)), new ClientIdentity() {
            @Override
            public Digest getFrom() {
                return Router.CLIENT_ID_CONTEXT_KEY.get();
            }
        });
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
                        headers.put(Router.TARGET_METADATA_KEY, qb64(to.getId()));
                        super.start(responseListener, headers);
                    }
                };
            }
        };
        final var builder = NettyChannelBuilder.forAddress(bridge)
                                               .eventLoopGroup(getEventLoopGroup())
                                               .channelType(getChannelType())
                                               .keepAliveTime(1, TimeUnit.MILLISECONDS)
                                               .usePlaintext()
                                               .executor(executor)
                                               .intercept(clientInterceptor);
        disableTrash(builder);
        return builder.build();
    }

    private void disableTrash(final NettyChannelBuilder builder) {
        try {
            final Method method = InProcessChannelBuilder.class.getDeclaredMethod("delegate");
            method.setAccessible(true);
            ManagedChannelImplBuilder delegate = (ManagedChannelImplBuilder) method.invoke(builder);
            delegate.setTracingEnabled(false);
        } catch (Throwable e) {
            log.error("Can't disable trash", e);
        }
    }

    private ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                         final Metadata requestHeaders,
                                                                         ServerCallHandler<ReqT, RespT> next) {
                String id = requestHeaders.get(Router.CLIENT_ID_METADATA_KEY);
                if (id == null) {
                    log.error("No member id in call headers: {}", requestHeaders.keys());
                    throw new IllegalStateException("No member ID in call");
                }
                Context ctx = Context.current().withValue(Router.CLIENT_ID_CONTEXT_KEY, digest(id));
                return Contexts.interceptCall(ctx, call, requestHeaders, next);
            }
        };
    }
}
