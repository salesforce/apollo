/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.grpc.server.ConcurrencyLimitServerInterceptor;
import com.netflix.concurrency.limits.grpc.server.GrpcServerLimiterBuilder;
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
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.inprocess.InternalInProcessChannelBuilder;
import io.grpc.internal.ManagedChannelImplBuilder;

/**
 * @author hal.hildebrand
 *
 */
public class LocalServer implements RouterSupplier {
    private static final Logger log           = LoggerFactory.getLogger(LocalServer.class);
    private static final String NAME_TEMPLATE = "%s-%s";

    private final ClientInterceptor clientInterceptor;
    private final Executor          executor;
    private final Member            from;
    private final String            prefix;

    public LocalServer(String prefix, Member member, Executor executor) {
        this.from = member;
        this.prefix = prefix;
        this.executor = executor;
        clientInterceptor = new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(Router.METADATA_CLIENT_ID_KEY, qb64(from.getId()));
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    @Override
    public Router router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit, Executor executor,
                         LimitsRegistry limitsRegistry) {
        String name = String.format(NAME_TEMPLATE, prefix, qb64(from.getId()));
        var limitsBuilder = new GrpcServerLimiterBuilder().limit(serverLimit.get());
        if (limitsRegistry != null) {
            limitsBuilder.metricRegistry(limitsRegistry);
        }
        ServerBuilder<?> serverBuilder = InProcessServerBuilder.forName(name)
                                                               .executor(executor)
                                                               .intercept(ConcurrencyLimitServerInterceptor.newBuilder(limitsBuilder.build())
                                                                                                           .statusSupplier(() -> Status.RESOURCE_EXHAUSTED.withDescription("Server concurrency limit reached"))
                                                                                                           .build())
                                                               .intercept(serverInterceptor());
        return new Router(serverBuilder, cacheBuilder.setFactory(t -> connectTo(t)), new ClientIdentity() {
            @Override
            public Digest getFrom() {
                return Router.SERVER_CLIENT_ID_KEY.get();
            }
        }, executor);
    }

    private ManagedChannel connectTo(Member to) {
        final var name = String.format(NAME_TEMPLATE, prefix, qb64(to.getId()));
        final InProcessChannelBuilder builder = InProcessChannelBuilder.forName(name)
                                                                       .executor(executor)
                                                                       .usePlaintext()
                                                                       .intercept(clientInterceptor);
        disableTrash(builder);
        InternalInProcessChannelBuilder.setStatsEnabled(builder, false);
        return builder.build();
    }

    private void disableTrash(final InProcessChannelBuilder builder) {
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
