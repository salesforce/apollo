/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.grpc.server.ConcurrencyLimitServerInterceptor;
import com.netflix.concurrency.limits.grpc.server.GrpcServerLimiterBuilder;
import com.salesforce.apollo.archipelago.server.FernetServerInterceptor;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.LimitsRegistry;
import io.grpc.*;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.inprocess.InternalInProcessChannelBuilder;
import io.grpc.internal.ManagedChannelImplBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.salesforce.apollo.cryptography.QualifiedBase64.digest;
import static com.salesforce.apollo.cryptography.QualifiedBase64.qb64;

/**
 * @author hal.hildebrand
 */
public class LocalServer implements RouterSupplier {
    private static final Logger log           = LoggerFactory.getLogger(LocalServer.class);
    private static final String NAME_TEMPLATE = "%s-%s";

    private final ClientInterceptor clientInterceptor;
    private final Member            from;
    private final String            prefix;

    public LocalServer(String prefix, Member member) {
        this.from = member;
        this.prefix = prefix;
        clientInterceptor = new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(Constants.METADATA_CLIENT_ID_KEY, qb64(from.getId()));
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    public Member getFrom() {
        return from;
    }

    @Override
    public RouterImpl router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit,
                             LimitsRegistry limitsRegistry, List<ServerInterceptor> interceptors,
                             Predicate<FernetServerInterceptor.HashedToken> validator, ExecutorService executor) {
        if (executor == null) {
            executor = Executors.newVirtualThreadPerTaskExecutor();
        }
        String name = String.format(NAME_TEMPLATE, prefix, qb64(from.getId()));
        var limitsBuilder = new GrpcServerLimiterBuilder().limit(serverLimit.get());
        if (limitsRegistry != null) {
            limitsBuilder.metricRegistry(limitsRegistry);
        }
        ServerBuilder<?> serverBuilder = InProcessServerBuilder.forName(name)
                                                               .executor(executor)
                                                               .intercept(ConcurrencyLimitServerInterceptor.newBuilder(
                                                                                                           limitsBuilder.build())
                                                                                                           .statusSupplier(
                                                                                                           () -> Status.RESOURCE_EXHAUSTED.withDescription(
                                                                                                           "Server concurrency limit reached"))
                                                                                                           .build())
                                                               .intercept(serverInterceptor());
        interceptors.forEach(i -> {
            serverBuilder.intercept(i);
        });
        return new RouterImpl(from, serverBuilder, cacheBuilder.setFactory(t -> connectTo(t)), new ClientIdentity() {
            @Override
            public Digest getFrom() {
                return Constants.SERVER_CLIENT_ID_KEY.get();
            }
        }, d -> {
        }, validator, executor);
    }

    private ManagedChannel connectTo(Member to) {
        final var name = String.format(NAME_TEMPLATE, prefix, qb64(to.getId()));
        final InProcessChannelBuilder builder = InProcessChannelBuilder.forName(name)
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
                String id = requestHeaders.get(Constants.METADATA_CLIENT_ID_KEY);
                if (id == null) {
                    log.error("No member id in call headers: {}", requestHeaders.keys());
                    throw new IllegalStateException("No member ID in call");
                }
                Context ctx = Context.current().withValue(Constants.SERVER_CLIENT_ID_KEY, digest(id));
                return Contexts.interceptCall(ctx, call, requestHeaders, next);
            }
        };
    }
}
