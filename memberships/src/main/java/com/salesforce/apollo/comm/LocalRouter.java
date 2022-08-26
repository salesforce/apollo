/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import static com.codahale.metrics.MetricRegistry.name;
import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.grpc.client.ConcurrencyLimitClientInterceptor;
import com.netflix.concurrency.limits.grpc.client.GrpcClientLimiterBuilder;
import com.netflix.concurrency.limits.grpc.server.ConcurrencyLimitServerInterceptor;
import com.netflix.concurrency.limits.grpc.server.GrpcServerLimiterBuilder;
import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionFactory;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
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
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.inprocess.InternalInProcessChannelBuilder;
import io.grpc.internal.ManagedChannelImplBuilder;
import io.grpc.util.MutableHandlerRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class LocalRouter extends Router {

    public static class LocalServerConnectionFactory implements ServerConnectionFactory {

        private final Executor           executor;
        private GrpcClientLimiterBuilder limitBuilder;
        private final String             prefix;

        public LocalServerConnectionFactory(String prefix, Supplier<Limit> clientLimit, LimitsRegistry limitsRegistry,
                                            Executor executor) {
            this.prefix = prefix;
            limitBuilder = new GrpcClientLimiterBuilder().limit(clientLimit.get()).blockOnLimit(false);
            if (limitsRegistry != null) {
                limitBuilder.metricRegistry(limitsRegistry);
            }
            this.executor = executor;
        }

        @Override
        public ManagedChannel connectTo(Member to, SigningMember from) {
            ClientInterceptor clientInterceptor = new ClientInterceptor() {

                @Override
                public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                           CallOptions callOptions, Channel next) {
                    ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                    return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                        @Override
                        public void start(Listener<RespT> responseListener, Metadata headers) {
                            headers.put(AUTHORIZATION_METADATA_KEY, qb64(from.getId()));
                            super.start(responseListener, headers);
                        }
                    };
                }
            };
            final var name = String.format(NAME_TEMPLATE, prefix, qb64(to.getId()));
            final InProcessChannelBuilder builder;
            limitBuilder.named(name(from.getId().shortString(), "to", to.getId().shortString()));
            builder = InProcessChannelBuilder.forName(name)
                                             .executor(executor)
                                             .intercept(clientInterceptor,
                                                        new ConcurrencyLimitClientInterceptor(limitBuilder.build(),
                                                                                              () -> Status.RESOURCE_EXHAUSTED.withDescription("Client concurrency limit reached")));
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
    }

    private static final class ThreadIdentity implements ClientIdentity {
        @Override
        public Digest getFrom() {
            Member member = CLIENT_ID_CONTEXT_KEY.get();
            return member == null ? null : member.getId();
        }

    }

    public static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY = Metadata.Key.of("Authorization",
                                                                                          Metadata.ASCII_STRING_MARSHALLER);
    public static final String               NAME_TEMPLATE              = "%s-%s";

    private static final Context.Key<Member> CLIENT_ID_CONTEXT_KEY = Context.key("from.id");
    private static final ThreadIdentity      LOCAL_IDENTITY        = new ThreadIdentity();
    private static final Logger              log                   = LoggerFactory.getLogger(LocalRouter.class);
    private static final Map<Digest, Member> serverMembers         = new ConcurrentHashMap<>();

    private final Executor           executor;
    private GrpcServerLimiterBuilder limitsBuilder;
    private Member                   member;
    private final String             prefix;
    private Server                   server;

    public LocalRouter(String prefix, ServerConnectionCache.Builder builder, Executor executor,
                       LimitsRegistry limitsRegistry) {
        this(prefix, () -> Router.defaultClientLimit(), builder, new MutableHandlerRegistry(),
             () -> Router.defaultServerLimit(), executor, limitsRegistry);
    }

    public LocalRouter(String prefix, Supplier<Limit> clientLimit, ServerConnectionCache.Builder builder,
                       MutableHandlerRegistry registry, Supplier<Limit> serverLimit, Executor executor,
                       LimitsRegistry limitsRegistry) {
        super(builder.setFactory(new LocalServerConnectionFactory(prefix, clientLimit, limitsRegistry, executor))
                     .build(),
              registry);

        limitsBuilder = new GrpcServerLimiterBuilder().limit(serverLimit.get());
        if (limitsRegistry != null) {
            limitsBuilder.metricRegistry(limitsRegistry);
        }

        this.prefix = prefix;
        this.executor = executor;
    }

    public LocalRouter(String prefix, Supplier<Limit> clientLimit, ServerConnectionCache.Builder builder,
                       Supplier<Limit> serverLimit, Executor executor, LimitsRegistry limitsRegistry) {
        this(prefix, clientLimit, builder, new MutableHandlerRegistry(), serverLimit, executor, limitsRegistry);
    }

    @Override
    public void close() {
        if (!started.get()) {
            return;
        }
        super.close();
        serverMembers.remove(member.getId());
        if (server != null) {
            server.shutdownNow();
            try {
                server.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Unknown server state as we've been interrupted in the process of shutdown",
                                                e);
            } finally {
                server = null;
            }
        }
    }

    @Override
    public ClientIdentity getClientIdentityProvider() {
        return LOCAL_IDENTITY;
    }

    public Member getMember() {
        return member;
    }

    public void setMember(Member member) {
        this.member = member;
    }

    @Override
    public void start() {
        if (member == null) {
            throw new IllegalStateException("Must set member before starting");
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        final var name = String.format(NAME_TEMPLATE, prefix, qb64(member.getId()));

        limitsBuilder.named(name(member.getId().shortString(), "service"));
        server = InProcessServerBuilder.forName(name)
                                       .executor(executor)
                                       .intercept(interceptor())
                                       .intercept(ConcurrencyLimitServerInterceptor.newBuilder(limitsBuilder.build())
                                                                                   .statusSupplier(() -> Status.RESOURCE_EXHAUSTED.withDescription("Server concurrency limit reached"))
                                                                                   .build())
                                       .fallbackHandlerRegistry(registry)
                                       .build();
        try {
            serverMembers.put(member.getId(), member);
            server.start();
        } catch (IOException e) {
            log.error("Cannot start in process server for: " + member, e);
        }
        log.info("Starting server: {} for: {}", name, member);
    }

    private ServerInterceptor interceptor() {
        return new ServerInterceptor() {

            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                         final Metadata requestHeaders,
                                                                         ServerCallHandler<ReqT, RespT> next) {
                String id = requestHeaders.get(AUTHORIZATION_METADATA_KEY);
                if (id == null) {
                    log.error("No member id in call headers: {}", requestHeaders.keys());
                    throw new IllegalStateException("No member ID in call");
                }
                Member member = serverMembers.get(digest(id));
                if (member == null) {
                    call.close(Status.UNAUTHENTICATED.withDescription("No member for id: " + id), null);
                    return new ServerCall.Listener<ReqT>() {
                    };
                }
                Context ctx = Context.current().withValue(CLIENT_ID_CONTEXT_KEY, member);
                return Contexts.interceptCall(ctx, call, requestHeaders, next);
            }

        };
    }

}
