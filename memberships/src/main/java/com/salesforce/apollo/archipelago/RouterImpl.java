/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.limit.AIMDLimit;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.BindableService;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.util.MutableHandlerRegistry;

/**
 * Context based GRPC routing
 *
 * @author hal.hildebrand
 *
 */
public class RouterImpl implements Router {

    public class CommonCommunications<Client extends Link, Service> implements Router.ClientConnector<Client> {
        public static <Client> Client vanilla(Member from) {
            @SuppressWarnings("unchecked")
            Client client = (Client) new Link() {

                @Override
                public void close() throws IOException {
                }

                @Override
                public Member getMember() {
                    return from;
                }
            };
            return client;
        }

        private final Digest                             context;
        private final CreateClientCommunications<Client> createFunction;
        private final Member                             from;
        private final Client                             localLoopback;

        private final RoutableService<Service> routing;

        public <T extends Member> CommonCommunications(Digest context, Member from, RoutableService<Service> routing) {
            this(context, from, routing, m -> vanilla(from), vanilla(from));

        }

        public <T extends Member> CommonCommunications(Digest context, Member from, RoutableService<Service> routing,
                                                       CreateClientCommunications<Client> createFunction,
                                                       Client localLoopback) {
            this.context = context;
            this.routing = routing;
            this.createFunction = createFunction;
            this.localLoopback = localLoopback;
            this.from = from;
        }

        @Override
        public Client connect(Member to) {
            if (to == null) {
                return null;
            }
            return started.get() ? (to.equals(from) ? localLoopback : cache.borrow(context, to, createFunction)) : null;
        }

        public void deregister(Digest context) {
            routing.unbind(context);
        }

        public void register(Digest context, Service service) {
            routing.bind(context, service);
        }
    }

    private final static Logger log = LoggerFactory.getLogger(RouterImpl.class);

    public static ClientInterceptor clientInterceptor(Digest ctx) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(RouterImpl.METADATA_CONTEXT_KEY, qb64(ctx));
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    public static Limit defaultServerLimit() {
        return AIMDLimit.newBuilder().initialLimit(100).maxLimit(1000).timeout(500, TimeUnit.MILLISECONDS).build();
    }

    public static ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                         final Metadata requestHeaders,
                                                                         ServerCallHandler<ReqT, RespT> next) {
                String id = requestHeaders.get(METADATA_CONTEXT_KEY);
                if (id == null) {
                    log.trace("No context id in call headers: {}", requestHeaders.keys());
                    return next.startCall(call, requestHeaders);
                }

                return Contexts.interceptCall(Context.current().withValue(SERVER_CONTEXT_KEY, digest(id)), call,
                                              requestHeaders, next);
            }
        };
    }

    private final ServerConnectionCache           cache;
    private final ClientIdentity                  clientIdentityProvider;
    private final Consumer<Digest>                contextRegistration;
    private final Executor                        executor;
    private final Member                          from;
    private final MutableHandlerRegistry          registry = new MutableHandlerRegistry();
    private final Server                          server;
    private final Map<String, RoutableService<?>> services = new ConcurrentHashMap<>();
    private final AtomicBoolean                   started  = new AtomicBoolean();

    public RouterImpl(Member from, ServerBuilder<?> serverBuilder, ServerConnectionCache.Builder cacheBuilder,
                      ClientIdentity clientIdentityProvider) {
        this(from, serverBuilder, cacheBuilder, clientIdentityProvider, r -> r.run());
    }

    public RouterImpl(Member from, ServerBuilder<?> serverBuilder, ServerConnectionCache.Builder cacheBuilder,
                      ClientIdentity clientIdentityProvider, Consumer<Digest> contextRegistration, Executor executor) {
        this.server = serverBuilder.fallbackHandlerRegistry(registry).intercept(serverInterceptor()).build();
        this.cache = cacheBuilder.build();
        this.clientIdentityProvider = clientIdentityProvider;
        this.contextRegistration = contextRegistration;
        this.executor = executor;
        this.from = from;
    }

    public RouterImpl(Member from, ServerBuilder<?> serverBuilder, ServerConnectionCache.Builder cacheBuilder,
                      ClientIdentity clientIdentityProvider, Executor executor) {
        this(from, serverBuilder, cacheBuilder, clientIdentityProvider, d -> {
        }, executor);
    }

    @Override
    public void close(Duration await) {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        cache.close();
        server.shutdown();
        try {
            server.awaitTermination(await.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public <Client extends Link, Service extends Router.ServiceRouting> CommonCommunications<Client, Service> create(Member member,
                                                                                                                     Digest context,
                                                                                                                     Service service,
                                                                                                                     Function<RoutableService<Service>, BindableService> factory,
                                                                                                                     CreateClientCommunications<Client> createFunction,
                                                                                                                     Client localLoopback) {
        return create(member, context, service, service.routing(), factory, createFunction, localLoopback);
    }

    @Override
    public <Service, Client extends Link> CommonCommunications<Client, Service> create(Member member, Digest context,
                                                                                       Service service,
                                                                                       String routingLabel,
                                                                                       Function<RoutableService<Service>, BindableService> factory) {
        @SuppressWarnings("unchecked")
        RoutableService<Service> routing = (RoutableService<Service>) services.computeIfAbsent(routingLabel, c -> {
            var route = new RoutableService<Service>(executor);
            BindableService bindableService = factory.apply(route);
            registry.addService(bindableService);
            return route;
        });
        routing.bind(context, service);
        contextRegistration.accept(context);
        log.info("Communications created for: " + member.getId());
        return new CommonCommunications<Client, Service>(context, member, routing);
    }

    @Override
    public <Client extends Link, Service> CommonCommunications<Client, Service> create(Member member, Digest context,
                                                                                       Service service,
                                                                                       String routingLabel,
                                                                                       Function<RoutableService<Service>, BindableService> factory,
                                                                                       CreateClientCommunications<Client> createFunction,
                                                                                       Client localLoopback) {
        @SuppressWarnings("unchecked")
        RoutableService<Service> routing = (RoutableService<Service>) services.computeIfAbsent(routingLabel, c -> {
            var route = new RoutableService<Service>(executor);
            BindableService bindableService = factory.apply(route);
            registry.addService(bindableService);
            return route;
        });
        routing.bind(context, service);
        contextRegistration.accept(context);
        log.info("Communications created for: " + member.getId());
        return new CommonCommunications<Client, Service>(context, member, routing, createFunction, localLoopback);
    }

    @Override
    public ClientIdentity getClientIdentityProvider() {
        return clientIdentityProvider;
    }

    @Override
    public Member getFrom() {
        return from;
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        try {
            server.start();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot start server", e);
        }
        log.info("Started router: {}", server.getListenSockets());
    }
}
