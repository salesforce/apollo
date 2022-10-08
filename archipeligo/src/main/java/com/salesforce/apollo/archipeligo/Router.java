/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.archipeligo.ServerConnectionCache.CreateClientCommunications;
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
public class Router<To extends Member, From extends Member> {

    @FunctionalInterface
    public interface ClientConnector<Client, To extends Member, From extends Member> {
        Client connect(To to, From from, Digest context);
    }

    public class CommonCommunications<Client extends Link<To>, Service> implements ClientConnector<Client, To, From> {
        private final CreateClientCommunications<Client, To, From> createFunction;
        private final Client                                       localLoopback;
        private final RoutableService<Service>                     routing;

        public CommonCommunications(RoutableService<Service> routing,
                                    CreateClientCommunications<Client, To, From> createFunction, Client localLoopback) {
            this.routing = routing;
            this.createFunction = createFunction;
            this.localLoopback = localLoopback;
        }

        @SuppressWarnings("unlikely-arg-type")
        @Override
        public Client connect(To to, From from, Digest context) {
            if (to == null) {
                return null;
            }
            return started.get() ? to.equals(from) ? localLoopback : cache.borrow(to, from, createFunction) : null;
        }

        public void deregister(Digest context) {
            routing.unbind(context);
        }

        public void register(Digest context, Service service) {
            routing.bind(context, service);
        }
    }

    public interface ServiceRouting {
        default String routing() {
            return getClass().getCanonicalName();
        }
    }

    public static final Context.Key<Digest>  CLIENT_CONTEXT_KEY   = Context.key("com.salesforce.apollo.archipeligo.from.Context");
    public static final Metadata.Key<String> CONTEXT_METADATA_KEY = Metadata.Key.of("com.salesforce.apollo.archipeligo.from.Context",
                                                                                    Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<Digest>  SERVER_CONTEXT_KEY   = Context.key("com.salesforce.apollo.archipeligo.Context.from");

    private final static Logger log = LoggerFactory.getLogger(Router.class);

    public static ClientInterceptor clientInterceptor() {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(CONTEXT_METADATA_KEY, qb64(CLIENT_CONTEXT_KEY.get()));
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    public static ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                         final Metadata requestHeaders,
                                                                         ServerCallHandler<ReqT, RespT> next) {
                String id = requestHeaders.get(CONTEXT_METADATA_KEY);
                if (id == null) {
                    log.error("No context id in call headers: {}", requestHeaders.keys());
                    throw new IllegalStateException("No context ID in call");
                }

                return Contexts.interceptCall(io.grpc.Context.current().withValue(SERVER_CONTEXT_KEY, digest(id)), call,
                                              requestHeaders, next);
            }
        };
    }

    private final ServerConnectionCache<To, From> cache;
    private final ClientIdentity                  clientIdentityProvider;
    private final MutableHandlerRegistry          registry = new MutableHandlerRegistry();
    private final Server                          server;
    private final Map<String, RoutableService<?>> services = new ConcurrentHashMap<>();
    private final AtomicBoolean                   started  = new AtomicBoolean();

    public Router(ServerBuilder<?> serverBuilder, ServerConnectionCache.Builder<To, From> cacheBuilder,
                  ClientIdentity clientIdentityProvider) {
        this.server = serverBuilder.intercept(serverInterceptor()).build();
        this.cache = cacheBuilder.build();
        this.clientIdentityProvider = clientIdentityProvider;
    }

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

    public <Client extends Link<To>, Service extends ServiceRouting> CommonCommunications<Client, Service> create(To member,
                                                                                                                  Digest context,
                                                                                                                  Service service,
                                                                                                                  Function<RoutableService<Service>, BindableService> factory,
                                                                                                                  CreateClientCommunications<Client, To, From> createFunction,
                                                                                                                  Client localLoopback) {
        return create(member, context, service, service.routing(), factory, createFunction, localLoopback);
    }

    public <Client extends Link<To>, Service> CommonCommunications<Client, Service> create(To member, Digest context,
                                                                                           Service service,
                                                                                           String routingLabel,
                                                                                           Function<RoutableService<Service>, BindableService> factory,
                                                                                           CreateClientCommunications<Client, To, From> createFunction,
                                                                                           Client localLoopback) {
        @SuppressWarnings("unchecked")
        RoutableService<Service> routing = (RoutableService<Service>) services.computeIfAbsent(routingLabel, c -> {
            RoutableService<Service> route = new RoutableService<Service>();
            BindableService bindableService = factory.apply(route);
            registry.addService(bindableService);
            return route;
        });
        routing.bind(context, service);
        log.info("Communications created for: " + member);
        return new CommonCommunications<Client, Service>(routing, createFunction, localLoopback);
    }

    public ClientIdentity getClientIdentityProvider() {
        return clientIdentityProvider;
    }

    public void start() throws IOException {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        server.start();
    }
}
