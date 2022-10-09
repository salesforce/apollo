/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;

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
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.util.MutableHandlerRegistry;

/**
 * Context based GRPC routing
 *
 * @author hal.hildebrand
 *
 */
public class Router<To extends Member> {

    @FunctionalInterface
    public interface ClientConnector<Client, To extends Member> {
        Client connect(To to);
    }

    public class CommonCommunications<Client extends Link<To>, Service> implements ClientConnector<Client, To> {
        private final Digest                                 context;
        private final CreateClientCommunications<Client, To> createFunction;
        private final Member                                 from;
        private final Client                                 localLoopback;
        private final RoutableService<Service>               routing;

        public CommonCommunications(Digest context, Member from, RoutableService<Service> routing,
                                    CreateClientCommunications<Client, To> createFunction, Client localLoopback) {
            this.context = context;
            this.routing = routing;
            this.createFunction = createFunction;
            this.localLoopback = localLoopback;
            this.from = from;
        }

        @Override
        public Client connect(To to) {
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

    public interface ServiceRouting {
        default String routing() {
            return getClass().getCanonicalName();
        }
    }

    public static final Metadata.Key<String> CONTEXT_METADATA_KEY = Metadata.Key.of("com.salesforce.apollo.archipeligo.from.Context",
                                                                                    Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<Digest>  SERVER_CONTEXT_KEY   = Context.key("com.salesforce.apollo.archipeligo.Context.from");
    public static final Context.Key<Digest>  SERVER_TARGET_KEY    = Context.key("com.salesforce.apollo.archipeligo.to.Endpoint");
    public static final Metadata.Key<String> TARGET_METADATA_KEY  = Metadata.Key.of("com.salesforce.apollo.archipeligo.to.Endpoint",
                                                                                    Metadata.ASCII_STRING_MARSHALLER);

    private final static Logger log = LoggerFactory.getLogger(Router.class);

    public static ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                         final Metadata requestHeaders,
                                                                         ServerCallHandler<ReqT, RespT> next) {
                var ctxt = Context.current();
                String id = requestHeaders.get(CONTEXT_METADATA_KEY);
                if (id == null) {
                    log.error("No context id in call headers: {}", requestHeaders.keys());
                    throw new StatusRuntimeException(Status.UNKNOWN.withDescription("No context ID in call"));
                } else {
                    ctxt = ctxt.withValue(SERVER_CONTEXT_KEY, digest(id));
                }
                String target = requestHeaders.get(TARGET_METADATA_KEY);
                if (target != null) {
                    ctxt = ctxt.withValue(SERVER_TARGET_KEY, digest(target));
                }

                return Contexts.interceptCall(ctxt, call, requestHeaders, next);
            }
        };
    }

    private final ServerConnectionCache<To>       cache;
    private final ClientIdentity                  clientIdentityProvider;
    private final MutableHandlerRegistry          registry = new MutableHandlerRegistry();
    private final Server                          server;
    private final Map<String, RoutableService<?>> services = new ConcurrentHashMap<>();
    private final AtomicBoolean                   started  = new AtomicBoolean();

    public Router(ServerBuilder<?> serverBuilder, ServerConnectionCache.Builder<To> cacheBuilder,
                  ClientIdentity clientIdentityProvider) {
        this.server = serverBuilder.fallbackHandlerRegistry(registry).intercept(serverInterceptor()).build();
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
                                                                                                                  CreateClientCommunications<Client, To> createFunction,
                                                                                                                  Client localLoopback) {
        return create(member, context, service, service.routing(), factory, createFunction, localLoopback);
    }

    public <Client extends Link<To>, Service> CommonCommunications<Client, Service> create(To member, Digest context,
                                                                                           Service service,
                                                                                           String routingLabel,
                                                                                           Function<RoutableService<Service>, BindableService> factory,
                                                                                           CreateClientCommunications<Client, To> createFunction,
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
        return new CommonCommunications<Client, Service>(context, member, routing, createFunction, localLoopback);
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
