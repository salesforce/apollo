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
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.limit.AIMDLimit;
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
public class Router {

    @FunctionalInterface
    public interface ClientConnector<Client> {
        Client connect(Member to);
    }

    public class CommonCommunications<Client extends Link, Service> implements ClientConnector<Client> {
        private final Digest                             context;
        private final CreateClientCommunications<Client> createFunction;
        private final Member                             from;
        private final Client                             localLoopback;
        private final RoutableService<Service>           routing;

        public CommonCommunications(Digest context, Member from, RoutableService<Service> routing,
                                    CreateClientCommunications<Client> createFunction, Client localLoopback) {
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

    public interface ServiceRouting {
        default String routing() {
            return getClass().getCanonicalName();
        }
    }

    public static final Context.Key<Digest>  CLIENT_CLIENT_ID_KEY   = Context.key("com.salesforce.apollo.archipeligo.from.id.client");
    public static final Metadata.Key<String> METADATA_CLIENT_ID_KEY = Metadata.Key.of("com.salesforce.apollo.archipeligo.from.id",
                                                                                      Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> METADATA_CONTEXT_KEY   = Metadata.Key.of("com.salesforce.apollo.archipeligo.context.id",
                                                                                      Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> METADATA_TARGET_KEY    = Metadata.Key.of("com.salesforce.apollo.archipeligo.to.id",
                                                                                      Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<Digest>  SERVER_CLIENT_ID_KEY   = Context.key("com.salesforce.apollo.archipeligo.from.id.server");
    public static final Context.Key<Digest>  SERVER_CONTEXT_KEY     = Context.key("com.salesforce.apollo.archipeligo.context.id.server");
    public static final Context.Key<Digest>  SERVER_TARGET_KEY      = Context.key("com.salesforce.apollo.archipeligo.to.id.server");

    private final static Logger log = LoggerFactory.getLogger(Router.class);

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
                    log.error("No context id in call headers: {}", requestHeaders.keys());
                    throw new StatusRuntimeException(Status.UNKNOWN.withDescription("No context ID in call"));
                }

                return Contexts.interceptCall(Context.current().withValue(SERVER_CONTEXT_KEY, digest(id)), call,
                                              requestHeaders, next);
            }
        };
    }

    private final ServerConnectionCache           cache;
    private final ClientIdentity                  clientIdentityProvider;
    private final Consumer<Digest>                contextRegistration;
    private final MutableHandlerRegistry          registry = new MutableHandlerRegistry();
    private final Server                          server;
    private final Map<String, RoutableService<?>> services = new ConcurrentHashMap<>();
    private final AtomicBoolean                   started  = new AtomicBoolean();

    public Router(ServerBuilder<?> serverBuilder, ServerConnectionCache.Builder cacheBuilder,
                  ClientIdentity clientIdentityProvider) {
        this(serverBuilder, cacheBuilder, clientIdentityProvider, d -> {
        });
    }

    public Router(ServerBuilder<?> serverBuilder, ServerConnectionCache.Builder cacheBuilder,
                  ClientIdentity clientIdentityProvider, Consumer<Digest> contextRegistration) {
        this.server = serverBuilder.fallbackHandlerRegistry(registry).intercept(serverInterceptor()).build();
        this.cache = cacheBuilder.build();
        this.clientIdentityProvider = clientIdentityProvider;
        this.contextRegistration = contextRegistration;
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

    public <Client extends Link, Service extends ServiceRouting> CommonCommunications<Client, Service> create(Member member,
                                                                                                              Digest context,
                                                                                                              Service service,
                                                                                                              Function<RoutableService<Service>, BindableService> factory,
                                                                                                              CreateClientCommunications<Client> createFunction,
                                                                                                              Client localLoopback) {
        return create(member, context, service, service.routing(), factory, createFunction, localLoopback);
    }

    public <Client extends Link, Service> CommonCommunications<Client, Service> create(Member member, Digest context,
                                                                                       Service service,
                                                                                       String routingLabel,
                                                                                       Function<RoutableService<Service>, BindableService> factory,
                                                                                       CreateClientCommunications<Client> createFunction,
                                                                                       Client localLoopback) {
        @SuppressWarnings("unchecked")
        RoutableService<Service> routing = (RoutableService<Service>) services.computeIfAbsent(routingLabel, c -> {
            RoutableService<Service> route = new RoutableService<Service>();
            BindableService bindableService = factory.apply(route);
            registry.addService(bindableService);
            return route;
        });
        routing.bind(context, service);
        contextRegistration.accept(context);
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
        log.info("Started router: {}", server.getListenSockets());
    }
}
