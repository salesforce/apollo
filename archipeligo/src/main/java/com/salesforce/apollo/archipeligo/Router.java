/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.archipeligo.ServerConnectionCache.CreateClientCommunications;
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
abstract public class Router<Ctx, To, From> {

    @FunctionalInterface
    public interface ClientConnector<Client, Ctx, To, From> {
        Client connect(To to, From from, Ctx context);
    }

    public class CommonCommunications<Client extends Link<To>, Service>
                                     implements ClientConnector<Client, Ctx, To, From> {
        private final CreateClientCommunications<Client, To, From> createFunction;
        private final Client                                       localLoopback;
        private final RoutableService<Ctx, Service>                routing;

        public CommonCommunications(RoutableService<Ctx, Service> routing,
                                    CreateClientCommunications<Client, To, From> createFunction, Client localLoopback) {
            this.routing = routing;
            this.createFunction = createFunction;
            this.localLoopback = localLoopback;
        }

        @SuppressWarnings("unlikely-arg-type")
        @Override
        public Client connect(To to, From from, Ctx context) {
            if (to == null) {
                return null;
            }
            return started.get() ? to.equals(from) ? localLoopback : cache.borrow(to, from, createFunction) : null;
        }

        public void deregister(Ctx context) {
            routing.unbind(context);
        }

        public void register(Ctx context, Service service) {
            routing.bind(context, service);
        }
    }

    public interface ServiceRouting {
        default String routing() {
            return getClass().getCanonicalName();
        }
    }

    private static final Metadata.Key<String> CONTEXT_METADATA_KEY = Metadata.Key.of("from.Context",
                                                                                     Metadata.ASCII_STRING_MARSHALLER);
    private final static Logger               log                  = LoggerFactory.getLogger(Router.class);

    protected final MutableHandlerRegistry registry;
    protected final ServerInterceptor      serverInterceptor;
    protected final AtomicBoolean          started = new AtomicBoolean();

    private final ServerConnectionCache<To, From>      cache;
    private final Context.Key<Ctx>                     CLIENT_CONTEXT_KEY = Context.key("from.Context");
    private final ClientInterceptor                    clientInterceptor;
    private final Map<String, RoutableService<Ctx, ?>> services           = new ConcurrentHashMap<>();

    public Router(ServerConnectionCache.Builder<To, From> builder, MutableHandlerRegistry registry,
                  Function<String, Ctx> ctxDeser, Function<Ctx, String> ctxSer) {
        serverInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                         final Metadata requestHeaders,
                                                                         ServerCallHandler<ReqT, RespT> next) {
                String id = requestHeaders.get(CONTEXT_METADATA_KEY);
                if (id == null) {
                    log.error("No context in call headers: {}", requestHeaders.keys());
                    throw new IllegalStateException("No member ID in call");
                }
                @SuppressWarnings("unused")
                var context = ctxDeser.apply(id);
                return Contexts.interceptCall(Context.current(), call, requestHeaders, next);
            }
        };
        clientInterceptor = new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(CONTEXT_METADATA_KEY, ctxSer.apply(CLIENT_CONTEXT_KEY.get()));
                        super.start(responseListener, headers);
                    }
                };
            }
        };
        this.cache = builder.build(clientInterceptor);
        this.registry = registry;
    }

    public void close() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        cache.close();
    }

    public <Client extends Link<To>, Service extends ServiceRouting> CommonCommunications<Client, Service> create(To member,
                                                                                                                  Ctx context,
                                                                                                                  Service service,
                                                                                                                  Function<RoutableService<Ctx, Service>, BindableService> factory,
                                                                                                                  CreateClientCommunications<Client, To, From> createFunction,
                                                                                                                  Client localLoopback) {
        return create(member, context, service, service.routing(), factory, createFunction, localLoopback);
    }

    public <Client extends Link<To>, Service> CommonCommunications<Client, Service> create(To member, Ctx context,
                                                                                           Service service,
                                                                                           String routingLabel,
                                                                                           Function<RoutableService<Ctx, Service>, BindableService> factory,
                                                                                           CreateClientCommunications<Client, To, From> createFunction,
                                                                                           Client localLoopback) {
        @SuppressWarnings("unchecked")
        RoutableService<Ctx, Service> routing = (RoutableService<Ctx, Service>) services.computeIfAbsent(routingLabel,
                                                                                                         c -> {
                                                                                                             RoutableService<Ctx, Service> route = new RoutableService<Ctx, Service>();
                                                                                                             BindableService bindableService = factory.apply(route);
                                                                                                             registry.addService(bindableService);
                                                                                                             return route;
                                                                                                         });
        routing.bind(context, service);
        log.info("Communications created for: " + member);
        return new CommonCommunications<Client, Service>(routing, createFunction, localLoopback);
    }

    abstract public ClientIdentity getClientIdentityProvider();

    abstract public void start();
}
