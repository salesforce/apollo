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
import java.util.function.BiFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.archipeligo.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.BindableService;
import io.grpc.util.MutableHandlerRegistry;

/**
 * Context based GRPC routing
 *
 * @author hal.hildebrand
 *
 */
abstract public class Router<Context, T, F> {
    public class CommonCommunications<Client extends Link<T>, Service> implements BiFunction<T, F, Client> {
        private final CreateClientCommunications<Client, T, F> createFunction;
        private final Client                                   localLoopback;
        private final RoutableService<Context, Service>        routing;

        public CommonCommunications(RoutableService<Context, Service> routing,
                                    CreateClientCommunications<Client, T, F> createFunction, Client localLoopback) {
            this.routing = routing;
            this.createFunction = createFunction;
            this.localLoopback = localLoopback;
        }

        @SuppressWarnings("unlikely-arg-type")
        @Override
        public Client apply(T to, F from) {
            if (to == null) {
                return null;
            }
            return started.get() ? to.equals(from) ? localLoopback : cache.borrow(to, from, createFunction) : null;
        }

        public void deregister(Context context) {
            routing.unbind(context);
        }

        public void register(Context context, Service service) {
            routing.bind(context, service);
        }
    }

    public interface ServiceRouting {
        default String routing() {
            return getClass().getCanonicalName();
        }
    }

    private final static Logger log = LoggerFactory.getLogger(Router.class);

    protected final MutableHandlerRegistry                 registry;
    protected final AtomicBoolean                          started  = new AtomicBoolean();
    private final ServerConnectionCache<T, F>              cache;
    private final Map<String, RoutableService<Context, ?>> services = new ConcurrentHashMap<>();

    public Router(ServerConnectionCache<T, F> cache, MutableHandlerRegistry registry) {
        this.cache = cache;
        this.registry = registry;
    }

    public void close() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        cache.close();
    }

    public <Client extends Link<T>, Service extends ServiceRouting> CommonCommunications<Client, Service> create(T member,
                                                                                                                 Context context,
                                                                                                                 Service service,
                                                                                                                 Function<RoutableService<Context, Service>, BindableService> factory,
                                                                                                                 CreateClientCommunications<Client, T, F> createFunction,
                                                                                                                 Client localLoopback) {
        return create(member, context, service, service.routing(), factory, createFunction, localLoopback);
    }

    public <Client extends Link<T>, Service> CommonCommunications<Client, Service> create(T member, Context context,
                                                                                          Service service,
                                                                                          String routingLabel,
                                                                                          Function<RoutableService<Context, Service>, BindableService> factory,
                                                                                          CreateClientCommunications<Client, T, F> createFunction,
                                                                                          Client localLoopback) {
        @SuppressWarnings("unchecked")
        RoutableService<Context, Service> routing = (RoutableService<Context, Service>) services.computeIfAbsent(routingLabel,
                                                                                                                 c -> {
                                                                                                                     RoutableService<Context, Service> route = new RoutableService<Context, Service>();
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
