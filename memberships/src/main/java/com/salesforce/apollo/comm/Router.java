/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.utils.Utils;

import io.grpc.BindableService;
import io.grpc.util.MutableHandlerRegistry;

/**
 * @author hal.hildebrand
 *
 */
abstract public class Router {
    public class CommonCommunications<Client extends Link, Service>
            implements BiFunction<Member, SigningMember, Client> {
        private final CreateClientCommunications<Client> createFunction;
        private final Client                             localLoopback;
        private final RoutableService<Service>           routing;

        public CommonCommunications(RoutableService<Service> routing, CreateClientCommunications<Client> createFunction,
                Client localLoopback) {
            this.routing = routing;
            this.createFunction = createFunction;
            this.localLoopback = localLoopback;
        }

        @Override
        public Client apply(Member to, SigningMember from) {
            return to.equals(from) ? localLoopback : cache.borrow(to, from, createFunction);
        }

        public void deregister(Digest context) {
            routing.unbind(context);
        }

        public void register(Digest context, Service service) {
            routing.bind(context, service);
        }
    }

    private final static Logger log = LoggerFactory.getLogger(Router.class);

    public static ForkJoinPool createFjPool() {
        return createFjPool(log);
    }

    public static ForkJoinPool createFjPool(Logger logger) {
        return new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory, Utils.uncaughtHandler(logger), false);
    }

    private final ServerConnectionCache             cache;
    private final MutableHandlerRegistry            registry;
    private final Map<Class<?>, RoutableService<?>> services = new ConcurrentHashMap<>();

    public Router(ServerConnectionCache cache, MutableHandlerRegistry registry) {
        this.cache = cache;
        this.registry = registry;
    }

    public void close() {
        cache.close();
    }

    public <Client extends Link, Service> CommonCommunications<Client, Service> create(Member member, Digest context,
                                                                                       Service service,
                                                                                       Function<RoutableService<Service>, BindableService> factory,
                                                                                       CreateClientCommunications<Client> createFunction,
                                                                                       Client localLoopback) {
        @SuppressWarnings("unchecked")
        RoutableService<Service> routing = (RoutableService<Service>) services.computeIfAbsent(service.getClass(),
                                                                                               c -> {
                                                                                                   RoutableService<Service> route = new RoutableService<Service>();
                                                                                                   BindableService bindableService = factory.apply(route);
                                                                                                   registry.addService(bindableService);
                                                                                                   return route;
                                                                                               });
        routing.bind(context, service);
        log.info("Communications created for: " + member.getId());
        return new CommonCommunications<Client, Service>(routing, createFunction, localLoopback);
    }

    abstract public ClientIdentity getClientIdentityProvider();

    abstract public void start();
}
