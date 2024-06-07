/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.netflix.concurrency.limits.Limit;
import com.salesforce.apollo.archipelago.server.FernetServerInterceptor;
import com.salesforce.apollo.protocols.LimitsRegistry;
import io.grpc.ServerInterceptor;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author hal.hildebrand
 */
public interface RouterSupplier {
    static ExecutorService newCachedThreadPool(int corePoolSize, ThreadFactory threadFactory) {
        return newCachedThreadPool(corePoolSize, threadFactory, true);
    }

    static ExecutorService newCachedThreadPool(int corePoolSize, ThreadFactory threadFactory, boolean preStart) {
        var threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                                                        new SynchronousQueue<Runnable>(), threadFactory);
        if (preStart) {
            threadPoolExecutor.prestartAllCoreThreads();
        }
        return threadPoolExecutor;
    }

    default Router router() {
        return router(ServerConnectionCache.newBuilder(), RouterImpl::defaultServerLimit, null);
    }

    default Router router(ServerConnectionCache.Builder cacheBuilder) {
        return router(cacheBuilder, RouterImpl::defaultServerLimit, null);
    }

    default Router router(ServerConnectionCache.Builder cacheBuilder, ExecutorService executor) {
        return router(cacheBuilder, RouterImpl::defaultServerLimit, null, Collections.emptyList(), null, executor);
    }

    default Router router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit,
                          LimitsRegistry limitsRegistry) {
        return router(cacheBuilder, serverLimit, limitsRegistry, Collections.emptyList());
    }

    default Router router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit,
                          LimitsRegistry limitsRegistry, List<ServerInterceptor> interceptors) {
        return router(cacheBuilder, serverLimit, limitsRegistry, interceptors, null);
    }

    default Router router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit,
                          LimitsRegistry limitsRegistry, List<ServerInterceptor> interceptors,
                          Predicate<FernetServerInterceptor.HashedToken> validator) {
        return router(cacheBuilder, serverLimit, limitsRegistry, interceptors, validator, null);

    }

    Router router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit,
                  LimitsRegistry limitsRegistry, List<ServerInterceptor> interceptors,
                  Predicate<FernetServerInterceptor.HashedToken> validator, ExecutorService executor);
}
