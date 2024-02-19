/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.macasaet.fernet.Token;
import com.netflix.concurrency.limits.Limit;
import com.salesforce.apollo.protocols.LimitsRegistry;
import io.grpc.ServerInterceptor;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author hal.hildebrand
 */
public interface RouterSupplier {

    default Router router() {
        return router(ServerConnectionCache.newBuilder(), RouterImpl::defaultServerLimit, null);
    }

    default Router router(ServerConnectionCache.Builder cacheBuilder) {
        return router(cacheBuilder, RouterImpl::defaultServerLimit, null);
    }

    default Router router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit,
                          LimitsRegistry limitsRegistry) {
        return router(cacheBuilder, serverLimit, limitsRegistry, Collections.emptyList());
    }

    default Router router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit,
                          LimitsRegistry limitsRegistry, List<ServerInterceptor> interceptors) {
        return router(cacheBuilder, serverLimit, limitsRegistry, interceptors, null);
    }

    Router router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit,
                  LimitsRegistry limitsRegistry, List<ServerInterceptor> interceptors, Predicate<Token> validator);

}
