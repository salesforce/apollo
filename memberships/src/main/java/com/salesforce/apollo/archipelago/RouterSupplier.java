/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

import com.netflix.concurrency.limits.Limit;
import com.salesforce.apollo.protocols.LimitsRegistry;

/**
 * @author hal.hildebrand
 * 
 */
public interface RouterSupplier {

    default Router router(Executor executor) {
        return router(ServerConnectionCache.newBuilder(), () -> RouterImpl.defaultServerLimit(), executor, null);
    }

    default Router router(ServerConnectionCache.Builder cacheBuilder, Executor executor) {
        return router(cacheBuilder, () -> RouterImpl.defaultServerLimit(), executor, null);
    }

    Router router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit, Executor executor,
                  LimitsRegistry limitsRegistry);

}
