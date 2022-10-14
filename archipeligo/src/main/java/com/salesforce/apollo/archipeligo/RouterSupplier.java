/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

import com.netflix.concurrency.limits.Limit;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.LimitsRegistry;

/**
 * @author hal.hildebrand
 *
 * @param <To>
 */
public interface RouterSupplier<To extends Member> {

    default Router<To> router(Executor executor) {
        return router(ServerConnectionCache.newBuilder(), () -> Router.defaultServerLimit(), executor, null);
    }

    default Router<To> router(ServerConnectionCache.Builder<To> cacheBuilder, Executor executor) {
        return router(cacheBuilder, () -> Router.defaultServerLimit(), executor, null);
    }

    Router<To> router(ServerConnectionCache.Builder<To> cacheBuilder, Supplier<Limit> serverLimit, Executor executor,
                      LimitsRegistry limitsRegistry);

}
