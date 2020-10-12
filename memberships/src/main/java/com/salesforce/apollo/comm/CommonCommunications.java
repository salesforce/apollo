/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.util.function.BiFunction;

import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.membership.Member;

public class CommonCommunications<T> implements BiFunction<Member, Member, T> {
    protected final ServerConnectionCache       cache;
    private final CreateClientCommunications<T> createFunction;

    public CommonCommunications(ServerConnectionCache cache, CreateClientCommunications<T> createFunction) {
        this.cache = cache;
        this.createFunction = createFunction;
    }

    @Override
    public T apply(Member to, Member from) {
        return (T) cache.borrow(to, from, createFunction);
    }
}
