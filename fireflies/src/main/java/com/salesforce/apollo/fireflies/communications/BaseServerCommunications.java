/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.function.Consumer;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public abstract interface BaseServerCommunications<T> {

    default void evaluate(StreamObserver<?> responseObserver, ByteString context, Consumer<T> c, T s,
                          Map<HashKey, T> services) {
        T service = getService(context, s, services);
        if (service == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN));
        } else {
            c.accept(service);
        }
    }

    default X509Certificate getCert() {
        return (X509Certificate) getClientIdentity().getCert();
    }

    ClientIdentity getClientIdentity();

    default HashKey getFrom() {
        return getClientIdentity().getFrom();
    }

    default T getService(ByteString context, T system, Map<HashKey, T> services) {
        return (context.isEmpty() && system != null) ? system : services.get(new HashKey(context));
    }

    void register(Member member, T service);

}
