/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;

import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.function.Consumer;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public interface BaseServerCommunications<T> {
    default void evaluate(StreamObserver<?> responseObserver, Digest id, Consumer<T> c, T s, Map<Digest, T> services) {
        T service = getService(id, s, services);
        if (service == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN));
        } else {
            c.accept(service);
        }
    }

    default void evaluate(StreamObserver<?> responseObserver, String id, Consumer<T> c, T s, Map<Digest, T> services) {
        evaluate(responseObserver, digest(id), c, s, services);
    }

    default X509Certificate getCert() {
        return getClientIdentity().getCert();
    }

    ClientIdentity getClientIdentity();

    default Digest getFrom() {
        return getClientIdentity().getFrom();
    }

    default T getService(Digest context, T system, Map<Digest, T> services) {
        return (context == null && system != null) ? system : services.get(context);
    }

    void register(Digest id, T service);

}
