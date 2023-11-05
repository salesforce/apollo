/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.salesforce.apollo.crypto.Digest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.salesforce.apollo.archipelago.Router.SERVER_CONTEXT_KEY;

/**
 * Service implementation routable by Digest context
 *
 * @author hal.hildebrand
 */
public class RoutableService<Service> {
    private static final Logger               log      = LoggerFactory.getLogger(RoutableService.class);
    private final        Map<Digest, Service> services = new ConcurrentHashMap<>();

    public void bind(Digest context, Service service) {
        services.put(context, service);
    }

    public void evaluate(StreamObserver<?> responseObserver, Consumer<Service> c) {
        var context = SERVER_CONTEXT_KEY.get();
        if (context == null) {
            responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
            log.error("Null context");
            return;
        } else {
            Service service = services.get(context);
            if (service == null) {
                log.trace("No service for context {}", context);
                responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
            } else {
                try {
                    c.accept(service);
                } catch (Throwable t) {
                    log.error("Uncaught exception in service evaluation for context: {}", context, t);
                    responseObserver.onError(t);
                }
            }
        }
    }

    public void unbind(Digest context) {
        services.remove(context);
    }
}
