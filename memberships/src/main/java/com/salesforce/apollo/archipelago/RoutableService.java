/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.salesforce.apollo.archipelago.server.FernetServerInterceptor;
import com.salesforce.apollo.cryptography.Digest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.salesforce.apollo.archipelago.Constants.SERVER_CONTEXT_KEY;

/**
 * Service implementation routable by Digest context
 *
 * @author hal.hildebrand
 */
public class RoutableService<Service> {
    private static final Logger                               log      = LoggerFactory.getLogger(RoutableService.class);
    private final        Map<Digest, ServiceBinding<Service>> services = new ConcurrentHashMap<>();

    public void bind(Digest context, Service service, Predicate<FernetServerInterceptor.HashedToken> validator) {
        services.put(context, new ServiceBinding<>(service, validator));
    }

    public void evaluate(StreamObserver<?> responseObserver, Consumer<Service> c) {
        var context = SERVER_CONTEXT_KEY.get();
        if (context == null) {
            responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
            log.error("Null context");
        } else {
            var binding = services.get(context);
            if (binding == null) {
                log.trace("No service for context {}", context);
                responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
            } else {
                try {
                    if (binding.validator != null) {
                        var token = FernetServerInterceptor.AccessTokenContextKey.get();
                        if (!binding.validator.test(token)) {
                            log.info("Unauthenticated on: {}", context);
                            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
                            return;
                        }
                    }
                    c.accept(binding.service);
                } catch (RejectedExecutionException e) {
                    log.debug("Rejected execution context: {} on: {}", context, c);
                } catch (Throwable t) {
                    log.error("Uncaught exception in service evaluation for context: {}", context, t);
                    responseObserver.onError(t);
                }
            }
        }
    }

    public void evaluate(StreamObserver<?> responseObserver,
                         BiConsumer<Service, FernetServerInterceptor.HashedToken> c) {
        var context = SERVER_CONTEXT_KEY.get();
        if (context == null) {
            responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
            log.error("Null context");
        } else {
            var binding = services.get(context);
            if (binding == null) {
                log.trace("No service for context {}", context);
                responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND));
            } else {
                try {
                    if (binding.validator != null) {
                        var ht = FernetServerInterceptor.AccessTokenContextKey.get();
                        if (!binding.validator.test(ht)) {
                            log.info("Unauthenticated on: {}", context);
                            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
                            return;
                        }
                    }
                    c.accept(binding.service, FernetServerInterceptor.AccessTokenContextKey.get());
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

    private record ServiceBinding<Service>(Service service, Predicate<FernetServerInterceptor.HashedToken> validator) {
    }
}
