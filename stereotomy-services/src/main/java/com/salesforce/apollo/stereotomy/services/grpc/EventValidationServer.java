/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.util.concurrent.CompletableFuture;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.BoolValue;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.ValidatorGrpc.ValidatorImplBase;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventValidation;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class EventValidationServer extends ValidatorImplBase {
    private ClientIdentity                              identity;
    private final StereotomyMetrics                     metrics;
    private final RoutableService<ProtoEventValidation> routing;

    public EventValidationServer(ClientIdentity identity, StereotomyMetrics metrics,
                                 RoutableService<ProtoEventValidation> router) {
        this.metrics = metrics;
        this.identity = identity;
        this.routing = router;
    }

    @Override
    public void validate(KeyEventContext request, StreamObserver<BoolValue> responseObserver) {
        Context timer = metrics != null ? metrics.validatorService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundValidatorRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            CompletableFuture<Boolean> result = s.validate(request.getKeyEvent());
            result.whenComplete((r, t) -> {
                if (timer != null) {
                    timer.stop();
                }
                if (t != null) {
                    responseObserver.onError(t);
                } else {
                    responseObserver.onNext(BoolValue.newBuilder().setValue(r).build());
                    responseObserver.onCompleted();
                }
            });
        });
    }
}
