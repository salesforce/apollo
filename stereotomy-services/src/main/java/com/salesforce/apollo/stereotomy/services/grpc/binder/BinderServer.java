/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.binder;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.stereotomy.event.proto.Binding;
import com.salesforce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.grpc.proto.BinderGrpc.BinderImplBase;
import com.salesforce.apollo.stereotomy.services.proto.ProtoBinder;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public class BinderServer extends BinderImplBase {

    private final StereotomyMetrics            metrics;
    private final RoutableService<ProtoBinder> routing;
    private final ClientIdentity               identity;

    public BinderServer(RoutableService<ProtoBinder> router, ClientIdentity identity, StereotomyMetrics metrics) {
        this.metrics = metrics;
        this.identity = identity;
        this.routing = router;
    }

    @Override
    public void bind(Binding request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.bindService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundBindRequest().mark(request.getSerializedSize());
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        routing.evaluate(responseObserver, s -> {
            s.bind(request).whenComplete((b, t) -> {
                if (timer != null) {
                    timer.stop();
                }
                if (t != null) {
                    responseObserver.onError(t);
                } else {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                }
            });
        });
    }

    @Override
    public void unbind(Ident request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.unbindService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundUnbindRequest().mark(request.getSerializedSize());
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        routing.evaluate(responseObserver, s -> {
            s.unbind(request).whenComplete((b, t) -> {
                if (timer != null) {
                    timer.stop();
                }
                if (t != null) {
                    responseObserver.onError(t);
                } else {
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                }
            });
        });
    }

}
