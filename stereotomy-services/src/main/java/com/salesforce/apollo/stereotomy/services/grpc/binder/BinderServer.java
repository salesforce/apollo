/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.binder;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.BindContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.BinderGrpc.BinderImplBase;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoBinder;
import com.salesforce.apollo.utils.Utils;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class BinderServer extends BinderImplBase {
    private final static Logger log = LoggerFactory.getLogger(BinderServer.class);

    private ClientIdentity                     identity;
    private final StereotomyMetrics            metrics;
    private final RoutableService<ProtoBinder> routing;
    private final Executor                     exec;

    public BinderServer(RoutableService<ProtoBinder> router, ClientIdentity identity, Executor exec,
                        StereotomyMetrics metrics) {
        this.metrics = metrics;
        this.identity = identity;
        this.routing = router;
        this.exec = exec;
    }

    @Override
    public void bind(BindContext request, StreamObserver<Empty> responseObserver) {
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
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var result = s.bind(request.getBinding());
            result.whenComplete((b, t) -> {
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
        }), log));
    }

    @Override
    public void unbind(IdentifierContext request, StreamObserver<Empty> responseObserver) {
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
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var result = s.unbind(request.getIdentifier());
            result.whenComplete((b, t) -> {
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
        }), log));
    }

}
