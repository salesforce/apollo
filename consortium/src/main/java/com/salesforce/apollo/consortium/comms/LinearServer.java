/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.LinearServiceGrpc.LinearServiceImplBase;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class LinearServer extends LinearServiceImplBase {
    @SuppressWarnings("unused")
    private ClientIdentity                 identity;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics        metrics;
    private final RoutableService<Service> router;

    public LinearServer(ClientIdentity identity, ConsortiumMetrics metrics, RoutableService<Service> router) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = router;
    }

    @Override
    public void join(Join request, StreamObserver<JoinResult> responseObserver) {
        router.evaluate(responseObserver, request.hasContext() ? new Digest(request.getContext()) : null, s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            responseObserver.onNext(s.join(request, from));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void stopData(StopData request, StreamObserver<Empty> responseObserver) {
        router.evaluate(responseObserver, request.hasContext() ? new Digest(request.getContext()) : null, s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            s.stopData(request, from);
        });
    }

    @Override
    public void submit(SubmitTransaction request, StreamObserver<TransactionResult> responseObserver) {
        router.evaluate(responseObserver, request.hasContext() ? new Digest(request.getContext()) : null, s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            responseObserver.onNext(s.clientSubmit(request, from));
            responseObserver.onCompleted();
        });
    }
}
