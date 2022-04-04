/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.TransactionSubmissionGrpc.TransactionSubmissionImplBase;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class TxnSubmitServer extends TransactionSubmissionImplBase {
    private ClientIdentity                   identity;
    @SuppressWarnings("unused")
    private final ChoamMetrics               metrics;
    private final RoutableService<Submitter> router;

    public TxnSubmitServer(ClientIdentity identity, ChoamMetrics metrics, RoutableService<Submitter> router) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = router;
    }

    @Override
    public void submit(SubmitTransaction request, StreamObserver<SubmitResult> responseObserver) {
        router.evaluate(responseObserver, request.hasContext() ? new Digest(request.getContext()) : null, s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            try {
                responseObserver.onNext(s.submit(request, from));
                responseObserver.onCompleted();
            } catch (StatusRuntimeException e) {
                responseObserver.onError(e);
            }
        });
    }
}
