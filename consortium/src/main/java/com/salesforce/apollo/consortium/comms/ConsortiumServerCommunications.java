/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import com.salesfoce.apollo.consortium.proto.OrderingServiceGrpc.OrderingServiceImplBase;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.ConsortiumMetrics;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class ConsortiumServerCommunications extends OrderingServiceImplBase {
    @SuppressWarnings("unused")
    private ClientIdentity                 identity;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics        metrics;
    private final RoutableService<Service> router;

    public ConsortiumServerCommunications(ClientIdentity identity, ConsortiumMetrics metrics,
            RoutableService<Service> router) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = router;
    }

    @Override
    public void submit(SubmitTransaction request, StreamObserver<TransactionResult> responseObserver) {
        router.evaluate(responseObserver, request.getContext().isEmpty() ? null : new HashKey(request.getContext()),
                        s -> {
                            responseObserver.onNext(s.clientSubmit(request));
                        });
    }

}
