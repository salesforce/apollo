/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import com.salesfoce.apollo.proto.OrdererGrpc.OrdererImplBase;
import com.salesfoce.apollo.proto.Submission;
import com.salesfoce.apollo.proto.SubmitResolution;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.Ordering;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class OrderingServerCommunications extends OrdererImplBase {

    @SuppressWarnings("unused")
    private final ClientIdentity identity;
    private final Ordering       service;

    public OrderingServerCommunications(Ordering service, ClientIdentity identity) {
        this.service = service;
        this.identity = identity;
    }

    @Override
    public void submit(Submission request, StreamObserver<SubmitResolution> responseObserver) {
        responseObserver.onNext(service.submit(request));
        responseObserver.onCompleted();
    }

}
