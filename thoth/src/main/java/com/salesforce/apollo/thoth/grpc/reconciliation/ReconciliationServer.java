/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.reconciliation;

import com.google.protobuf.Empty;
import com.salesforce.apollo.thoth.proto.Intervals;
import com.salesforce.apollo.thoth.proto.ReconciliationGrpc.ReconciliationImplBase;
import com.salesforce.apollo.thoth.proto.Update;
import com.salesforce.apollo.thoth.proto.Updating;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public class ReconciliationServer extends ReconciliationImplBase {
    private final ClientIdentity                  identity;
    @SuppressWarnings("unused")
    private final StereotomyMetrics               metrics;
    private final RoutableService<Reconciliation> router;

    public ReconciliationServer(RoutableService<Reconciliation> router, ClientIdentity identity,
                                StereotomyMetrics metrics) {
        this.metrics = metrics;
        this.router = router;
        this.identity = identity;
    }

    @Override
    public void reconcile(Intervals request, StreamObserver<Update> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        router.evaluate(responseObserver, s -> {
            var update = s.reconcile(request, from);
            responseObserver.onNext(update);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void update(Updating request, StreamObserver<Empty> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        router.evaluate(responseObserver, s -> {
            s.update(request, from);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

}
