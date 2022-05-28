/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.thoth.proto.Intervals;
import com.salesfoce.apollo.thoth.proto.ReconciliationGrpc.ReconciliationImplBase;
import com.salesfoce.apollo.thoth.proto.Update;
import com.salesfoce.apollo.thoth.proto.Updating;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.utils.Utils;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class ReconciliationServer extends ReconciliationImplBase {
    private final static Logger log = LoggerFactory.getLogger(ReconciliationServer.class);

    private final Executor                        exec;
    private final ClientIdentity                  identity;
    @SuppressWarnings("unused")
    private final StereotomyMetrics               metrics;
    private final RoutableService<Reconciliation> router;

    public ReconciliationServer(RoutableService<Reconciliation> router, ClientIdentity identity, Executor exec,
                                StereotomyMetrics metrics) {
        this.metrics = metrics;
        this.router = router;
        this.exec = exec;
        this.identity = identity;
    }

    @Override
    public void reconcile(Intervals request, StreamObserver<Update> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var update = s.reconcile(request, from);
            responseObserver.onNext(update);
            responseObserver.onCompleted();
        }), log));
    }

    @Override
    public void update(Updating request, StreamObserver<Empty> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            s.update(request, from);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        }), log));
    }

}
