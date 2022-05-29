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

import com.salesfoce.apollo.thoth.proto.Signatures;
import com.salesfoce.apollo.thoth.proto.ValidateContext;
import com.salesfoce.apollo.thoth.proto.Validated;
import com.salesfoce.apollo.thoth.proto.ValidatorGrpc.ValidatorImplBase;
import com.salesfoce.apollo.thoth.proto.WitnessContext;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.thoth.Sakshi;
import com.salesforce.apollo.utils.Utils;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class ValidatorServer extends ValidatorImplBase {
    private final static Logger log = LoggerFactory.getLogger(ReconciliationServer.class);

    private final Executor                exec;
    @SuppressWarnings("unused")
    private final StereotomyMetrics       metrics;
    private final RoutableService<Sakshi> router;

    public ValidatorServer(RoutableService<Sakshi> router, Executor exec, StereotomyMetrics metrics) {
        this.metrics = metrics;
        this.router = router;
        this.exec = exec;
    }

    @Override
    public void validate(ValidateContext request, StreamObserver<Validated> responseObserver) {
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var update = s.validate(request.getEventsList());
            responseObserver.onNext(update);
            responseObserver.onCompleted();
        }), log));
    }

    @Override
    public void witness(WitnessContext request, StreamObserver<Signatures> responseObserver) {
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var update = s.witness(request.getEventsList(), request.getIdentifier());
            responseObserver.onNext(update);
            responseObserver.onCompleted();
        }), log));
    }
}
