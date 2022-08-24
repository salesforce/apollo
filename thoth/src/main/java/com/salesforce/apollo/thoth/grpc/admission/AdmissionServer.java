/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.admission;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.thoth.proto.AdmissionsGrpc.AdmissionsImplBase;
import com.salesfoce.apollo.thoth.proto.Admittance;
import com.salesfoce.apollo.thoth.proto.Registration;
import com.salesfoce.apollo.thoth.proto.SignedAttestation;
import com.salesfoce.apollo.thoth.proto.SignedNonce;
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
public class AdmissionServer extends AdmissionsImplBase {
    private final static Logger log = LoggerFactory.getLogger(AdmissionServer.class);

    private final Executor                   exec;
    private final ClientIdentity             identity;
    @SuppressWarnings("unused")
    private final StereotomyMetrics          metrics;
    private final RoutableService<Admission> router;

    public AdmissionServer(RoutableService<Admission> router, ClientIdentity identity, Executor exec,
                           StereotomyMetrics metrics) {
        this.metrics = metrics;
        this.router = router;
        this.exec = exec;
        this.identity = identity;
    }

    @Override
    public void apply(Registration request, StreamObserver<SignedNonce> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var nonce = s.apply(request, from);
            responseObserver.onNext(nonce);
            responseObserver.onCompleted();
        }), log));
    }

    @Override
    public void register(SignedAttestation request, StreamObserver<Admittance> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var admittance = s.register(request, from);
            responseObserver.onNext(admittance);
            responseObserver.onCompleted();
        }), log));
    }
}
