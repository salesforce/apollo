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

import com.salesfoce.apollo.gorgoneion.proto.AdmissionsGrpc.AdmissionsImplBase;
import com.salesfoce.apollo.gorgoneion.proto.Registration;
import com.salesfoce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.thoth.metrics.GorgoneionMetrics;
import com.salesforce.apollo.utils.Utils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
    private final GorgoneionMetrics          metrics;
    private final RoutableService<Admission> router;

    public AdmissionServer(RoutableService<Admission> router, ClientIdentity identity, Executor exec,
                           GorgoneionMetrics metrics) {
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
            var nonceF = s.apply(request, from);
            if (nonceF == null) {
                responseObserver.onNext(SignedNonce.getDefaultInstance());
                responseObserver.onCompleted();
                return;
            }
            nonceF.whenComplete((sn, t) -> {
                if (t != null) {
                    log.error("Error in apply", t);
                    responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(t)));
                } else {
                    responseObserver.onNext(sn);
                    responseObserver.onCompleted();
                }
            });
        }), log));
    }

    @Override
    public void register(SignedAttestation request, StreamObserver<Validations> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            s.register(request, from, responseObserver);
        }), log));
    }
}
