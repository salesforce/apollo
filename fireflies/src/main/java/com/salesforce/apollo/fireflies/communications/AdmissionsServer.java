/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.fireflies.proto.AdmissionsGrpc.AdmissionsImplBase;
import com.salesfoce.apollo.fireflies.proto.Application;
import com.salesfoce.apollo.fireflies.proto.Credentials;
import com.salesfoce.apollo.fireflies.proto.Invitation;
import com.salesfoce.apollo.fireflies.proto.Notarization;
import com.salesfoce.apollo.fireflies.proto.SignedNonce;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.fireflies.FireflyMetrics;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.utils.Utils;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class AdmissionsServer extends AdmissionsImplBase {
    private final static Logger log = LoggerFactory.getLogger(FfServer.class);

    private final Executor exec;

    private ClientIdentity identity;

    private final FireflyMetrics           metrics;
    private final RoutableService<Service> router;

    public AdmissionsServer(Service system, ClientIdentity identity, RoutableService<Service> router, Executor exec,
                            FireflyMetrics metrics) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = router;
        this.exec = exec;
    }

    @Override
    public void apply(Application request, StreamObserver<SignedNonce> responseObserver) {
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundSeed().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            responseObserver.onNext(s.apply(request, from));
            responseObserver.onCompleted();
        }), log));
    }

    @Override
    public void enroll(Notarization request, StreamObserver<Empty> responseObserver) {
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundSeed().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            responseObserver.onNext(s.enroll(request, from));
        }), log));
    }

    @Override
    public void register(Credentials request, StreamObserver<Invitation> responseObserver) {
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundSeed().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
//            s.register(request, from, responseObserver, timer);
        }), log));
    }
}
