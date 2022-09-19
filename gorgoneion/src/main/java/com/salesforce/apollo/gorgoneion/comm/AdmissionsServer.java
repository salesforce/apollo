/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.gorgoneion.proto.AdmissionsGrpc.AdmissionsImplBase;
import com.salesfoce.apollo.gorgoneion.proto.Application;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.Invitation;
import com.salesfoce.apollo.gorgoneion.proto.Notarization;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.utils.Utils;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class AdmissionsServer extends AdmissionsImplBase {
    private final static Logger log = LoggerFactory.getLogger(AdmissionsServer.class);

    private final Executor                           exec;
    private ClientIdentity                           identity;
    private final GorgoneionMetrics                  metrics;
    private final RoutableService<AdmissionsService> router;

    public AdmissionsServer(ClientIdentity identity, RoutableService<AdmissionsService> r, Executor exec,
                            GorgoneionMetrics metrics) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = r;
        this.exec = exec;
    }

    @Override
    public void apply(Application request, StreamObserver<SignedNonce> responseObserver) {
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundApplication().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            s.apply(request, from, responseObserver, null);
        }), log));
    }

    @Override
    public void enroll(Notarization request, StreamObserver<Empty> responseObserver) {
        var timer = metrics == null ? null : metrics.enrollDuration().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundEnroll().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            s.enroll(request, from, responseObserver, timer);
        }), log));
    }

    @Override
    public void register(Credentials request, StreamObserver<Invitation> responseObserver) {
        var timer = metrics == null ? null : metrics.registerDuration().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundCredentials().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            s.register(request, from, responseObserver, timer);
        }), log));
    }
}
