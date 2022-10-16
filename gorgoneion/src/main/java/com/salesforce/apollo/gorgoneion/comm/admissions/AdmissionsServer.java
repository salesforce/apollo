/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.admissions;

import com.salesfoce.apollo.gorgoneion.proto.AdmissionsGrpc.AdmissionsImplBase;
import com.salesfoce.apollo.gorgoneion.proto.Application;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.Invitation;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesforce.apollo.archipeligo.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.gorgoneion.comm.GorgoneionMetrics;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class AdmissionsServer extends AdmissionsImplBase {

    private ClientIdentity                           identity;
    private final GorgoneionMetrics                  metrics;
    private final RoutableService<AdmissionsService> router;

    public AdmissionsServer(ClientIdentity identity, RoutableService<AdmissionsService> r, GorgoneionMetrics metrics) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = r;
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
        router.evaluate(responseObserver, s -> {
            s.apply(request, from, responseObserver, null);
        });
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
        router.evaluate(responseObserver, s -> {
            s.register(request, from, responseObserver, timer);
        });
    }
}
