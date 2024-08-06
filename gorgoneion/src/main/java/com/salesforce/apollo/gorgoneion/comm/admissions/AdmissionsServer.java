/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.admissions;

import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.gorgoneion.comm.GorgoneionMetrics;
import com.salesforce.apollo.gorgoneion.proto.AdmissionsGrpc.AdmissionsImplBase;
import com.salesforce.apollo.gorgoneion.proto.Credentials;
import com.salesforce.apollo.gorgoneion.proto.Establishment;
import com.salesforce.apollo.gorgoneion.proto.SignedNonce;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.stereotomy.event.proto.KERL_;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public class AdmissionsServer extends AdmissionsImplBase {

    private final ClientIdentity                     identity;
    private final GorgoneionMetrics                  metrics;
    private final RoutableService<AdmissionsService> router;

    public AdmissionsServer(ClientIdentity identity, RoutableService<AdmissionsService> r, GorgoneionMetrics metrics) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = r;
    }

    @Override
    public void apply(KERL_ application, StreamObserver<SignedNonce> responseObserver) {
        if (metrics != null) {
            var serializedSize = application.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundApplication().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            s.apply(application, from, responseObserver, null);
        });
    }

    @Override
    public void register(Credentials request, StreamObserver<Establishment> responseObserver) {
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
