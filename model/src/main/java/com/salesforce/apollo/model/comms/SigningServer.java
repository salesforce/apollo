/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.comms;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.model.proto.Request;
import com.salesfoce.apollo.model.proto.SigningServiceGrpc.SigningServiceImplBase;
import com.salesfoce.apollo.utils.proto.Sig;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class SigningServer extends SigningServiceImplBase {

    private final ClientIdentity identity;
    private final SigningMetrics metrics;
    private final Signer         signer;

    public SigningServer(Signer signer, ClientIdentity identity, SigningMetrics metrics) {
        this.signer = signer;
        this.identity = identity;
        this.metrics = metrics;
    }

    @Override
    public void sign(Request request, StreamObserver<Sig> responseObserver) {
        Context timer = metrics != null ? metrics.inboundSign().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundRequest().mark(request.getSerializedSize());
        }
        var from = identity.getFrom();
        try {
            Sig signature = signer.sign(request, from);
            if (signature == null) {
                responseObserver.onNext(Sig.getDefaultInstance());
            } else {
                responseObserver.onNext(signature);
            }
            responseObserver.onCompleted();
        } finally {
            if (timer != null) {
                timer.close();
            }
        }
    }

}
