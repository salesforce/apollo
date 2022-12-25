/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.comms;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.model.proto.Request;
import com.salesfoce.apollo.model.proto.SigningServiceGrpc;
import com.salesfoce.apollo.model.proto.SigningServiceGrpc.SigningServiceBlockingStub;
import com.salesfoce.apollo.utils.proto.Sig;

import io.grpc.ManagedChannel;

/**
 * @author hal.hildebrand
 *
 */
public class SigningClient implements SigningService {
    private final SigningServiceBlockingStub client;
    private final SigningMetrics             metrics;

    public SigningClient(ManagedChannel managedChannel, SigningMetrics metrics) {
        this.metrics = metrics;
        client = SigningServiceGrpc.newBlockingStub(managedChannel).withCompression("gzip");
    }

    @Override
    public Sig sign(Request request) {
        Context timer = metrics != null ? metrics.inboundSign().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundRequest().mark(serializedSize);
        }
        Sig signature = null;
        try {
            signature = client.sign(request);
            return signature;
        } finally {
            if (timer != null) {
                timer.stop();
                if (signature != null) {
                    final var serializedSize = signature.getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundSignature().mark(serializedSize);
                }
            }
        }
    }
}
