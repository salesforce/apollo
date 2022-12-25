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
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class SigningClient implements SigningService {

    private final ManagedServerChannel       channel;
    private final SigningServiceBlockingStub client;
    private final SigningMetrics             metrics;

    public SigningClient(ManagedServerChannel channel, SigningMetrics metrics) {
        this.metrics = metrics;
        this.channel = channel;
        client = SigningServiceGrpc.newBlockingStub(channel).withCompression("gzip");
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return channel.getMember();
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
