/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.demesnes.comm;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.demesne.proto.OuterContextGrpc;
import com.salesfoce.apollo.demesne.proto.OuterContextGrpc.OuterContextBlockingStub;
import com.salesfoce.apollo.demesne.proto.SubContext;
import com.salesfoce.apollo.utils.proto.Digeste;

import io.grpc.ManagedChannel;

/**
 * @author hal.hildebrand
 *
 */
public class OuterContextClient implements OuterContextService {
    private final OuterContextBlockingStub client;
    private final EnclaveMetrics           metrics;

    public OuterContextClient(ManagedChannel channel, EnclaveMetrics metrics) {
        this.metrics = metrics;
        client = OuterContextGrpc.newBlockingStub(channel).withCompression("gzip");
    }

    @Override
    public void deregister(Digeste context) {
        Context timer = metrics != null ? metrics.deregister().time() : null;
        if (metrics != null) {
            final var serializedSize = context.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundDeregister().mark(serializedSize);
        }

        client.deregister(context);
        if (timer != null) {
            timer.close();
        }
    }

    @Override
    public void register(SubContext context) {
        Context timer = metrics != null ? metrics.register().time() : null;
        if (metrics != null) {
            final var serializedSize = context.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundRegister().mark(serializedSize);
        }

        client.register(context);
        if (timer != null) {
            timer.close();
        }
    }
}
