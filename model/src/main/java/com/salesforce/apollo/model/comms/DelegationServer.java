/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.comms;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesforce.apollo.demesne.proto.DelegationGrpc.DelegationImplBase;
import com.salesforce.apollo.demesne.proto.DelegationUpdate;
import com.salesforce.apollo.cryptography.proto.Biff;
import com.salesforce.apollo.archipelago.Enclave.RoutingClientIdentity;
import com.salesforce.apollo.archipelago.RoutableService;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public class DelegationServer extends DelegationImplBase {
    private final RoutingClientIdentity              identity;
    private final OuterServerMetrics                 metrics;
    private final RoutableService<DelegationService> router;

    public DelegationServer(RoutingClientIdentity clientIdentity, RoutableService<DelegationService> router,
                            OuterServerMetrics metrics) {
        this.router = router;
        this.identity = clientIdentity;
        this.metrics = metrics;
    }

    @Override
    public void gossip(Biff request, StreamObserver<DelegationUpdate> responseObserver) {
        Context timer = metrics != null ? metrics.updateInbound().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundGossip().mark(request.getSerializedSize());
        }
        var from = identity.getAgent();
        router.evaluate(responseObserver, delegation -> {
            try {
                var update = delegation.gossip(request, from);
                responseObserver.onNext(update);
                responseObserver.onCompleted();
                final var serializedSize = update.getSerializedSize();
                if (timer != null) {
                    metrics.outboundBandwidth().mark(serializedSize);
                    metrics.outboundUpdate().mark(serializedSize);
                }
            } finally {
                if (timer != null) {
                    timer.close();
                }
            }
        });
    }

    @Override
    public void update(DelegationUpdate request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.updateInbound().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundUpdate().mark(request.getSerializedSize());
        }
        var from = identity.getAgent();
        router.evaluate(responseObserver, delegation -> {
            try {
                delegation.update(request, from);
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } finally {
                if (timer != null) {
                    timer.close();
                }
            }
        });
    }
}
