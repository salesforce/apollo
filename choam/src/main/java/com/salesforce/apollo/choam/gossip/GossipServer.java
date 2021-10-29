/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.gossip;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.choam.proto.GossipGrpc.GossipImplBase;
import com.salesfoce.apollo.choam.proto.Have;
import com.salesfoce.apollo.choam.proto.Update;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.comm.RouterMetrics;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class GossipServer extends GossipImplBase {
    @Override
    public void gossip(Have request, StreamObserver<Update> responseObserver) {
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Context timer = null;
            if (metrics != null) {
                timer = metrics.inboundGossipTimer().time();
            }
            try {
                Digest from = identity.getFrom();
                if (from == null) {
                    responseObserver.onError(new IllegalStateException("Member has been removed"));
                    return;
                }
                Update response = s.gossip(request, from);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                if (metrics != null) {
                    metrics.inboundGossipRate().mark();
                    metrics.inboundBandwidth().mark(request.getSerializedSize());
                    metrics.outboundBandwidth().mark(request.getSerializedSize());
                    metrics.inboundGossip().update(request.getSerializedSize());
                    metrics.gossipReply().update(response.getSerializedSize());
                }
            } finally {
                if (timer != null) {
                    timer.stop();
                }
            }
        });
    }

    private ClientIdentity                            identity;
    private final RouterMetrics                       metrics;
    private final RoutableService<GossipService> routing;

    public GossipServer(ClientIdentity identity, RouterMetrics metrics, RoutableService<GossipService> r) {
        this.metrics = metrics;
        this.identity = identity;
        this.routing = r;
    }

    public ClientIdentity getClientIdentity() {
        return identity;
    }

}
