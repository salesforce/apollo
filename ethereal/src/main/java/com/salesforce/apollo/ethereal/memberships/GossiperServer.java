/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.GossiperGrpc.GossiperImplBase;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.comm.RouterMetrics;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class GossiperServer extends GossiperImplBase {
    @Override
    public void gossip(Gossip request, StreamObserver<Update> responseObserver) {
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

    private ClientIdentity                 identity;
    private final RouterMetrics            metrics;
    private final RoutableService<GossiperService> routing;

    public GossiperServer(ClientIdentity identity, RouterMetrics metrics, RoutableService<GossiperService> r) {
        this.metrics = metrics;
        this.identity = identity;
        this.routing = r;
    }

    public ClientIdentity getClientIdentity() {
        return identity;
    }

}
