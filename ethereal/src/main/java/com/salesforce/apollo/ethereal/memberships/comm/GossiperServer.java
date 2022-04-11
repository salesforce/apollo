/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships.comm;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.ethereal.proto.ContextUpdate;
import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.GossiperGrpc.GossiperImplBase;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class GossiperServer extends GossiperImplBase {
    private ClientIdentity                         identity;
    private final EtherealMetrics                  metrics;
    private final RoutableService<GossiperService> routing;

    public GossiperServer(ClientIdentity identity, EtherealMetrics metrics, RoutableService<GossiperService> r) {
        this.metrics = metrics;
        this.identity = identity;
        this.routing = r;
    }

    @Override
    public void gossip(Gossip request, StreamObserver<Update> responseObserver) {
        Context timer = metrics != null ? metrics.inboundGossipTimer().time() : null;
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGossip().mark(serializedSize);
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            Update response = s.gossip(request, from);
            if (timer != null) {
                timer.stop();
                var serializedSize = response.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.gossipReply().mark(serializedSize);
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void update(ContextUpdate request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics == null ? null : metrics.inboundUpdateTimer().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundUpdate().mark(serializedSize);
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            s.update(request, from);
            if (timer != null) {
                timer.stop();
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

}
