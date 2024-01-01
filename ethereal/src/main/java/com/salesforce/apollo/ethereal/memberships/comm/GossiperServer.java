/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships.comm;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesforce.apollo.ethereal.proto.ContextUpdate;
import com.salesforce.apollo.ethereal.proto.Gossip;
import com.salesforce.apollo.ethereal.proto.GossiperGrpc.GossiperImplBase;
import com.salesforce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public class GossiperServer extends GossiperImplBase {
    private final EtherealMetrics                  metrics;
    private final RoutableService<GossiperService> routing;
    private       ClientIdentity                   identity;

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
            metrics.inboundGossip().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        routing.evaluate(responseObserver, s -> {
            Update response = s.gossip(request, from);
            if (timer != null) {
                timer.stop();
                var serializedSize = response.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.gossipReply().update(serializedSize);
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
            metrics.inboundUpdate().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        routing.evaluate(responseObserver, s -> {
            s.update(request, from);
            if (timer != null) {
                timer.stop();
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

}
