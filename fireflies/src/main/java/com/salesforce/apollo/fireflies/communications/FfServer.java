/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.fireflies.proto.FirefliesGrpc.FirefliesImplBase;
import com.salesfoce.apollo.fireflies.proto.Gossip;
import com.salesfoce.apollo.fireflies.proto.Null;
import com.salesfoce.apollo.fireflies.proto.SayWhat;
import com.salesfoce.apollo.fireflies.proto.State;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.fireflies.FireflyMetrics;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class FfServer extends FirefliesImplBase {
    private ClientIdentity                 identity;
    private final FireflyMetrics           metrics;
    private final RoutableService<Service> router;

    public FfServer(Service system, ClientIdentity identity, FireflyMetrics metrics, RoutableService<Service> router) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = router;
    }

    @Override
    public void gossip(SayWhat request, StreamObserver<Gossip> responseObserver) {
        router.evaluate(responseObserver, request.getContext(), s -> {
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
                Gossip gossip = s.rumors(request.getRing(), request.getGossip(), from, identity.getCert(),
                                         request.getNote());
                responseObserver.onNext(gossip);
                responseObserver.onCompleted();
                if (metrics != null) {
                    metrics.inboundGossipRate().mark();
                    metrics.inboundBandwidth().mark(request.getSerializedSize());
                    metrics.outboundBandwidth().mark(gossip.getSerializedSize());
                    metrics.inboundGossip().update(request.getSerializedSize());
                    metrics.gossipReply().update(gossip.getSerializedSize());
                }
            } finally {
                if (timer != null) {
                    timer.stop();
                }
            }
        });
    }

    @Override
    public void ping(Null request, StreamObserver<Null> responseObserver) {
        router.evaluate(responseObserver, request.getContext(), s -> {
            responseObserver.onNext(Null.getDefaultInstance());
            responseObserver.onCompleted();
            if (metrics != null) {
                metrics.inboundPingRate().mark();
            }
        });
    }

    @Override
    public void update(State request, StreamObserver<Null> responseObserver) {
        router.evaluate(responseObserver, request.getContext(), s -> {
            Context timer = null;
            if (metrics != null) {
                timer = metrics.inboundUpdateTimer().time();
            }
            try {
                Digest from = identity.getFrom();
                if (from == null) {
                    responseObserver.onError(new IllegalStateException("Member has been removed"));
                    return;
                }
                s.update(request.getRing(), request.getUpdate(), from);
                responseObserver.onNext(Null.getDefaultInstance());
                responseObserver.onCompleted();
                if (metrics != null) {
                    metrics.inboundBandwidth().mark(request.getSerializedSize());
                    metrics.outboundBandwidth().mark(Null.getDefaultInstance().getSerializedSize());
                    metrics.inboundUpdate().update(request.getSerializedSize());
                    metrics.inboundUpdateRate().mark();
                }
            } finally {
                if (timer != null) {
                    timer.stop();
                }
            }
        });
    }

}
