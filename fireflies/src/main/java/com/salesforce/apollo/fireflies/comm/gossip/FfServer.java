/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.gossip;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.fireflies.FireflyMetrics;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.fireflies.proto.FirefliesGrpc.FirefliesImplBase;
import com.salesforce.apollo.fireflies.proto.*;
import com.salesforce.apollo.protocols.ClientIdentity;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public class FfServer extends FirefliesImplBase {
    private final ClientIdentity           identity;
    private final FireflyMetrics           metrics;
    private final RoutableService<Service> router;

    public FfServer(ClientIdentity identity, RoutableService<Service> r, FireflyMetrics metrics) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = r;
    }

    @Override
    public void enjoin(Join request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics == null ? null : metrics.inboundEnjoinDuration().time();
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
        router.evaluate(responseObserver, s -> {
            s.enjoin(request, from);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            if (timer != null) {
                timer.stop();
            }
        });
    }

    @Override
    public void gossip(SayWhat request, StreamObserver<Gossip> responseObserver) {
        Context timer = metrics == null ? null : metrics.inboundGossipDuration().time();
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
        router.evaluate(responseObserver, s -> {
            Gossip gossip;
            try {
                gossip = s.rumors(request, from);
            } catch (StatusRuntimeException e) {
                responseObserver.onError(e);
                return;
            }
            responseObserver.onNext(gossip);
            responseObserver.onCompleted();
            if (timer != null) {
                var serializedSize = gossip.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.gossipReply().update(serializedSize);
                timer.stop();
            }
        });
    }

    @Override
    public void ping(Ping request, StreamObserver<Empty> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            try {
                s.ping(request, from);
            } catch (StatusRuntimeException e) {
                responseObserver.onError(e);
                return;
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void update(State request, StreamObserver<Empty> responseObserver) {
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
        router.evaluate(responseObserver, s -> {
            try {
                try {
                    s.update(request, from);
                } catch (StatusRuntimeException e) {
                    responseObserver.onError(e);
                    return;
                }
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (StatusRuntimeException e) {
                responseObserver.onError(e);
            }
            if (timer != null) {
                timer.stop();
            }
        });
    }
}
