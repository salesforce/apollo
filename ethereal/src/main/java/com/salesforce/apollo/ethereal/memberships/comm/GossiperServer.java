/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships.comm;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.ethereal.proto.ContextUpdate;
import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.GossiperGrpc.GossiperImplBase;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.utils.Utils;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class GossiperServer extends GossiperImplBase {
    private static final Logger                    log = LoggerFactory.getLogger(GossiperServer.class);
    private ClientIdentity                         identity;
    private final EtherealMetrics                  metrics;
    private final RoutableService<GossiperService> routing;
    private final Executor                         exec;

    public GossiperServer(ClientIdentity identity, EtherealMetrics metrics, RoutableService<GossiperService> r,
                          Executor exec) {
        this.metrics = metrics;
        this.identity = identity;
        this.routing = r;
        this.exec = exec;
    }

    @Override
    public void gossip(Gossip request, StreamObserver<Update> responseObserver) {
        Context timer = metrics != null ? metrics.inboundGossipTimer().time() : null;
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGossip().mark(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Update response = s.gossip(request, from);
            if (timer != null) {
                timer.stop();
                var serializedSize = response.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.gossipReply().mark(serializedSize);
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }), log));
    }

    @Override
    public void update(ContextUpdate request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics == null ? null : metrics.inboundUpdateTimer().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundUpdate().mark(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            s.update(request, from);
            if (timer != null) {
                timer.stop();
            }
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        }), log));
    }

}
