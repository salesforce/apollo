/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.rbc.comms;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesfoce.apollo.messaging.proto.RBCGrpc.RBCImplBase;
import com.salesfoce.apollo.messaging.proto.Reconcile;
import com.salesfoce.apollo.messaging.proto.ReconcileContext;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.messaging.rbc.RbcMetrics;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Service;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.utils.Utils;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class RbcServer extends RBCImplBase {
    private final Executor                 exec;
    private ClientIdentity                 identity;
    private final Logger                   log = LoggerFactory.getLogger(RbcServer.class);
    private final RbcMetrics               metrics;
    private final RoutableService<Service> routing;

    public RbcServer(ClientIdentity identity, RbcMetrics metrics, RoutableService<Service> r, Executor exec) {
        this.metrics = metrics;
        this.identity = identity;
        this.routing = r;
        this.exec = exec;
    }

    public ClientIdentity getClientIdentity() {
        return identity;
    }

    @Override
    public void gossip(MessageBff request, StreamObserver<Reconcile> responseObserver) {
        Context timer = metrics == null ? null : metrics.inboundGossipTimer().time();
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
        exec.execute(Utils.wrapped(() -> {
            routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
                try {
                    Reconcile response = s.gossip(request, from);
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    if (metrics != null) {
                        var serializedSize = response.getSerializedSize();
                        metrics.outboundBandwidth().mark(serializedSize);
                        metrics.gossipReply().mark(serializedSize);
                    }
                } finally {
                    if (timer != null) {
                        timer.stop();
                    }
                }
            });
        }, log));
    }

    @Override
    public void update(ReconcileContext request, StreamObserver<Empty> responseObserver) {
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
        exec.execute(Utils.wrapped(() -> {
            routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
                try {
                    s.update(request, from);
                    responseObserver.onNext(Empty.getDefaultInstance());
                    responseObserver.onCompleted();
                } finally {
                    if (timer != null) {
                        timer.stop();
                    }
                }
            });
        }, log));
    }

}
