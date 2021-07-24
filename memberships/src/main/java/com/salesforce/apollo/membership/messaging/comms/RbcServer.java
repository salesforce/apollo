/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.comms;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesfoce.apollo.messaging.proto.RBCGrpc.RBCImplBase;
import com.salesfoce.apollo.messaging.proto.Reconcile;
import com.salesfoce.apollo.messaging.proto.Reconciliation;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.messaging.MessagingMetrics;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Service;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class RbcServer extends RBCImplBase {
    @Override
    public void gossip(MessageBff request, StreamObserver<Reconcile> responseObserver) {
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            Reconcile response = s.gossip(request, from);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            if (metrics != null) {
                metrics.inboundGossipRate().mark();
                metrics.inboundBandwidth().mark(request.getSerializedSize());
                metrics.outboundBandwidth().mark(request.getSerializedSize());
                metrics.inboundGossip().update(request.getSerializedSize());
                metrics.gossipReply().update(response.getSerializedSize());
            }
        });
    }

    @Override
    public void update(Reconciliation request, StreamObserver<Empty> responseObserver) {
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            s.update(request, from);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
            if (metrics != null) {
                metrics.inboundUpdateRate().mark();
                metrics.inboundBandwidth().mark(request.getSerializedSize());
                metrics.inboundUpdate().update(request.getSerializedSize());
            }
        });
    }

    private ClientIdentity                 identity;
    private final MessagingMetrics         metrics;
    private final RoutableService<Service> routing;

    public RbcServer(ClientIdentity identity, MessagingMetrics metrics, RoutableService<Service> r) {
        this.metrics = metrics;
        this.identity = identity;
        this.routing = r;
    }

    public ClientIdentity getClientIdentity() {
        return identity;
    }

}
