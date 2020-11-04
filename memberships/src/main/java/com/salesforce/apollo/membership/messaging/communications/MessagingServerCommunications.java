/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.communications;

import com.salesfoce.apollo.proto.MessageBff;
import com.salesfoce.apollo.proto.Messages;
import com.salesfoce.apollo.proto.MessagingGrpc.MessagingImplBase;
import com.salesfoce.apollo.proto.Null;
import com.salesfoce.apollo.proto.Push;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.membership.messaging.MessagingMetrics;
import com.salesforce.apollo.membership.messaging.Messenger.Service;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class MessagingServerCommunications extends MessagingImplBase {
    @Override
    public void gossip(MessageBff request, StreamObserver<Messages> responseObserver) {
        routing.evaluate(responseObserver, request.getContext(), s -> {
            Messages response = s.gossip(request, identity.getFrom());
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
    public void update(Push request, StreamObserver<Null> responseObserver) {
        routing.evaluate(responseObserver, request.getContext(), s -> {
            s.update(request, identity.getFrom());
            responseObserver.onNext(Null.getDefaultInstance());
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

    public MessagingServerCommunications(ClientIdentity identity, MessagingMetrics metrics,
            RoutableService<Service> routing) {
        this.metrics = metrics;
        this.identity = identity;
        this.routing = routing;
    }

    public ClientIdentity getClientIdentity() {
        return identity;
    }

}
