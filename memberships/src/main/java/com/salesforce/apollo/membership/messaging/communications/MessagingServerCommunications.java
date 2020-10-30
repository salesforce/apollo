/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.communications;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.salesfoce.apollo.proto.MessageBff;
import com.salesfoce.apollo.proto.Messages;
import com.salesfoce.apollo.proto.MessagingGrpc.MessagingImplBase;
import com.salesfoce.apollo.proto.Null;
import com.salesfoce.apollo.proto.Push;
import com.salesforce.apollo.comm.grpc.BaseServerCommunications;
import com.salesforce.apollo.membership.messaging.MessagingMetrics;
import com.salesforce.apollo.membership.messaging.Messenger.Service;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class MessagingServerCommunications extends MessagingImplBase implements BaseServerCommunications<Service> {
    @Override
    public void gossip(MessageBff request, StreamObserver<Messages> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            Messages response = s.gossip(request);
            if (metrics != null) {
                metrics.inboundGossipRate().mark();
                metrics.inboundBandwidth().mark(request.getSerializedSize());
                metrics.outboundBandwidth().mark(request.getSerializedSize());
                metrics.inboundGossip().update(request.getSerializedSize());
                metrics.gossipReply().update(response.getSerializedSize());
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }, system, services);
    }

    @Override
    public void update(Push request, StreamObserver<Null> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            s.update(request);
            if (metrics != null) {
                metrics.inboundUpdateRate().mark();
                metrics.inboundBandwidth().mark(request.getSerializedSize());
                metrics.inboundUpdate().update(request.getSerializedSize());
            }
        }, system, services);
    }

    private Service                system;
    private ClientIdentity         identity;
    private Map<HashKey, Service>  services = new ConcurrentHashMap<>();
    private final MessagingMetrics metrics;

    public MessagingServerCommunications(Service system, ClientIdentity identity, MessagingMetrics metrics) {
        this.metrics = metrics;
        this.system = system;
        this.identity = identity;
    }

    @Override
    public void register(HashKey id, Service service) {
        services.computeIfAbsent(id, m -> service);
    }

    @Override
    public ClientIdentity getClientIdentity() {
        return identity;
    }

}
