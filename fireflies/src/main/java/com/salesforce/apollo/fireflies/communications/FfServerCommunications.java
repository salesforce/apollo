/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.proto.FirefliesGrpc.FirefliesImplBase;
import com.salesfoce.apollo.proto.Gossip;
import com.salesfoce.apollo.proto.Null;
import com.salesfoce.apollo.proto.SayWhat;
import com.salesfoce.apollo.proto.State;
import com.salesforce.apollo.comm.grpc.BaseServerCommunications;
import com.salesforce.apollo.fireflies.FireflyMetrics;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class FfServerCommunications extends FirefliesImplBase implements BaseServerCommunications<Service> {
    private Service               system;
    private ClientIdentity        identity;
    private Map<HashKey, Service> services = new ConcurrentHashMap<>();
    private final FireflyMetrics  metrics;

    public FfServerCommunications(Service system, ClientIdentity identity, FireflyMetrics metrics) {
        this.metrics = metrics;
        this.system = system;
        this.identity = identity;
    }

    @Override
    public void gossip(SayWhat request, StreamObserver<Gossip> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            Context timer = null;
            if (metrics != null) {
                timer = metrics.inboundGossipTimer().time();
            }
            try {
                Gossip gossip = s.rumors(request.getRing(), request.getGossip(), getFrom(), getCert(),
                                         request.getNote());
                responseObserver.onNext(gossip);
                responseObserver.onCompleted();
                if (metrics != null) {
                    metrics.inboundGossipRate().mark();
                    metrics.inboundBandwidth().inc(request.getSerializedSize());
                    metrics.outboundBandwidth().inc(gossip.getSerializedSize());
                    metrics.inboundGossip().update(request.getSerializedSize());
                    metrics.gossipReply().update(gossip.getSerializedSize());
                }
            } finally {
                if (timer != null) {
                    timer.stop();
                }
            }
        }, system, services);
    }

    @Override
    public void ping(Null request, StreamObserver<Null> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            responseObserver.onNext(Null.getDefaultInstance());
            responseObserver.onCompleted();
            if (metrics != null) {
                metrics.inboundPingRate().mark();
            }
        }, system, services);
    }

    @Override
    public void register(HashKey id, Service service) {
        services.computeIfAbsent(id, m -> service);
    }

    @Override
    public void update(State request, StreamObserver<Null> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            Context timer = null;
            if (metrics != null) {
                timer = metrics.inboundUpdateTimer().time();
            }
            try {
                s.update(request.getRing(), request.getUpdate(), getFrom());
                responseObserver.onNext(Null.getDefaultInstance());
                responseObserver.onCompleted();
                if (metrics != null) {
                    metrics.inboundBandwidth().inc(request.getSerializedSize());
                    metrics.outboundBandwidth().inc(Null.getDefaultInstance().getSerializedSize());
                    metrics.inboundUpdate().update(request.getSerializedSize());
                    metrics.inboundUpdateRate().mark();
                }
            } finally {
                if (timer != null) {
                    timer.stop();
                }
            }
        }, system, services);
    }

    @Override
    public ClientIdentity getClientIdentity() {
        return identity;
    }

}
