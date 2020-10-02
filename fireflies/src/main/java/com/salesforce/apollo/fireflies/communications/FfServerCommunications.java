/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.salesfoce.apollo.proto.FirefliesGrpc.FirefliesImplBase;
import com.salesfoce.apollo.proto.Gossip;
import com.salesfoce.apollo.proto.Null;
import com.salesfoce.apollo.proto.SayWhat;
import com.salesfoce.apollo.proto.State;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.membership.Member;
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

    public FfServerCommunications(Service system, ClientIdentity identity) {
        this.system = system;
        this.identity = identity;
    }

    @Override
    public void gossip(SayWhat request, StreamObserver<Gossip> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            Gossip gossip = s.rumors(request.getRing(), request.getGossip(), getFrom(), getCert(), request.getNote());
            responseObserver.onNext(gossip);
            responseObserver.onCompleted();
        }, system, services);
    }

    @Override
    public void ping(Null request, StreamObserver<Null> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            responseObserver.onNext(Null.getDefaultInstance());
            responseObserver.onCompleted();
        }, system, services);
    }

    @Override
    public void register(Member member, Service service) {
        services.computeIfAbsent(member.getId(), m -> service);
    }

    @Override
    public void update(State request, StreamObserver<Null> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            s.update(request.getRing(), request.getUpdate(), getFrom());
            responseObserver.onNext(Null.getDefaultInstance());
            responseObserver.onCompleted();
        }, system, services);
    }

    @Override
    public ClientIdentity getClientIdentity() {
        return identity;
    }

}
