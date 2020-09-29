/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.security.cert.X509Certificate;

import com.salesfoce.apollo.proto.FirefliesGrpc.FirefliesImplBase;
import com.salesfoce.apollo.proto.Gossip;
import com.salesfoce.apollo.proto.Null;
import com.salesfoce.apollo.proto.SayWhat;
import com.salesfoce.apollo.proto.State;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class FfServerCommunications extends FirefliesImplBase {

    private final ClientIdentity identity;
    private final Service        service;

    public FfServerCommunications(Service service, ClientIdentity identity) {
        this.service = service;
        this.identity = identity;
    }

    @Override
    public void gossip(SayWhat request, StreamObserver<Gossip> responseObserver) {
        Gossip gossip = service.rumors(request.getRing(), request.getGossip(), getFrom(), getCert(), request.getNote());
        responseObserver.onNext(gossip);
        responseObserver.onCompleted();
    }

    @Override
    public void ping(Null request, StreamObserver<Null> responseObserver) {
        responseObserver.onNext(Null.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void update(State request, StreamObserver<Null> responseObserver) {
        service.update(request.getRing(), request.getUpdate(), getFrom());
        responseObserver.onNext(Null.newBuilder().build());
        responseObserver.onCompleted();
    }

    private X509Certificate getCert() {
        return (X509Certificate) identity.getCert();
    }

    private HashKey getFrom() {
        return identity.getFrom();
    }

}
