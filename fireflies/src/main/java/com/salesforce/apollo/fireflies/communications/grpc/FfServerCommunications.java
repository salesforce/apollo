/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications.grpc;

import com.salesfoce.apollo.proto.FirefliesGrpc.FirefliesImplBase;
import com.salesfoce.apollo.proto.Gossip;
import com.salesfoce.apollo.proto.Null;
import com.salesfoce.apollo.proto.SayWhat;
import com.salesfoce.apollo.proto.State;
import com.salesforce.apollo.fireflies.View.Service;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class FfServerCommunications extends FirefliesImplBase {
    
    public final Service service;
     
    public FfServerCommunications(Service service) { 
        this.service = service;
    }

    @Override
    public void gossip(SayWhat request, StreamObserver<Gossip> responseObserver) {
        service.rumors(request.getRing(), request.getGossip(), request.get, request.get, note);
        // TODO Auto-generated method stub
        super.gossip(request, responseObserver);
    }

    @Override
    public void update(State request, StreamObserver<Null> responseObserver) {
        // TODO Auto-generated method stub
        super.update(request, responseObserver);
    }

    @Override
    public void ping(Null request, StreamObserver<Null> responseObserver) {
        // TODO Auto-generated method stub
        super.ping(request, responseObserver);
    }

}
