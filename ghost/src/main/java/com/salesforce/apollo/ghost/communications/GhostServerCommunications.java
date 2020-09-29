/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import java.util.stream.Collectors;

import com.salesfoce.apollo.proto.Bytes;
import com.salesfoce.apollo.proto.DagEntries;
import com.salesfoce.apollo.proto.DagEntries.Builder;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.GhostGrpc.GhostImplBase;
import com.salesfoce.apollo.proto.Intervals;
import com.salesfoce.apollo.proto.Null;
import com.salesforce.apollo.ghost.Ghost.Service;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostServerCommunications extends GhostImplBase {
    @Override
    public void get(Bytes request, StreamObserver<DagEntry> responseObserver) {
        responseObserver.onNext(ghost.get(new HashKey(request.getBites())));
        responseObserver.onCompleted();
    }

    @Override
    public void put(DagEntry request, StreamObserver<Null> responseObserver) {
        ghost.put(request);
        responseObserver.onNext(Null.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void intervals(Intervals request, StreamObserver<DagEntries> responseObserver) {
        Builder builder = DagEntries.newBuilder();
        ghost.intervals(request.getIntervalsList(),
                        request.getHaveList().stream().map(e -> new HashKey(e)).collect(Collectors.toList()))
             .forEach(e -> builder.addEntries(e));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private final Service ghost;

    public GhostServerCommunications(Service ghost, ClientIdentity identity) {
        this.ghost = ghost;
    }

}
