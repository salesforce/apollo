/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;

import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Entry;
import com.salesfoce.apollo.ghost.proto.Get;
import com.salesfoce.apollo.ghost.proto.GhostGrpc.GhostImplBase;
import com.salesfoce.apollo.ghost.proto.Intervals;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.ghost.Ghost.Service;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostServerCommunications extends GhostImplBase {
    private final ClientIdentity           identity;
    private final RoutableService<Service> router;

    public GhostServerCommunications(ClientIdentity identity, RoutableService<Service> router) {
        this.identity = identity;
        this.router = router;
    }

    @Override
    public void get(Get request, StreamObserver<Any> responseObserver) {
        router.evaluate(responseObserver, digest(request.getContext()), s -> {
            responseObserver.onNext(s.get(request));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void intervals(Intervals request, StreamObserver<Entries> responseObserver) {
        router.evaluate(responseObserver, digest(request.getContext()), s -> {
            responseObserver.onNext(s.intervals(request, identity.getFrom()));
            responseObserver.onCompleted();
        });

    }

    @Override
    public void put(Entry request, StreamObserver<Empty> responseObserver) {
        router.evaluate(responseObserver, digest(request.getContext()), s -> {
            s.put(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

}
