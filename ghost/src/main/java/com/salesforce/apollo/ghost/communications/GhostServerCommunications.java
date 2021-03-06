/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.ghost.proto.Bind;
import com.salesfoce.apollo.ghost.proto.Binding;
import com.salesfoce.apollo.ghost.proto.ClockMongering;
import com.salesfoce.apollo.ghost.proto.Content;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Entry;
import com.salesfoce.apollo.ghost.proto.Get;
import com.salesfoce.apollo.ghost.proto.GhostChat;
import com.salesfoce.apollo.ghost.proto.GhostGrpc.GhostImplBase;
import com.salesfoce.apollo.ghost.proto.Intervals;
import com.salesfoce.apollo.ghost.proto.Lookup;
import com.salesfoce.apollo.utils.proto.Sig;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostServerCommunications extends GhostImplBase {
    private final ClientIdentity                identity;
    private final RoutableService<GhostService> router;

    public GhostServerCommunications(ClientIdentity identity, RoutableService<GhostService> router) {
        this.identity = identity;
        this.router = router;
    }

    @Override
    public void ghosting(GhostChat request, StreamObserver<ClockMongering> responseObserver) {
        router.evaluate(responseObserver, digest(request.getContext()), s -> {
            responseObserver.onNext(s.ghosting(request, identity.getFrom()));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void get(Get request, StreamObserver<Content> responseObserver) {
        router.evaluate(responseObserver, digest(request.getContext()), s -> {
            responseObserver.onNext(s.get(request, identity.getFrom()));
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
    public void put(Entry request, StreamObserver<Sig> responseObserver) {
        router.evaluate(responseObserver, digest(request.getContext()), s -> {
            var sig = s.put(request, identity.getFrom());
            responseObserver.onNext(sig);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void purge(Get request, StreamObserver<Empty> responseObserver) {
        router.evaluate(responseObserver, digest(request.getContext()), s -> {
            s.purge(request, identity.getFrom());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void lookup(Lookup request, StreamObserver<Binding> responseObserver) {
        router.evaluate(responseObserver, digest(request.getContext()), s -> {
            responseObserver.onNext(s.lookup(request, identity.getFrom()));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void bind(Bind request, StreamObserver<Sig> responseObserver) {
        router.evaluate(responseObserver, digest(request.getContext()), s -> {
            var sig = s.bind(request, identity.getFrom());
            responseObserver.onNext(sig);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void remove(Lookup request, StreamObserver<Empty> responseObserver) {
        router.evaluate(responseObserver, digest(request.getContext()), s -> {
            s.remove(request, identity.getFrom());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

}
