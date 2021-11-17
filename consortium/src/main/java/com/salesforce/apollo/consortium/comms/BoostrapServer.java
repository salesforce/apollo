/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import com.salesfoce.apollo.consortium.proto.BlockReplication;
import com.salesfoce.apollo.consortium.proto.Blocks;
import com.salesfoce.apollo.consortium.proto.BoostrapGrpc.BoostrapImplBase;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesfoce.apollo.consortium.proto.Initial;
import com.salesfoce.apollo.consortium.proto.Synchronize;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.consortium.Consortium.Bootstrapping;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class BoostrapServer extends BoostrapImplBase {
    private ClientIdentity                       identity;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics              metrics;
    private final RoutableService<Bootstrapping> router;

    public BoostrapServer(ClientIdentity identity, ConsortiumMetrics metrics, RoutableService<Bootstrapping> router) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = router;
    }

    @Override
    public void fetch(CheckpointReplication request, StreamObserver<CheckpointSegments> responseObserver) {
        router.evaluate(responseObserver, request.hasContext() ? new Digest(request.getContext()) : null, s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            responseObserver.onNext(s.fetch(request, from));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void fetchBlocks(BlockReplication request, StreamObserver<Blocks> responseObserver) {
        router.evaluate(responseObserver, request.hasContext() ? new Digest(request.getContext()) : null, s -> {
            responseObserver.onNext(s.fetchBlocks(request, identity.getFrom()));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void sync(Synchronize request, StreamObserver<Initial> responseObserver) {
        router.evaluate(responseObserver, request.hasContext() ? new Digest(request.getContext()) : null, s -> {
            responseObserver.onNext(s.sync(request, identity.getFrom()));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void fetchViewChain(BlockReplication request, StreamObserver<Blocks> responseObserver) {
        router.evaluate(responseObserver, request.hasContext() ? new Digest(request.getContext()) : null, s -> {
            responseObserver.onNext(s.fetchViewChain(request, identity.getFrom()));
            responseObserver.onCompleted();
        });
    }
}
