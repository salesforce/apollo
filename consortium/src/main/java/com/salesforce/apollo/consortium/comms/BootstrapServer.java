/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import com.salesfoce.apollo.consortium.proto.BlockReplication;
import com.salesfoce.apollo.consortium.proto.Blocks;
import com.salesfoce.apollo.consortium.proto.BootstrapServiceGrpc.BootstrapServiceImplBase;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class BootstrapServer extends BootstrapServiceImplBase {
    private ClientIdentity                 identity;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics        metrics;
    private final RoutableService<Service> router;

    public BootstrapServer(ClientIdentity identity, ConsortiumMetrics metrics, RoutableService<Service> router) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = router;
    }

    @Override
    public void fetch(CheckpointReplication request, StreamObserver<CheckpointSegments> responseObserver) {
        router.evaluate(responseObserver, request.getContext().isEmpty() ? null : new HashKey(request.getContext()),
                        s -> {
                            HashKey from = identity.getFrom();
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
        router.evaluate(responseObserver, request.getContext().isEmpty() ? null : new HashKey(request.getContext()),
                        s -> {
                            responseObserver.onNext(s.fetchBlocks(request, identity.getFrom()));
                            responseObserver.onCompleted();
                        });
    }
}
