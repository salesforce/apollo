/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesfoce.apollo.choam.proto.Initial;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.Synchronize;
import com.salesfoce.apollo.choam.proto.TerminalGrpc.TerminalImplBase;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.choam.CHOAM.Concierge;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class TerminalServer extends TerminalImplBase {
    private ClientIdentity                   identity;
    @SuppressWarnings("unused")
    private final ChoamMetrics               metrics;
    private final RoutableService<Concierge> router;

    public TerminalServer(ClientIdentity identity, ChoamMetrics metrics, RoutableService<Concierge> router) {
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
    public void fetchViewChain(BlockReplication request, StreamObserver<Blocks> responseObserver) {
        router.evaluate(responseObserver, request.hasContext() ? new Digest(request.getContext()) : null, s -> {
            responseObserver.onNext(s.fetchViewChain(request, identity.getFrom()));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void join(JoinRequest request, StreamObserver<ViewMember> responseObserver) {
        router.evaluate(responseObserver, request.hasContext() ? new Digest(request.getContext()) : null, s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            responseObserver.onNext(s.join(request, from));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void submit(SubmitTransaction request, StreamObserver<SubmitResult> responseObserver) {
        router.evaluate(responseObserver, request.hasContext() ? new Digest(request.getContext()) : null, s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            responseObserver.onNext(s.submit(request, from));
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
}
