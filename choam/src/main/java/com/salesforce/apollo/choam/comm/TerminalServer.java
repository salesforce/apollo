/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import com.google.protobuf.Empty;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.choam.proto.*;
import com.salesforce.apollo.choam.proto.TerminalGrpc.TerminalImplBase;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public class TerminalServer extends TerminalImplBase {
    @SuppressWarnings("unused")
    private final ChoamMetrics               metrics;
    private final RoutableService<Concierge> router;
    private final ClientIdentity             identity;

    public TerminalServer(ClientIdentity identity, ChoamMetrics metrics, RoutableService<Concierge> router) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = router;
    }

    @Override
    public void fetch(CheckpointReplication request, StreamObserver<CheckpointSegments> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            responseObserver.onNext(s.fetch(request, from));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void fetchBlocks(BlockReplication request, StreamObserver<Blocks> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            responseObserver.onNext(s.fetchBlocks(request, from));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void fetchViewChain(BlockReplication request, StreamObserver<Blocks> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            responseObserver.onNext(s.fetchViewChain(request, from));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void join(SignedViewMember request, StreamObserver<Empty> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            responseObserver.onNext(s.join(request, from));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void sync(Synchronize request, StreamObserver<Initial> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            responseObserver.onNext(s.sync(request, from));
            responseObserver.onCompleted();
        });
    }
}
