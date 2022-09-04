/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.admission.gossip;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.gorgoneion.proto.Gossip;
import com.salesfoce.apollo.gorgoneion.proto.ReplicationGrpc.ReplicationImplBase;
import com.salesfoce.apollo.gorgoneion.proto.Update;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.thoth.metrics.GorgoneionMetrics;
import com.salesforce.apollo.utils.Utils;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class ReplicationServer extends ReplicationImplBase {
    private final static Logger log = LoggerFactory.getLogger(ReplicationServer.class);

    private final Executor                               exec;
    private final ClientIdentity                         identity;
    @SuppressWarnings("unused")
    private final GorgoneionMetrics                      metrics;
    private final RoutableService<Replication> router;

    public ReplicationServer(RoutableService<Replication> router, ClientIdentity identity,
                                       Executor exec, GorgoneionMetrics metrics) {
        this.metrics = metrics;
        this.router = router;
        this.exec = exec;
        this.identity = identity;
    }

    @Override
    public void gossip(Gossip request, StreamObserver<Update> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var update = s.gossip(request, from);
            responseObserver.onNext(update);
            responseObserver.onCompleted();
        }), log));
    }

    @Override
    public void update(Update request, StreamObserver<Empty> responseObserver) {
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            s.update(request, from);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        }), log));
    }
}
