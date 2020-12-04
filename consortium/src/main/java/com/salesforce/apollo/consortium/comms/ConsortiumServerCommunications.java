/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import org.slf4j.LoggerFactory;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.OrderingServiceGrpc.OrderingServiceImplBase;
import com.salesfoce.apollo.consortium.proto.ReplicateTransactions;
import com.salesfoce.apollo.consortium.proto.Stop;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.Sync;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.ConsortiumMetrics;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class ConsortiumServerCommunications extends OrderingServiceImplBase {
    private ClientIdentity                 identity;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics        metrics;
    private final RoutableService<Service> router;

    public ConsortiumServerCommunications(ClientIdentity identity, ConsortiumMetrics metrics,
            RoutableService<Service> router) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = router;
    }

    @Override
    public void join(Join request, StreamObserver<JoinResult> responseObserver) {
        try {
            router.evaluate(responseObserver, request.getContext().isEmpty() ? null : new HashKey(request.getContext()),
                            s -> {
                                responseObserver.onNext(s.join(request, identity.getFrom()));
                                responseObserver.onCompleted();
                            });
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void stopData(StopData request, StreamObserver<Empty> responseObserver) {
        router.evaluate(responseObserver, request.getContext().isEmpty() ? null : new HashKey(request.getContext()),
                        s -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                            s.stopData(request, identity.getFrom());
                        });
    }

    @Override
    public void submit(SubmitTransaction request, StreamObserver<TransactionResult> responseObserver) {
        router.evaluate(responseObserver, request.getContext().isEmpty() ? null : new HashKey(request.getContext()),
                        s -> {
                            responseObserver.onNext(s.clientSubmit(request, identity.getFrom()));
                            responseObserver.onCompleted();
                        });
    }

    @Override
    public void replicate(ReplicateTransactions request, StreamObserver<Empty> responseObserver) {
        router.evaluate(responseObserver, request.getContext().isEmpty() ? null : new HashKey(request.getContext()),
                        s -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                            HashKey from = identity.getFrom();
                            try {
                                s.replicate(request, from);
                            } catch (Throwable t) {
                                LoggerFactory.getLogger(ConsortiumServerCommunications.class)
                                             .error("error processing replicating", t);
                            }
                        });
    }

    @Override
    public void stop(Stop request, StreamObserver<Empty> responseObserver) {
        router.evaluate(responseObserver, request.getContext().isEmpty() ? null : new HashKey(request.getContext()),
                        s -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                            HashKey from = identity.getFrom();
                            try {
                                s.stop(request, from);
                            } catch (Throwable t) {
                                LoggerFactory.getLogger(ConsortiumServerCommunications.class)
                                             .error("error processing stop", t);
                            }
                        });
    }

    @Override
    public void sync(Sync request, StreamObserver<Empty> responseObserver) {
        router.evaluate(responseObserver, request.getContext().isEmpty() ? null : new HashKey(request.getContext()),
                        s -> {
                            responseObserver.onNext(Empty.getDefaultInstance());
                            responseObserver.onCompleted();
                            HashKey from = identity.getFrom();
                            try {
                                s.sync(request, from);
                            } catch (Throwable t) {
                                LoggerFactory.getLogger(ConsortiumServerCommunications.class)
                                             .error("error processing sync", t);
                            }
                        });
    }
}
