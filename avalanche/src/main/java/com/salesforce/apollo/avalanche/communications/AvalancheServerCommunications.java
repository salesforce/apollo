/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.AvalancheGrpc.AvalancheImplBase;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.Query;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.SuppliedDagNodes;
import com.salesforce.apollo.avalanche.Avalanche.Service;
import com.salesforce.apollo.avalanche.AvalancheMetrics;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheServerCommunications extends AvalancheImplBase {
    @SuppressWarnings("unused")
    private final ClientIdentity           identity;
    private final AvalancheMetrics         metrics;
    private final RoutableService<Service> router;

    public AvalancheServerCommunications(ClientIdentity identity, AvalancheMetrics metrics,
            RoutableService<Service> router) {
        this.identity = identity;
        this.metrics = metrics;
        this.router = router;
    }

    @Override
    public void query(Query request, StreamObserver<QueryResult> responseObserver) {
        router.evaluate(responseObserver, request.getContext(), s -> {
            QueryResult result = s.onQuery(request.getHashesList(), request.getTransactionsList(),
                                           request.getWantedList()
                                                  .stream()
                                                  .map(e -> new HashKey(e))
                                                  .collect(Collectors.toList()));
            responseObserver.onNext(result);
            responseObserver.onCompleted();
            if (metrics != null) {
                metrics.inboundBandwidth().mark(request.getSerializedSize());
                metrics.outboundBandwidth().mark(result.getSerializedSize());
                metrics.inboundQuery().update(request.getSerializedSize());
                metrics.queryReply().update(result.getSerializedSize());
            }
        });
    }

    @Override
    public void requestDag(DagNodes request, StreamObserver<SuppliedDagNodes> responseObserver) {
        router.evaluate(responseObserver, request.getContext(), s -> {
            List<ByteString> result = s.requestDAG(request.getEntriesList()
                                                          .stream()
                                                          .map(e -> new HashKey(e))
                                                          .collect(Collectors.toList()));
            SuppliedDagNodes dags = SuppliedDagNodes.newBuilder().addAllEntries(result).build();
            responseObserver.onNext(dags);
            responseObserver.onCompleted();
            if (metrics != null) {
                metrics.inboundBandwidth().mark(request.getSerializedSize());
                metrics.outboundBandwidth().mark(dags.getSerializedSize());
                metrics.inboundRequestDag().update(request.getSerializedSize());
                metrics.requestDagReply().update(dags.getSerializedSize());
            }
        });
    }
}
