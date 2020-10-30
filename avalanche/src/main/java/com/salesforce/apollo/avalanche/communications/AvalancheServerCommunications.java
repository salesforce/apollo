/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.AvalancheGrpc.AvalancheImplBase;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.Query;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.SuppliedDagNodes;
import com.salesforce.apollo.avalanche.Avalanche.Service;
import com.salesforce.apollo.avalanche.AvalancheMetrics;
import com.salesforce.apollo.comm.grpc.BaseServerCommunications;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheServerCommunications extends AvalancheImplBase implements BaseServerCommunications<Service> {
    private final ClientIdentity        identity;
    private final Map<HashKey, Service> services = new ConcurrentHashMap<>();
    private final Service               system;
    private final AvalancheMetrics      metrics;

    public AvalancheServerCommunications(Service avalanche, ClientIdentity identity, AvalancheMetrics metrics) {
        this.system = avalanche;
        this.identity = identity;
        this.metrics = metrics;
    }

    @Override
    public ClientIdentity getClientIdentity() {
        return identity;
    }

    @Override
    public void query(Query request, StreamObserver<QueryResult> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            QueryResult result = s.onQuery(request.getTransactionsList()
                                                  .stream()
                                                  .map(e -> e.toByteArray())
                                                  .collect(Collectors.toList()),
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
        }, system, services);
    }

    @Override
    public void register(HashKey id, Service service) {
        services.computeIfAbsent(id, m -> service);
    }

    @Override
    public void requestDag(DagNodes request, StreamObserver<SuppliedDagNodes> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            List<ByteBuffer> result = s.requestDAG(request.getEntriesList()
                                                          .stream()
                                                          .map(e -> new HashKey(e))
                                                          .collect(Collectors.toList()));
            com.salesfoce.apollo.proto.SuppliedDagNodes.Builder builder = SuppliedDagNodes.newBuilder();
            result.forEach(e -> builder.addEntries(ByteString.copyFrom(e)));
            SuppliedDagNodes dags = builder.build();
            responseObserver.onNext(dags);
            responseObserver.onCompleted();
            if (metrics != null) {
                metrics.inboundBandwidth().mark(request.getSerializedSize());
                metrics.outboundBandwidth().mark(dags.getSerializedSize());
                metrics.inboundRequestDag().update(request.getSerializedSize());
                metrics.requestDagReply().update(dags.getSerializedSize());
            }
        }, system, services);
    }
}
