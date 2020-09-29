/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.AvalancheGrpc.AvalancheImplBase;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.DagNodes.Builder;
import com.salesfoce.apollo.proto.Query;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesforce.apollo.avalanche.Avalanche.Service;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheServerCommunications extends AvalancheImplBase {
    private final Service        avalanche;
    @SuppressWarnings("unused")
    private final ClientIdentity identity;

    public AvalancheServerCommunications(Service avalanche, ClientIdentity identity) {
        this.avalanche = avalanche;
        this.identity = identity;
    }

    @Override
    public void query(Query request, StreamObserver<QueryResult> responseObserver) {
        QueryResult result = avalanche.onQuery(request.getTransactionsList()
                                                      .stream()
                                                      .map(e -> ByteBuffer.wrap(e.toByteArray()))
                                                      .collect(Collectors.toList()),
                                               request.getWantedList()
                                                      .stream()
                                                      .map(e -> new HashKey(e))
                                                      .collect(Collectors.toList()));
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void requestDag(DagNodes request, StreamObserver<DagNodes> responseObserver) {
        List<ByteBuffer> result = avalanche.requestDAG(request.getEntriesList()
                                                              .stream()
                                                              .map(e -> new HashKey(e))
                                                              .collect(Collectors.toList()));
        Builder builder = DagNodes.newBuilder();
        result.forEach(e -> builder.addEntries(ByteString.copyFrom(e)));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

}
