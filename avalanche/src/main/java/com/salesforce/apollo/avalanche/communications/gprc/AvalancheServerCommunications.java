/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications.gprc;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.AvalancheGrpc.AvalancheImplBase;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.Query;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.QueryResult.Builder;
import com.salesfoce.apollo.proto.QueryResult.Vote;
import com.salesforce.apollo.avalanche.Avalanche.Service;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheServerCommunications extends AvalancheImplBase {
    private final Service avalanche;

    public AvalancheServerCommunications(Service avalanche) {
        this.avalanche = avalanche;
    }

    @Override
    public void query(Query request, StreamObserver<com.salesfoce.apollo.proto.QueryResult> responseObserver) {
        com.salesforce.apollo.avalanche.Avalanche.Query result = avalanche.onQuery(request.getTransactionsList()
                                                                                          .stream()
                                                                                          .map(e -> e.asReadOnlyByteBuffer())
                                                                                          .collect(Collectors.toList()),
                                                                                   request.getWantedList()
                                                                                          .stream()
                                                                                          .map(e -> new HashKey(e))
                                                                                          .collect(Collectors.toList()));
        Builder builder = QueryResult.newBuilder();
        builder.addAllResult(result.stronglyPreferred.stream().map(e -> {
            if (e == null) {
                return Vote.UNKNOWN;
            }
            return e ? Vote.TRUE : Vote.FALSE;
        }).collect(Collectors.toList()));
        builder.addAllWanted(result.wanted.stream().map(e -> ByteString.copyFrom(e)).collect(Collectors.toList()));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void requestDag(DagNodes request, StreamObserver<DagNodes> responseObserver) {
        List<ByteBuffer> result = avalanche.requestDAG(request.getEntriesList()
                                                              .stream()
                                                              .map(e -> new HashKey(e))
                                                              .collect(Collectors.toList()));
        com.salesfoce.apollo.proto.DagNodes.Builder builder = DagNodes.newBuilder();
        builder.addAllEntries(result.stream().map(e -> ByteString.copyFrom(e)).collect(Collectors.toList()));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

}
