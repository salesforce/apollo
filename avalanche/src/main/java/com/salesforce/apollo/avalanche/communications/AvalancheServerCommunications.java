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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.AvalancheGrpc.AvalancheImplBase;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.DagNodes.Builder;
import com.salesfoce.apollo.proto.Query;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesforce.apollo.avalanche.Avalanche.Service;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheServerCommunications extends AvalancheImplBase {
    @SuppressWarnings("unused")
    private final ClientIdentity        identity;
    private final Map<HashKey, Service> services = new ConcurrentHashMap<>();
    private final Service               system;

    public AvalancheServerCommunications(Service avalanche, ClientIdentity identity) {
        this.system = avalanche;
        this.identity = identity;
    }

    @Override
    public void query(Query request, StreamObserver<QueryResult> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            QueryResult result = s.onQuery(request.getTransactionsList()
                                                  .stream()
                                                  .map(e -> ByteBuffer.wrap(e.toByteArray()))
                                                  .collect(Collectors.toList()),
                                           request.getWantedList()
                                                  .stream()
                                                  .map(e -> new HashKey(e))
                                                  .collect(Collectors.toList()));
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        });
    }

    public void register(Member member, Service service) {
        services.computeIfAbsent(member.getId(), m -> service);
    }

    @Override
    public void requestDag(DagNodes request, StreamObserver<DagNodes> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            List<ByteBuffer> result = s.requestDAG(request.getEntriesList()
                                                          .stream()
                                                          .map(e -> new HashKey(e))
                                                          .collect(Collectors.toList()));
            Builder builder = DagNodes.newBuilder();
            result.forEach(e -> builder.addEntries(ByteString.copyFrom(e)));
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        });
    }

    private void evaluate(StreamObserver<?> responseObserver, ByteString context, Consumer<Service> c) {
        Service service = getService(context);
        if (service == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN));
        } else {
            c.accept(service);
        }
    }

    private Service getService(ByteString context) {
        return (context.isEmpty() && system != null) ? system : services.get(new HashKey(context));
    }
}
