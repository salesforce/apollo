/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.salesfoce.apollo.consortium.proto.OrderingServiceGrpc.OrderingServiceImplBase;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesforce.apollo.comm.grpc.BaseServerCommunications;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.ConsortiumMetrics;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class ConsortiumServerCommunications extends OrderingServiceImplBase
        implements BaseServerCommunications<Service> {
    private Service                 system;
    private ClientIdentity          identity;
    private Map<HashKey, Service>   services = new ConcurrentHashMap<>();
    @SuppressWarnings("unused")
    private final ConsortiumMetrics metrics;

    public ConsortiumServerCommunications(Service system, ClientIdentity identity, ConsortiumMetrics metrics) {
        this.metrics = metrics;
        this.system = system;
        this.identity = identity;
    }

    @Override
    public ClientIdentity getClientIdentity() {
        return identity;
    }

    @Override
    public void register(HashKey id, Service service) {
        services.computeIfAbsent(id, m -> service);
    }

    @Override
    public void submit(SubmitTransaction request, StreamObserver<TransactionResult> responseObserver) {
        evaluate(responseObserver, request.getContext().isEmpty() ? null : new HashKey(request.getContext()), s -> {
            responseObserver.onNext(s.clientSubmit(request));
        }, system, services);
    }

}
