/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.salesfoce.apollo.proto.ADagEntry;
import com.salesfoce.apollo.proto.Bytes;
import com.salesfoce.apollo.proto.DagEntries;
import com.salesfoce.apollo.proto.DagEntries.Builder;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.GhostGrpc.GhostImplBase;
import com.salesfoce.apollo.proto.Intervals;
import com.salesfoce.apollo.proto.Null;
import com.salesforce.apollo.fireflies.communications.BaseServerCommunications;
import com.salesforce.apollo.ghost.Ghost.Service;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostServerCommunications extends GhostImplBase implements BaseServerCommunications<Service> {
    private final ClientIdentity  identity;
    private Map<HashKey, Service> services = new ConcurrentHashMap<>();
    private final Service         system;

    public GhostServerCommunications(Service system, ClientIdentity identity) {
        this.system = system;
        this.identity = identity;
    }

    @Override
    public void get(Bytes request, StreamObserver<DagEntry> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            responseObserver.onNext(s.get(new HashKey(request.getBites())));
            responseObserver.onCompleted();
        }, system, services);
    }

    @Override
    public ClientIdentity getClientIdentity() {
        return identity;
    }

    @Override
    public void intervals(Intervals request, StreamObserver<DagEntries> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            Builder builder = DagEntries.newBuilder();
            s.intervals(request.getIntervalsList(),
                        request.getHaveList().stream().map(e -> new HashKey(e)).collect(Collectors.toList()))
             .forEach(e -> builder.addEntries(e));
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }, system, services);

    }

    @Override
    public void put(ADagEntry request, StreamObserver<Null> responseObserver) {
        evaluate(responseObserver, request.getContext(), s -> {
            s.put(request.getEntry());
            responseObserver.onNext(Null.getDefaultInstance());
            responseObserver.onCompleted();
        }, system, services);
    }

    @Override
    public void register(Member member, Service service) {
        services.computeIfAbsent(member.getId(), id -> service);
    }

}
