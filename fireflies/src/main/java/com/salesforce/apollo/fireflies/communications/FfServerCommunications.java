/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.FirefliesGrpc.FirefliesImplBase;
import com.salesfoce.apollo.proto.Gossip;
import com.salesfoce.apollo.proto.Null;
import com.salesfoce.apollo.proto.SayWhat;
import com.salesfoce.apollo.proto.State;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class FfServerCommunications extends FirefliesImplBase {

    private final ClientIdentity        identity;
    private final Map<HashKey, Service> services = new ConcurrentHashMap<>();
    private final Service               system;

    public FfServerCommunications(Service system, ClientIdentity identity) {
        this.identity = identity;
        this.system = system;
    }

    @Override
    public void gossip(SayWhat request, StreamObserver<Gossip> responseObserver) {
        ByteString context = request.getContext();
        Service service = getService(context);
        if (service == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN));
        } else {
            Gossip gossip = service.rumors(request.getRing(), request.getGossip(), getFrom(), getCert(),
                                           request.getNote());
            responseObserver.onNext(gossip);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void ping(Null request, StreamObserver<Null> responseObserver) {
        ByteString context = request.getContext();
        Service service = getService(context);
        if (service == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN));
        } else {
            responseObserver.onNext(Null.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    public void register(Member member, Service service) {
        services.computeIfAbsent(member.getId(), m -> service);
    }

    @Override
    public void update(State request, StreamObserver<Null> responseObserver) {
        ByteString context = request.getContext();
        Service service = getService(context);
        if (service == null) {
            responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN));
        } else {
            service.update(request.getRing(), request.getUpdate(), getFrom());
            responseObserver.onNext(Null.newBuilder().build());
            responseObserver.onCompleted();
        }
    }

    private X509Certificate getCert() {
        return (X509Certificate) identity.getCert();
    }

    private HashKey getFrom() {
        return identity.getFrom();
    }

    private Service getService(ByteString context) {
        return (context.isEmpty() && system != null) ? system : services.get(new HashKey(context));
    }

}
