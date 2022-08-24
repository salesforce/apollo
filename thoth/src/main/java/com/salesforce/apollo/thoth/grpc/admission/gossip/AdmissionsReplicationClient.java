/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.admission.gossip;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.thoth.proto.AdmissionsGossip;
import com.salesfoce.apollo.thoth.proto.AdmissionsReplicationGrpc;
import com.salesfoce.apollo.thoth.proto.AdmissionsReplicationGrpc.AdmissionsReplicationFutureStub;
import com.salesfoce.apollo.thoth.proto.AdmissionsUpdate;
import com.salesfoce.apollo.thoth.proto.Expunge;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;

/**
 * @author hal.hildebrand
 *
 */
public class AdmissionsReplicationClient implements AdmissionReplicationService {
    public static CreateClientCommunications<AdmissionReplicationService> getCreate(Digest context,
                                                                                    StereotomyMetrics metrics) {
        return (t, f, c) -> {
            return new AdmissionsReplicationClient(context, c, t, metrics);
        };
    }

    public static AdmissionReplicationService getLocalLoopback(AdmissionsReplication service, SigningMember member) {
        return new AdmissionReplicationService() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public ListenableFuture<Empty> expunge(Expunge expunge) {
                return null;
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<AdmissionsUpdate> gossip(AdmissionsGossip gossip) {
                return null;
            }

            @Override
            public ListenableFuture<Empty> register(AdmissionsUpdate update) {
                return null;
            }
        };
    }

    private final ManagedServerConnection         channel;
    private final AdmissionsReplicationFutureStub client;
    @SuppressWarnings("unused")
    private final Digeste                         context;
    private final Member                          member;
    @SuppressWarnings("unused")
    private final StereotomyMetrics               metrics;

    public AdmissionsReplicationClient(Digest context, ManagedServerConnection channel, Member member,
                                       StereotomyMetrics metrics) {
        this.context = context.toDigeste();
        this.member = member;
        this.channel = channel;
        this.client = AdmissionsReplicationGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public void close() throws IOException {
        channel.release();
    }

    @Override
    public ListenableFuture<Empty> expunge(Expunge expunge) {
        return client.expunge(expunge);
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<AdmissionsUpdate> gossip(AdmissionsGossip gossip) {
        return client.gossip(gossip);
    }

    @Override
    public ListenableFuture<Empty> register(AdmissionsUpdate update) {
        return client.update(update);
    }
}
