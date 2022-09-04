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
import com.salesfoce.apollo.gorgoneion.proto.Gossip;
import com.salesfoce.apollo.gorgoneion.proto.ReplicationGrpc;
import com.salesfoce.apollo.gorgoneion.proto.ReplicationGrpc.ReplicationFutureStub;
import com.salesfoce.apollo.gorgoneion.proto.Update;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.thoth.metrics.GorgoneionMetrics;

/**
 * @author hal.hildebrand
 *
 */
public class AdmissionsReplicationClient implements AdmissionReplicationService {
    public static CreateClientCommunications<AdmissionReplicationService> getCreate(Digest context,
                                                                                    GorgoneionMetrics metrics) {
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
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<Update> gossip(Gossip gossip) {
                return null;
            }

            @Override
            public ListenableFuture<Empty> update(Update update) {
                return null;
            }
        };
    }

    private final ManagedServerConnection channel;
    private final ReplicationFutureStub   client;
    @SuppressWarnings("unused")
    private final Digeste                 context;
    private final Member                  member;
    @SuppressWarnings("unused")
    private final GorgoneionMetrics       metrics;

    public AdmissionsReplicationClient(Digest context, ManagedServerConnection channel, Member member,
                                       GorgoneionMetrics metrics) {
        this.context = context.toDigeste();
        this.member = member;
        this.channel = channel;
        this.client = ReplicationGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public void close() throws IOException {
        channel.release();
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<Update> gossip(Gossip gossip) {
        return client.gossip(gossip);
    }

    @Override
    public ListenableFuture<Empty> update(Update update) {
        return client.update(update);
    }
}
