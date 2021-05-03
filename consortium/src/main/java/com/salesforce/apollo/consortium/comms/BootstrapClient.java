/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.BootstrapServiceGrpc;
import com.salesfoce.apollo.consortium.proto.BootstrapServiceGrpc.BootstrapServiceFutureStub;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class BootstrapClient implements BootstrapService {

    public static CreateClientCommunications<ConsortiumClient> getCreate(ConsortiumMetrics metrics) {
        return (t, f, c) -> new ConsortiumClient(c, t, metrics);

    }

    private final ManagedServerConnection    channel;
    private final BootstrapServiceFutureStub client;
    private final Member                     member;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics          metrics;

    public BootstrapClient(ManagedServerConnection channel, Member member, ConsortiumMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = BootstrapServiceGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public ListenableFuture<CheckpointSegments> fetch(CheckpointReplication request) {
        return client.fetch(request);
    }

    public Member getMember() {
        return member;
    }

    public void release() {
        channel.release();
    }
}
