/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.comms;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.demesne.proto.DelegationGrpc;
import com.salesfoce.apollo.demesne.proto.DelegationGrpc.DelegationFutureStub;
import com.salesfoce.apollo.demesne.proto.DelegationUpdate;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class DelegationClient implements Delegation {
    private final ManagedServerChannel channel;
    private final DelegationFutureStub client;
    private final OuterServerMetrics   metrics;

    public DelegationClient(ManagedServerChannel channel, OuterServerMetrics metrics) {
        this.metrics = metrics;
        client = DelegationGrpc.newFutureStub(channel).withCompression("gzip");
        this.channel = channel;
    }

    @Override
    public void close() throws IOException {
        channel.release();
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    @Override
    public ListenableFuture<DelegationUpdate> gossip(Biff identifiers) {
        Context timer = metrics != null ? metrics.gossip().time() : null;
        if (metrics != null) {
            final var serializedSize = identifiers.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGossip().mark(serializedSize);
        }
        ListenableFuture<DelegationUpdate> update = null;
        try {
            update = client.gossip(identifiers);
            return update;
        } finally {
            if (timer != null) {
                var u = update;
                update.addListener(() -> {
                    timer.stop();
                    DelegationUpdate up;
                    try {
                        up = u.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (ExecutionException e) {
                        LoggerFactory.getLogger(getClass()).warn("Error retrieving update", e);
                        return;
                    }
                    final var serializedSize = up.getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.outboundUpdate().mark(serializedSize);
                }, r -> r.run());
            }
        }
    }

    @Override
    public void update(DelegationUpdate update) {
        Context timer = metrics != null ? metrics.updateOutbound().time() : null;
        if (metrics != null) {
            final var serializedSize = update.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundUpdate().mark(serializedSize);
        }
        ListenableFuture<Empty> ret = null;
        try {
            ret = client.update(update);
        } finally {
            if (timer != null) {
                ret.addListener(() -> {
                    timer.stop();
                }, r -> r.run());
            }
        }
    }
}
