/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.entrance;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.fireflies.FireflyMetrics;
import com.salesforce.apollo.fireflies.proto.*;
import com.salesforce.apollo.fireflies.proto.EntranceGrpc.EntranceFutureStub;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.stereotomy.event.proto.KeyState_;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author hal.hildebrand
 */
public class EntranceClient implements Entrance {

    private final ManagedServerChannel channel;
    private final EntranceFutureStub   client;
    private final FireflyMetrics       metrics;

    public EntranceClient(ManagedServerChannel channel, FireflyMetrics metrics) {
        this.channel = channel;
        this.client = EntranceGrpc.newFutureStub(channel).withCompression("gzip");
        this.metrics = metrics;
    }

    public static CreateClientCommunications<Entrance> getCreate(FireflyMetrics metrics) {
        return (c) -> new EntranceClient(c, metrics);

    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public ListenableFuture<KeyState_> getKeyState(IdentifierSequenceNumber idSeq) {
        return client.getKeyStateIdentifier(idSeq);
    }

    @Override
    public ListenableFuture<KeyState_> getKeyState(EventCoords coords) {
        return client.getKeyStateCoords(coords);
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    @Override
    public ListenableFuture<Gateway> join(Join join, Duration timeout) {
        if (metrics != null) {
            var serializedSize = join.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundJoin().update(serializedSize);
        }

        ListenableFuture<Gateway> result = client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS).join(join);
        result.addListener(() -> {
            if (metrics != null) {
                try {
                    var serializedSize = result.get().getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundGateway().update(serializedSize);
                } catch (Throwable e) {
                    // nothing
                }
            }
        }, r -> r.run());
        return result;
    }

    @Override
    public ListenableFuture<Redirect> seed(Registration registration) {
        if (metrics != null) {
            var serializedSize = registration.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundSeed().update(serializedSize);
        }
        ListenableFuture<Redirect> result = client.seed(registration);
        result.addListener(() -> {
            if (metrics != null) {
                try {
                    var serializedSize = result.get().getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundRedirect().update(serializedSize);
                } catch (Throwable e) {
                    // nothing
                }
            }
        }, r -> r.run());
        return result;
    }
}
