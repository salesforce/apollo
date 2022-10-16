/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.endorsement;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.EndorseNonce;
import com.salesfoce.apollo.gorgoneion.proto.EndorsementGrpc;
import com.salesfoce.apollo.gorgoneion.proto.EndorsementGrpc.EndorsementFutureStub;
import com.salesfoce.apollo.gorgoneion.proto.Notarization;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ReleasableManagedChannel;
import com.salesforce.apollo.gorgoneion.comm.GorgoneionMetrics;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class EndorsementClient implements Endorsement {

    public static CreateClientCommunications<Endorsement> getCreate(GorgoneionMetrics metrics) {
        return (t, f, c) -> new EndorsementClient(c, t, metrics);

    }

    private final ReleasableManagedChannel channel;
    private final EndorsementFutureStub   client;
    private final Member                  member;
    private final GorgoneionMetrics       metrics;

    public EndorsementClient(ReleasableManagedChannel channel, Member member, GorgoneionMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = EndorsementGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public void close() throws IOException {
        channel.release();
    }

    @Override
    public ListenableFuture<Validation_> endorse(EndorseNonce nonce, Duration timeout) {
        if (metrics != null) {
            var serializedSize = nonce.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundEndorseNonce().update(serializedSize);
        }

        ListenableFuture<Validation_> result = client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
                                                     .endorse(nonce);
        result.addListener(() -> {
            if (metrics != null) {
                try {
                    var serializedSize = result.get().getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundValidation().update(serializedSize);
                } catch (Throwable e) {
                    // nothing
                }
            }
        }, r -> r.run());
        return result;
    }

    @Override
    public ListenableFuture<Empty> enroll(Notarization notarization, Duration timeout) {
        if (metrics != null) {
            var serializedSize = notarization.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundNotarization().update(serializedSize);
        }

        return client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS).enroll(notarization);
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<Validation_> validate(Credentials credentials, Duration timeout) {
        if (metrics != null) {
            var serializedSize = credentials.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundValidateCredentials().update(serializedSize);
        }

        ListenableFuture<Validation_> result = client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
                                                     .validate(credentials);
        result.addListener(() -> {
            if (metrics != null) {
                try {
                    var serializedSize = result.get().getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundCredentialValidation().update(serializedSize);
                } catch (Throwable e) {
                    // nothing
                }
            }
        }, r -> r.run());
        return result;
    }

}
