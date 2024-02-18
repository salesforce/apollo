/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.endorsement;

import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.gorgoneion.comm.GorgoneionMetrics;
import com.salesforce.apollo.gorgoneion.proto.*;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.event.proto.Validation_;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author hal.hildebrand
 */
public class EndorsementClient implements Endorsement {

    private final ManagedServerChannel                    channel;
    private final EndorsementGrpc.EndorsementBlockingStub client;
    private final GorgoneionMetrics                       metrics;

    public EndorsementClient(ManagedServerChannel channel, GorgoneionMetrics metrics) {
        this.channel = channel;
        this.client = channel.wrap(EndorsementGrpc.newBlockingStub(channel));
        this.metrics = metrics;
    }

    public static CreateClientCommunications<Endorsement> getCreate(GorgoneionMetrics metrics) {
        return (c) -> new EndorsementClient(c, metrics);

    }

    @Override
    public void close() throws IOException {
        channel.release();
    }

    @Override
    public MemberSignature endorse(Nonce nonce, Duration timeout) {
        if (metrics != null) {
            var serializedSize = nonce.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundEndorseNonce().update(serializedSize);
        }

        var result = client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS).endorse(nonce);
        if (metrics != null) {
            try {
                var serializedSize = result.getSerializedSize();
                metrics.inboundBandwidth().mark(serializedSize);
                metrics.inboundValidation().update(serializedSize);
            } catch (Throwable e) {
                // nothing
            }
        }
        return result;
    }

    @Override
    public void enroll(Notarization notarization, Duration timeout) {
        if (metrics != null) {
            var serializedSize = notarization.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundNotarization().update(serializedSize);
        }

        client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS).enroll(notarization);
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    @Override
    public Validation_ validate(Credentials credentials, Duration timeout) {
        if (metrics != null) {
            var serializedSize = credentials.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundValidateCredentials().update(serializedSize);
        }

        var result = client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS).validate(credentials);
        if (metrics != null) {
            try {
                var serializedSize = result.getSerializedSize();
                metrics.inboundBandwidth().mark(serializedSize);
                metrics.inboundCredentialValidation().update(serializedSize);
            } catch (Throwable e) {
                // nothing
            }
        }
        return result;
    }
}
