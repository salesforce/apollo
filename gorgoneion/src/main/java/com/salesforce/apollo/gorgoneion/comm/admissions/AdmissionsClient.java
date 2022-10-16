/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.admissions;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.gorgoneion.proto.AdmissionsGrpc;
import com.salesfoce.apollo.gorgoneion.proto.AdmissionsGrpc.AdmissionsFutureStub;
import com.salesfoce.apollo.gorgoneion.proto.Application;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.Invitation;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
;
import com.salesforce.apollo.gorgoneion.comm.GorgoneionMetrics;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class AdmissionsClient implements Admissions {

    public static CreateClientCommunications<Admissions> getCreate(GorgoneionMetrics metrics) {
        return (t, f, c) -> new AdmissionsClient(c, t, metrics);

    }

    private final ManagedServerChannel channel;
    private final AdmissionsFutureStub    client;
    private final Member                  member;
    private final GorgoneionMetrics       metrics;

    public AdmissionsClient(ManagedServerChannel channel, Member member, GorgoneionMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = AdmissionsGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public ListenableFuture<SignedNonce> apply(Application application, Duration timeout) {
        if (metrics != null) {
            var serializedSize = application.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundApplication().update(serializedSize);
        }

        ListenableFuture<SignedNonce> result = client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
                                                     .apply(application);
        result.addListener(() -> {
            if (metrics != null) {
                try {
                    var serializedSize = result.get().getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundApplication().update(serializedSize);
                } catch (Throwable e) {
                    // nothing
                }
            }
        }, r -> r.run());
        return result;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<Invitation> register(Credentials credentials, Duration timeout) {
        if (metrics != null) {
            var serializedSize = credentials.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundCredentials().update(serializedSize);
        }

        ListenableFuture<Invitation> result = client.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
                                                    .register(credentials);
        result.addListener(() -> {
            if (metrics != null) {
                try {
                    var serializedSize = result.get().getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.inboundInvitation().update(serializedSize);
                } catch (Throwable e) {
                    // nothing
                }
            }
        }, r -> r.run());
        return result;
    }
}
