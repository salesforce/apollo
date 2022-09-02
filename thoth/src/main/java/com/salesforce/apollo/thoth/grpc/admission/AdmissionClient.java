/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.admission;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.thoth.proto.AdmissionsGrpc;
import com.salesfoce.apollo.thoth.proto.AdmissionsGrpc.AdmissionsFutureStub;
import com.salesfoce.apollo.thoth.proto.Admittance;
import com.salesfoce.apollo.thoth.proto.Registration;
import com.salesfoce.apollo.thoth.proto.SignedAttestation;
import com.salesfoce.apollo.thoth.proto.SignedNonce;
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
public class AdmissionClient implements AdmissionService {
    public static CreateClientCommunications<AdmissionService> getCreate(Digest context, GorgoneionMetrics metrics) {
        return (t, f, c) -> {
            return new AdmissionClient(context, c, t, metrics);
        };
    }

    public static AdmissionService getLocalLoopback(Admission service, SigningMember member) {
        return new AdmissionService() {

            @Override
            public ListenableFuture<SignedNonce> apply(Registration registration) {
                return null;
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<Admittance> register(SignedAttestation attestation) {
                return null;
            }
        };
    }

    private final ManagedServerConnection channel;
    private final AdmissionsFutureStub    client;
    @SuppressWarnings("unused")
    private final Digeste                 context;
    private final Member                  member;
    @SuppressWarnings("unused")
    private final GorgoneionMetrics       metrics;

    public AdmissionClient(Digest context, ManagedServerConnection channel, Member member, GorgoneionMetrics metrics) {
        this.context = context.toDigeste();
        this.member = member;
        this.channel = channel;
        this.client = AdmissionsGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public ListenableFuture<SignedNonce> apply(Registration registration) {
        return client.apply(registration);
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
    public ListenableFuture<Admittance> register(SignedAttestation attestation) {
        return client.register(attestation);
    }
}
