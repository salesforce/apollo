/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.reconciliation;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.thoth.proto.Intervals;
import com.salesfoce.apollo.thoth.proto.ReconciliationGrpc;
import com.salesfoce.apollo.thoth.proto.ReconciliationGrpc.ReconciliationFutureStub;
import com.salesfoce.apollo.thoth.proto.Update;
import com.salesfoce.apollo.thoth.proto.Updating;
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
public class ReconciliationClient implements ReconciliationService {
    public static CreateClientCommunications<ReconciliationService> getCreate(Digest context,
                                                                              StereotomyMetrics metrics) {
        return (t, f, c) -> {
            return new ReconciliationClient(context, c, t, metrics);
        };
    }

    public static ReconciliationService getLocalLoopback(Reconciliation service, SigningMember member) {
        return new ReconciliationService() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<Update> reconcile(Intervals intervals) {
                SettableFuture<Update> fs = SettableFuture.create();
                fs.set(Update.getDefaultInstance());
                return fs;
            }

            @Override
            public ListenableFuture<Empty> update(Updating update) {
                SettableFuture<Empty> fs = SettableFuture.create();
                fs.set(Empty.getDefaultInstance());
                return fs;
            }
        };
    }

    private final ManagedServerConnection  channel;
    private final ReconciliationFutureStub client;
    @SuppressWarnings("unused")
    private final Digeste                  context;
    private final Member                   member;
    @SuppressWarnings("unused")
    private final StereotomyMetrics        metrics;

    public ReconciliationClient(Digest context, ManagedServerConnection channel, Member member,
                                StereotomyMetrics metrics) {
        this.context = context.toDigeste();
        this.member = member;
        this.channel = channel;
        this.client = ReconciliationGrpc.newFutureStub(channel.channel).withCompression("gzip");
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
    public ListenableFuture<Update> reconcile(Intervals intervals) {
        return client.reconcile(intervals);
    }

    @Override
    public ListenableFuture<Empty> update(Updating update) {
        return client.update(update);
    }
}
