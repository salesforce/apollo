/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc;

import java.io.IOException;

import com.salesfoce.apollo.thoth.proto.ReconciliationGrpc;
import com.salesfoce.apollo.thoth.proto.ReconciliationGrpc.ReconciliationFutureStub;
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
            public Member getMember() {
                return member;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    private final ManagedServerConnection  channel;
    private final ReconciliationFutureStub client;
    private final Digeste                  context;
    private final Member                   member;
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
        // TODO Auto-generated method stub

    }

    @Override
    public Member getMember() {
        // TODO Auto-generated method stub
        return null;
    }

}
