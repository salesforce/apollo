/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.TransactionSubmissionGrpc;
import com.salesfoce.apollo.choam.proto.TransactionSubmissionGrpc.TransactionSubmissionBlockingStub;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class TxnSubmitClient implements TxnSubmission {

    public static CreateClientCommunications<TxnSubmission> getCreate(ChoamMetrics metrics) {
        return (t, f, c) -> new TxnSubmitClient(c, t, metrics);

    }

    private final ManagedServerConnection channel;

    private final TransactionSubmissionBlockingStub client;
    private final Member                            member;
    @SuppressWarnings("unused")
    private final ChoamMetrics                      metrics;

    public TxnSubmitClient(ManagedServerConnection channel, Member member, ChoamMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = TransactionSubmissionGrpc.newBlockingStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return member;
    }

    public void release() {
        close();
    }

    @Override
    public SubmitResult submit(SubmitTransaction request) {
        return client.submit(request);
    }
}
