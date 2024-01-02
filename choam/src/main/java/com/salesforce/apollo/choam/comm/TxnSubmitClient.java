/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.choam.proto.SubmitResult;
import com.salesforce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.proto.TransactionSubmissionGrpc;
import com.salesforce.apollo.choam.proto.TransactionSubmissionGrpc.TransactionSubmissionBlockingStub;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 */
public class TxnSubmitClient implements TxnSubmission {

    private final ManagedServerChannel              channel;
    private final TransactionSubmissionBlockingStub client;

    public TxnSubmitClient(ManagedServerChannel channel, ChoamMetrics metrics) {
        this.channel = channel;
        this.client = TransactionSubmissionGrpc.newBlockingStub(channel).withCompression("gzip");
    }

    public static CreateClientCommunications<TxnSubmission> getCreate(ChoamMetrics metrics) {
        return (c) -> new TxnSubmitClient(c, metrics);

    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    public void release() {
        close();
    }

    @Override
    public SubmitResult submit(Transaction request) {
        return client.submit(request);
    }
}
