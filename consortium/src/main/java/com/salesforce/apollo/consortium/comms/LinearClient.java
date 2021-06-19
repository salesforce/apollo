/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.LinearServiceGrpc;
import com.salesfoce.apollo.consortium.proto.LinearServiceGrpc.LinearServiceFutureStub;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class LinearClient implements LinearService, Link {
    private static Logger log = LoggerFactory.getLogger(LinearClient.class);

    public static CreateClientCommunications<LinearClient> getCreate(ConsortiumMetrics metrics) {
        return (t, f, c) -> new LinearClient(c, t, metrics);

    }

    private final ManagedServerConnection channel;
    private final LinearServiceFutureStub client;
    private final Member                  member;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics       metrics;

    public LinearClient(ManagedServerConnection channel, Member member, ConsortiumMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = LinearServiceGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public ListenableFuture<TransactionResult> clientSubmit(SubmitTransaction txn) {
        try {
            return client.submit(txn);
        } catch (Throwable e) {
            log.error("Unable to submit", e);
            return null;
        }
    }

    @Override
    public void close() {
        channel.release();
    }

    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<JoinResult> join(Join join) {
        try {
            return client.join(join);
        } catch (Throwable e) {
            log.error("Unable to join", e);
            return null;
        }
    }

    public void release() {
        close();
    }

    @Override
    public void stopData(StopData stopData) {
        try {
            client.stopData(stopData);
        } catch (Throwable e) {
            log.error("Unable to stop", e);
            return;
        }
    }

}
