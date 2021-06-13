/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.math3.util.Pair;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.AvalancheGrpc;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.Query;
import com.salesfoce.apollo.proto.Query.Builder;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.SuppliedDagNodes;
import com.salesforce.apollo.avalanche.AvalancheMetrics;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheClient implements Avalanche {

    public static CreateClientCommunications<AvalancheClient> getCreate(AvalancheMetrics metrics) {
        return (t, f, c) -> new AvalancheClient(c, t, metrics);

    }

    private final ManagedServerConnection           channel;
    private final AvalancheGrpc.AvalancheFutureStub client;
    private final Member                            member;
    private final AvalancheMetrics                  metrics;

    public AvalancheClient(ManagedServerConnection conn, Member member, AvalancheMetrics metrics) {
        this.channel = conn;
        this.member = member;
        this.client = AvalancheGrpc.newFutureStub(conn.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    public Participant getMember() {
        return (Participant) member;
    }

    @Override
    public ListenableFuture<QueryResult> query(Digest context, List<Pair<Digest, ByteString>> transactions,
                                               Collection<Digest> wanted) {
        Builder builder = Query.newBuilder().setContext(qb64(context));
        transactions.forEach(t -> {
            builder.addHashes(qb64(t.getFirst()));
            builder.addTransactions(t.getSecond());
        });
        wanted.forEach(e -> builder.addWanted(qb64(e)));
        try {
            Query query = builder.build();
            ListenableFuture<QueryResult> result = client.query(query);

            if (metrics != null) {
                result.addListener(() -> {
                    metrics.outboundBandwidth().mark(query.getSerializedSize());
                    QueryResult queryResult;
                    try {
                        queryResult = result.get();
                        metrics.inboundBandwidth().mark(queryResult.getSerializedSize());
                        metrics.outboundQuery().update(query.getSerializedSize());
                        metrics.queryResponse().update(queryResult.getSerializedSize());
                    } catch (InterruptedException | ExecutionException e1) {
                        // ignored for metrics gathering
                    }
                }, ForkJoinPool.commonPool());

            }
            return result;
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        }
    }

    public void release() {
        channel.release();
    }

    @Override
    public ListenableFuture<SuppliedDagNodes> requestDAG(Digest context, Collection<Digest> want) {
        com.salesfoce.apollo.proto.DagNodes.Builder builder = DagNodes.newBuilder().setContext(qb64(context));
        want.forEach(e -> builder.addEntries(qb64(e)));
        try {
            DagNodes request = builder.build();
            ListenableFuture<SuppliedDagNodes> requested = client.requestDag(request);
            if (metrics != null) {
                requested.addListener(() -> {
                    metrics.outboundBandwidth().mark(request.getSerializedSize());
                    SuppliedDagNodes suppliedDagNodes;
                    try {
                        suppliedDagNodes = requested.get();
                        metrics.inboundBandwidth().mark(suppliedDagNodes.getSerializedSize());
                        metrics.outboundRequestDag().update(request.getSerializedSize());
                        metrics.requestDagResponse().update(suppliedDagNodes.getSerializedSize());
                    } catch (InterruptedException | ExecutionException e1) {
                        // ignored for metrics gathering
                    }
                }, ForkJoinPool.commonPool());
            }
            return requested;
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        }
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }
}
