/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import java.util.Collection;
import java.util.List;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.AvalancheGrpc;
import com.salesfoce.apollo.proto.AvalancheGrpc.AvalancheBlockingStub;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.ID;
import com.salesfoce.apollo.proto.Query;
import com.salesfoce.apollo.proto.Query.Builder;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.ReQuery;
import com.salesfoce.apollo.proto.SuppliedDagNodes;
import com.salesforce.apollo.avalanche.AvalancheMetrics;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Avalanche;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheClientCommunications implements Avalanche {

    public static CreateClientCommunications<AvalancheClientCommunications> getCreate(AvalancheMetrics metrics) {
        return (t, f, c) -> new AvalancheClientCommunications(c, t, metrics);

    }

    private final ManagedServerConnection channel;
    private final AvalancheBlockingStub   client;
    private final Member                  member;
    private final AvalancheMetrics        metrics;

    public AvalancheClientCommunications(ManagedServerConnection conn, Member member, AvalancheMetrics metrics) {
        this.channel = conn;
        this.member = member;
        this.client = AvalancheGrpc.newBlockingStub(conn.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    public Participant getMember() {
        return (Participant) member;
    }

    @Override
    public QueryResult query(HashKey context, List<ID> transactions, Collection<HashKey> wanted) {
        Builder builder = Query.newBuilder().setContext(context.toID());
        transactions.stream().forEach(e -> builder.addTransactions(e));
        wanted.forEach(e -> builder.addWanted(e.toID()));
        try {
            Query query = builder.build();
            QueryResult result = client.query(query);
            if (metrics != null) {
                metrics.outboundBandwidth().mark(query.getSerializedSize());
                metrics.inboundBandwidth().mark(result.getSerializedSize());
                metrics.outboundQuery().update(query.getSerializedSize());
                metrics.queryResponse().update(result.getSerializedSize());
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
    public List<ByteString> requestDAG(HashKey context, Collection<HashKey> want) {
        com.salesfoce.apollo.proto.DagNodes.Builder builder = DagNodes.newBuilder().setContext(context.toID());
        want.forEach(e -> builder.addEntries(e.toID()));
        try {
            DagNodes request = builder.build();
            SuppliedDagNodes requested = client.requestDag(request);
            if (metrics != null) {
                metrics.outboundBandwidth().mark(request.getSerializedSize());
                metrics.inboundBandwidth().mark(requested.getSerializedSize());
                metrics.outboundRequestDag().update(request.getSerializedSize());
                metrics.requestDagResponse().update(requested.getSerializedSize());
            }
            return requested.getEntriesList();
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        }
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

    @Override
    public QueryResult requery(HashKey context, List<ByteString> transactions) {
        ReQuery.Builder builder = ReQuery.newBuilder().setContext(context.toID());
        transactions.stream().forEach(e -> builder.addTransactions(e));
        try {
            ReQuery query = builder.build();
            QueryResult result = client.requery(query);
            if (metrics != null) {
                metrics.outboundBandwidth().mark(query.getSerializedSize());
                metrics.inboundBandwidth().mark(result.getSerializedSize());
                metrics.outboundQuery().update(query.getSerializedSize());
                metrics.queryResponse().update(result.getSerializedSize());
            }
            return result;
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        }
    }
}
