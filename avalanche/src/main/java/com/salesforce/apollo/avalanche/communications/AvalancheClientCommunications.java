/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.AvalancheGrpc;
import com.salesfoce.apollo.proto.AvalancheGrpc.AvalancheBlockingStub;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.Query;
import com.salesfoce.apollo.proto.Query.Builder;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesforce.apollo.avalanche.AvalancheMetrics;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.fireflies.Node;
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
        return (t, f, c) -> new AvalancheClientCommunications(c, (Participant) t, metrics);

    }

    private final ManagedServerConnection channel;
    private final AvalancheBlockingStub   client;
    private final Member                  member;
    private final AvalancheMetrics        metrics;

    public AvalancheClientCommunications(ManagedServerConnection conn, Member member, AvalancheMetrics metrics) {
        assert !(member instanceof Node) : "whoops : " + member + " is not to defined for instance of Node";
        this.channel = conn;
        this.member = member;
        this.client = AvalancheGrpc.newBlockingStub(conn.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    public Participant getMember() {
        return (Participant) member;
    }

    @Override
    public QueryResult query(List<ByteBuffer> transactions, Collection<HashKey> wanted) {
        Builder builder = Query.newBuilder();
        transactions.stream()
                    .filter(e -> e.hasRemaining())
                    .forEach(e -> builder.addTransactions(ByteString.copyFrom(e.array())));
        wanted.forEach(e -> builder.addWanted(e.toByteString()));
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
    public List<ByteBuffer> requestDAG(Collection<HashKey> want) {
        com.salesfoce.apollo.proto.DagNodes.Builder builder = DagNodes.newBuilder();
        want.forEach(e -> builder.addEntries(e.toByteString()));
        try {
            DagNodes request = builder.build();
            DagNodes requested = client.requestDag(request);
            if (metrics != null) {
                metrics.outboundBandwidth().mark(request.getSerializedSize());
                metrics.inboundBandwidth().mark(requested.getSerializedSize());
                metrics.outboundRequestDag().update(request.getSerializedSize());
                metrics.requestDagResponse().update(requested.getSerializedSize());
            }
            return requested.getEntriesList().stream().map(e -> e.asReadOnlyByteBuffer()).collect(Collectors.toList());
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        }
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }
}
