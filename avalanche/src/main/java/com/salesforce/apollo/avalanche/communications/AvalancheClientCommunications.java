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
import com.salesforce.apollo.comm.CommonClientCommunications;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Avalanche;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.ManagedChannel;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheClientCommunications extends CommonClientCommunications implements Avalanche {
    private final AvalancheBlockingStub client;
    private final ManagedChannel        channel;

    public AvalancheClientCommunications(ManagedChannel channel, Member member) {
        super(member);
        assert !(member instanceof Node) : "whoops : " + member + " is not to defined for instance of Node";
        this.channel = channel;
        this.client = AvalancheGrpc.newBlockingStub(channel);
    }

    @Override
    public Participant getMember() {
        return (Participant) super.getMember();
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

    @Override
    public void close() {
        channel.shutdown();
    }

    @Override
    public QueryResult query(List<ByteBuffer> transactions, Collection<HashKey> wanted) {
        Builder builder = Query.newBuilder();
        transactions.stream().filter(e -> e.hasRemaining()).forEach(e -> builder.addTransactions(ByteString.copyFrom(e.array())));
        wanted.forEach(e -> builder.addWanted(e.toByteString()));
        try {
            return client.query(builder.build());
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        }
    }

    @Override
    public List<ByteBuffer> requestDAG(Collection<HashKey> want) {
        com.salesfoce.apollo.proto.DagNodes.Builder builder = DagNodes.newBuilder();
        want.forEach(e -> builder.addEntries(e.toByteString()));
        try {
            DagNodes requested = client.requestDag(builder.build());
            return requested.getEntriesList().stream().map(e -> e.asReadOnlyByteBuffer()).collect(Collectors.toList());
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        }
    }
}
