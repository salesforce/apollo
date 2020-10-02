/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import java.util.List;

import com.salesfoce.apollo.proto.ADagEntry;
import com.salesfoce.apollo.proto.Bytes;
import com.salesfoce.apollo.proto.DagEntries;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.GhostGrpc;
import com.salesfoce.apollo.proto.GhostGrpc.GhostBlockingStub;
import com.salesfoce.apollo.proto.Interval;
import com.salesfoce.apollo.proto.Intervals;
import com.salesfoce.apollo.proto.Intervals.Builder;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.SpaceGhost;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostClientCommunications implements SpaceGhost {

    public static CreateClientCommunications<GhostClientCommunications> getCreate() {
        return (t, f, c) -> new GhostClientCommunications(c, (Participant) t);
    }

    private final GhostBlockingStub       client;
    private final ManagedServerConnection channel;
    private final Member                  member;

    public GhostClientCommunications(ManagedServerConnection channel, Member member) {
//        assert !(member instanceof Node) : "whoops : " + member + " is not to defined for instance of Node";
        this.member = member;
        this.channel = channel;
        this.client = GhostGrpc.newBlockingStub(channel.channel);
    }

    public Participant getMember() {
        return (Participant) member;
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

    public void release() {
        channel.release();
    }

    @Override
    public DagEntry get(HashKey entry) {
        return client.get(Bytes.newBuilder().setBites(entry.toByteString()).build());
    }

    @Override
    public List<DagEntry> intervals(List<Interval> intervals, List<HashKey> have) {
        Builder builder = Intervals.newBuilder();
        intervals.forEach(e -> builder.addIntervals(e));
        DagEntries result = client.intervals(builder.build());
        return result.getEntriesList();
    }

    @Override
    public void put(DagEntry value) {
        client.put(ADagEntry.newBuilder().setEntry(value).build());
    }

}
