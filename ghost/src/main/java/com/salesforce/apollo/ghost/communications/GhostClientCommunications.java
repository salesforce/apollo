/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import java.util.List;

import com.google.protobuf.Any;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Entry;
import com.salesfoce.apollo.ghost.proto.Get;
import com.salesfoce.apollo.ghost.proto.GhostGrpc;
import com.salesfoce.apollo.ghost.proto.GhostGrpc.GhostBlockingStub;
import com.salesfoce.apollo.ghost.proto.Interval;
import com.salesfoce.apollo.ghost.proto.Intervals;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostClientCommunications implements SpaceGhost {

    public static CreateClientCommunications<GhostClientCommunications> getCreate() {
        return (t, f, c) -> new GhostClientCommunications(c, (Participant) t);
    }

    private final ManagedServerConnection channel;
    private final GhostBlockingStub       client;
    private final Member                  member;

    public GhostClientCommunications(ManagedServerConnection channel, Member member) {
//        assert !(member instanceof Node) : "whoops : " + member + " is not to defined for instance of Node";
        this.member = member;
        this.channel = channel;
        this.client = GhostGrpc.newBlockingStub(channel.channel);
    }

    @Override
    public Any get(Digest entry) {
        return client.get(Get.newBuilder().setId(entry.toByteString()).build());
    }

    public Participant getMember() {
        return (Participant) member;
    }

    @Override
    public List<Any> intervals(List<Interval> intervals, List<Digest> have) {
        Intervals.Builder builder = Intervals.newBuilder();
        intervals.forEach(e -> builder.addIntervals(e));
        Entries result = client.intervals(builder.build());
        return result.getRecordsList();
    }

    @Override
    public void put(Any any) {
        client.put(Entry.newBuilder().setValue(any).build());
    }

    public void release() {
        channel.release();
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

}
