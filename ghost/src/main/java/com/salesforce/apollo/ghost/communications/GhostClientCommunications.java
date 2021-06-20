/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Entry;
import com.salesfoce.apollo.ghost.proto.Get;
import com.salesfoce.apollo.ghost.proto.GhostGrpc;
import com.salesfoce.apollo.ghost.proto.Interval;
import com.salesfoce.apollo.ghost.proto.Intervals;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostClientCommunications implements SpaceGhost, Link {

    public static CreateClientCommunications<GhostClientCommunications> getCreate() {
        return (t, f, c) -> new GhostClientCommunications(c, t);
    }

    private final ManagedServerConnection   channel;
    private final GhostGrpc.GhostFutureStub client;
    private final Member                    member;

    public GhostClientCommunications(ManagedServerConnection channel, Member member) {
//        assert !(member instanceof Node) : "whoops : " + member + " is not to defined for instance of Node";
        this.member = member;
        this.channel = channel;
        this.client = GhostGrpc.newFutureStub(channel.channel);
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public ListenableFuture<Any> get(Get key) {
        return client.get(key);
    }

    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<Entries> intervals(List<Interval> intervals) {
        Intervals.Builder builder = Intervals.newBuilder();
        intervals.forEach(e -> builder.addIntervals(e));
        return client.intervals(builder.build());
    }

    @Override
    public void put(Entry value) {
        client.put(value);
    }

    public void release() {
        close();
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

}
