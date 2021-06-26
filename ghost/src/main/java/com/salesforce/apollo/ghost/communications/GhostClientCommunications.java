/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.ghost.proto.Bind;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Entry;
import com.salesfoce.apollo.ghost.proto.Get;
import com.salesfoce.apollo.ghost.proto.GhostGrpc;
import com.salesfoce.apollo.ghost.proto.Intervals;
import com.salesfoce.apollo.ghost.proto.Lookup;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostClientCommunications implements SpaceGhost {

    public static CreateClientCommunications<SpaceGhost> getCreate() {
        return (t, f, c) -> new GhostClientCommunications(c, t);
    }

    private final ManagedServerConnection   channel;
    private final GhostGrpc.GhostFutureStub client;
    private final Member                    member;

    public GhostClientCommunications(ManagedServerConnection channel, Member member) {
        this.member = member;
        this.channel = channel;
        this.client = GhostGrpc.newFutureStub(channel.channel);
    }

    @Override
    public ListenableFuture<Empty> bind(Bind binding) {
        return client.bind(binding);
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public ListenableFuture<Any> get(Get key) {
        return client.get(key);
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<Entries> intervals(Intervals intervals) {
        return client.intervals(intervals);
    }

    @Override
    public ListenableFuture<Any> lookup(Lookup query) {
        return client.lookup(query);
    }

    @Override
    public ListenableFuture<Empty> purge(Get key) {
        return client.purge(key);
    }

    @Override
    public ListenableFuture<Empty> put(Entry value) {
        return client.put(value);
    }

    public void release() {
        close();
    }

    @Override
    public ListenableFuture<Empty> remove(Lookup query) {
        return client.remove(query);
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

}
