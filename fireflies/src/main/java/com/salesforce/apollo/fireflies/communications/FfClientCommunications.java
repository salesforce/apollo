/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import com.salesfoce.apollo.proto.Digests;
import com.salesfoce.apollo.proto.FirefliesGrpc;
import com.salesfoce.apollo.proto.FirefliesGrpc.FirefliesBlockingStub;
import com.salesfoce.apollo.proto.Gossip;
import com.salesfoce.apollo.proto.Null;
import com.salesfoce.apollo.proto.SayWhat;
import com.salesfoce.apollo.proto.Signed;
import com.salesfoce.apollo.proto.State;
import com.salesfoce.apollo.proto.Update;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.protocols.Fireflies;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FfClientCommunications implements Fireflies {

    public static CreateClientCommunications<FfClientCommunications> getCreate() {
        CreateClientCommunications<FfClientCommunications> createFunction = (t, f, c) -> new FfClientCommunications(c,
                (Participant) t);
        return createFunction;

    }

    private final ManagedServerConnection channel;
    private final FirefliesBlockingStub   client;
    private Participant                   member;

    public FfClientCommunications(ManagedServerConnection channel, Participant member) {
        this.member = member;
        assert !(member instanceof Node) : "whoops : " + member;
        this.channel = channel;
        this.client = FirefliesGrpc.newBlockingStub(channel.channel);
    }

    public Participant getMember() {
        return member;
    }

    @Override
    public Gossip gossip(Signed note, int ring, Digests digests) {
        try {
            return client.gossip(SayWhat.newBuilder().setNote(note).setRing(ring).setGossip(digests).build());
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        }
    }

    @Override
    public int ping(int ping) {
        try {
            client.ping(Null.getDefaultInstance());
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        }
        return 0;
    }

    public void release() {
        channel.release();
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

    @Override
    public void update(int ring, Update update) {
        try {
            client.update(State.newBuilder().setRing(ring).setUpdate(update).build());
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        }
    }

    public void start() {

    }
}
