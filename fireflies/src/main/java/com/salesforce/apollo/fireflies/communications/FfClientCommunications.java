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
import com.salesforce.apollo.comm.CommonClientCommunications;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.protocols.Fireflies;

import io.grpc.Channel;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FfClientCommunications extends CommonClientCommunications implements Fireflies {
    private final FirefliesBlockingStub client;

    public FfClientCommunications(Channel channel, Participant member) {
        super(member);
        assert !(member instanceof Node) : "whoops : " + member;

        this.client = FirefliesGrpc.newBlockingStub(channel);
    }

    @Override
    public Participant getMember() {
        return (Participant) super.getMember();
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
}
