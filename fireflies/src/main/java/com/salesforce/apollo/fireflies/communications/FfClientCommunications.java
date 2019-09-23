/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.io.IOException;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.specific.SpecificData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.avro.Apollo;
import com.salesforce.apollo.avro.Digests;
import com.salesforce.apollo.avro.Gossip;
import com.salesforce.apollo.avro.Signed;
import com.salesforce.apollo.avro.Update;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.protocols.Fireflies;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FfClientCommunications extends CommonClientCommunications implements Fireflies {
    private final static Logger log = LoggerFactory.getLogger(FfClientCommunications.class);

    private final Transceiver transceiver;
    private final Apollo client;
    private final SpecificRequestor requestor;

    public FfClientCommunications(Transceiver transceiver, Member member) {
        super(member);
        assert !(member instanceof Node) : "whoops : " + member;

        this.transceiver = transceiver;
        try {
            requestor = new SpecificRequestor(Apollo.PROTOCOL, transceiver, SpecificData.get());
            client = SpecificRequestor.getClient(Apollo.class, requestor);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create specific requestor for: " + member + " : " + transceiver, e);
        }
    }

    public void add(RPCPlugin plugin) {
        requestor.addRPCPlugin(plugin);
    }

    @Override
    public void close() {
        try {
            transceiver.close();
        } catch (IOException e) {
            log.trace("error closing communications with " + member, e);
        }
    }

    @Override
    public Gossip gossip(Signed note, int ring, Digests digests) throws AvroRemoteException {
        try {
            return client.gossip(note, ring, digests);
        } catch (Throwable e) {
            throw new AvroRemoteException("Unexpected exception in communication", e);
        }
    }

    public boolean isConnected() {
        return transceiver.isConnected();
    }

    @Override
    public int ping(int ping) throws AvroRemoteException {
        try {
            return client.ping(ping);
        } catch (Throwable e) {
            throw new AvroRemoteException("Unexpected exception in communication", e);
        }
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

    @Override
    public void update(int ring, Update update) throws AvroRemoteException {
        try {
            client.update(ring, update);
        } catch (Throwable e) {
            throw new AvroRemoteException("Unexpected exception in communication", e);
        }
    }

}
