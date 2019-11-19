/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.specific.SpecificData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.avro.Apollo;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.avro.QueryResult;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.communications.CommonClientCommunications;
import com.salesforce.apollo.protocols.Avalanche;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheClientCommunications extends CommonClientCommunications implements Avalanche {
    private static final Logger     log = LoggerFactory.getLogger(AvalancheClientCommunications.class);
    private final Apollo            client;
    private final Transceiver       transceiver;
    private final SpecificRequestor requestor;

    public AvalancheClientCommunications(Transceiver transceiver, Member member) throws AvroRemoteException {
        super(member);
        this.transceiver = transceiver;
        try {
            requestor = new SpecificRequestor(Apollo.PROTOCOL, transceiver, SpecificData.get());
            client = SpecificRequestor.getClient(Apollo.class, requestor);
        } catch (IOException e) {
            throw new AvroRemoteException("Cannot create proxy rpc client to: " + member + " : " + transceiver, e);
        }
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
    public QueryResult query(List<ByteBuffer> transactions, List<HASH> wanted) throws AvroRemoteException {
        return client.query(transactions, wanted);
    }

    public void add(RPCPlugin plugin) {
        requestor.addRPCPlugin(plugin);
    }
}
