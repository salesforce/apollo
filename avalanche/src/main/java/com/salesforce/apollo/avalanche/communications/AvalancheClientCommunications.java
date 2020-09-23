/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.specific.SpecificData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.proto.QueryResult;
import com.salesforce.apollo.avro.Apollo;
import com.salesforce.apollo.comm.CommonClientCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Avalanche;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheClientCommunications extends CommonClientCommunications implements Avalanche {
    private static final Logger     log = LoggerFactory.getLogger(AvalancheClientCommunications.class);
    private final Apollo            client;
    private final SpecificRequestor requestor;
    private final Transceiver       transceiver;

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
    public QueryResult query(List<ByteBuffer> transactions, Collection<HashKey> wanted) {
        return client.query(transactions, wanted);
    }

    @Override
    public List<ByteBuffer> requestDAG(Collection<HashKey> want) {
        return client.requestDag(want);
    }
}
