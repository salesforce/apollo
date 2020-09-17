/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.LocalTransceiver;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.specific.SpecificResponder;

import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avro.Apollo;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheLocalCommSim implements AvalancheCommunications {

    private final Map<HashKey, Avalanche> servers = new ConcurrentHashMap<>();
    private final RPCPlugin               stats;

    public AvalancheLocalCommSim() {
        this(null);
    }

    public AvalancheLocalCommSim(RPCPlugin stats) {
        this.stats = stats;
    }

    @Override
    public void close() {
    }

    @Override
    public AvalancheClientCommunications connectToNode(Member to, Node from) {
        Avalanche av = servers.get(to.getId());
        if (av == null) {
            throw new IllegalArgumentException("Unable to connect to: " + to + " from: " + from);
        }
        SpecificResponder responder = new SpecificResponder(Apollo.PROTOCOL,
                new AvalancheServerCommunications(av.getService()));

        AvalancheClientCommunications clientCommunications;
        try {
            clientCommunications = new AvalancheClientCommunications(new LocalTransceiver(responder), to);
        } catch (AvroRemoteException e) {
            return null;
        }
        if (stats != null) {
            clientCommunications.add(stats);
        }
        return clientCommunications;
    }

    public Map<HashKey, Avalanche> getServers() {
        return servers;
    }

    public RPCPlugin getStats() {
        return stats;
    }

    @Override
    public void initialize(Avalanche av) {
        servers.put(av.getNode().getId(), av);
    }

    @Override
    public void start() {
    }

}
