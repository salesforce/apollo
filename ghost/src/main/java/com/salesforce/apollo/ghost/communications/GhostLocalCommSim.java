/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.LocalTransceiver;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.specific.SpecificResponder;

import com.salesforce.apollo.avro.Apollo;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.ghost.Ghost;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostLocalCommSim implements GhostCommunications {

    private final Map<HashKey, Ghost> servers = new ConcurrentHashMap<>();
    private final RPCPlugin stats;

    public GhostLocalCommSim() {
        this(null);
    }

    public GhostLocalCommSim(RPCPlugin stats) {
        this.stats = stats;
    }

    @Override
    public void close() {}

    public Map<HashKey, Ghost> getServers() {
        return servers;
    }

    public RPCPlugin getStats() {
        return stats;
    }

    @Override
    public void initialize(Ghost ghost) {
        servers.put(ghost.getNode().getId(), ghost);
    }

    @Override
    public void start() {}

    @Override
    public GhostClientCommunications connect(Member to, Node from) {
        Ghost ghost = servers.get(to.getId());
        if (ghost == null) { return null; }
        SpecificResponder responder = new SpecificResponder(Apollo.PROTOCOL,
                                                            new GhostServerCommunications(ghost.getService()));

        GhostClientCommunications clientCommunications;
        try {
            clientCommunications = new GhostClientCommunications(new LocalTransceiver(responder), to);
        } catch (AvroRemoteException e) {
            return null;
        }
        return clientCommunications;
    }

}
